package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yumekaz/duraflow/internal/engine"
	"github.com/yumekaz/duraflow/internal/store"
	"github.com/yumekaz/duraflow/internal/workflow"
)

type WorkerDaemon struct {
	workerID  string
	store     store.EventStore
	eng       *engine.WorkflowEngine
	activeJobs sync.Map // map[string]chan struct{} for step lease cancel/renewal
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewWorkerDaemon(store store.EventStore, eng *engine.WorkflowEngine) *WorkerDaemon {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerDaemon{
		workerID:  uuid.New().String(),
		store:     store,
		eng:       eng,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (w *WorkerDaemon) WorkerID() string {
	return w.workerID
}

func (w *WorkerDaemon) Start() error {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	w1 := &store.Worker{
		WorkerID:        w.workerID,
		Hostname:        hostname,
		PID:             os.Getpid(),
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	}

	if err := w.store.RegisterWorker(w1); err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	// 1. Start Heartbeat Loop
	w.wg.Add(1)
	go w.heartbeatLoop()

	// 2. Start Poll Loop
	w.wg.Add(1)
	go w.pollLoop()

	// 3. Start Cron Loop
	w.wg.Add(1)
	go w.cronLoop()

	return nil
}

func (w *WorkerDaemon) cronLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.triggerCronSchedules()
		}
	}
}

func (w *WorkerDaemon) triggerCronSchedules() {
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	dueSchedules, err := w.store.GetDueCronSchedules(nowStr)
	if err != nil {
		return
	}

	for _, cs := range dueSchedules {
		_, triggered, err := w.store.TriggerCronSchedule(cs.WorkflowName, now)
		if err == nil && triggered {
			fmt.Printf("Worker %s triggered cron workflow run for %s\n", w.workerID, cs.WorkflowName)
		}
	}
}

func (w *WorkerDaemon) Stop() {
	w.cancel()
	w.wg.Wait()
}

func (w *WorkerDaemon) heartbeatLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			// Mark worker as INACTIVE on graceful shutdown
			hostname, _ := os.Hostname()
			_ = w.store.RegisterWorker(&store.Worker{
				WorkerID:        w.workerID,
				Hostname:        hostname,
				PID:             os.Getpid(),
				StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
				LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
				Status:          "INACTIVE",
			})
			return
		case <-ticker.C:
			_ = w.store.HeartbeatWorker(w.workerID)
		}
	}
}

func (w *WorkerDaemon) pollLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.scanAndRunEligibleSteps()
		}
	}
}

func (w *WorkerDaemon) scanAndRunEligibleSteps() {
	// Query all incomplete runs
	runs, err := w.store.GetIncompleteRuns()
	if err != nil {
		return
	}

	for _, run := range runs {
		// Load workflow definition
		yamlContent, err := w.store.GetWorkflowYAML(run.WorkflowName, run.WorkflowVersion)
		if err != nil {
			continue
		}

		def, _, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
		if err != nil {
			continue
		}

		// Load step states
		states, err := w.store.GetStepStates(run.RunID)
		if err != nil {
			continue
		}

		stateMap := make(map[string]*store.StepState)
		for _, st := range states {
			stateMap[st.StepID] = st
		}

		// Find next eligible steps in topological order
		for _, step := range orderedSteps {
			st := stateMap[step.ID]

			// A step is eligible to execute if:
			// 1. It is PENDING, or
			// 2. It is RUNNING (so we try to acquire/reclaim its expired lease)
			// 3. It is RETRY_SCHEDULED and the retry delay has elapsed or is empty.
			// 4. It is WAITING and its timer has fired.
			isEligible := false
			if st == nil || st.Status == engine.StepPending || st.Status == engine.StepRunning {
				isEligible = true
			} else if st.Status == engine.StepWaiting {
				tRecord, err := w.store.GetTimer(run.RunID, step.ID)
				if err == nil && tRecord != nil {
					if tRecord.Status == "FIRED" {
						isEligible = true
					} else if tRecord.Status == "PENDING" {
						fireTime, err := time.Parse(time.RFC3339Nano, tRecord.FireAt)
						if err == nil && time.Now().UTC().After(fireTime) {
							isEligible = true
						}
					}
				}
			} else if st.Status == engine.StepRetryScheduled {
				nowStr := time.Now().UTC().Format(time.RFC3339Nano)
				if st.NextRetryAt == "" || st.NextRetryAt <= nowStr {
					isEligible = true
				}
			}

			if !isEligible {
				continue
			}

			// Verify dependencies are met
			depsMet := true
			for _, dep := range step.DependsOn {
				depState := stateMap[dep]
				if depState == nil || depState.Status != engine.StepSucceeded {
					depsMet = false
					break
				}
			}

			if !depsMet {
				continue
			}

			preLeaseStatus := engine.StepPending
			if st != nil {
				preLeaseStatus = st.Status
			}

			// Try to acquire lease
			leaseDuration := 10 * time.Second
			acquired, err := w.store.AcquireLease(run.RunID, step.ID, w.workerID, leaseDuration)
			if err != nil || !acquired {
				continue
			}

			// Retrieve the updated step state from AcquireLease transaction to get the assigned attempt
			updatedStates, err := w.store.GetStepStates(run.RunID)
			if err != nil {
				_ = w.store.ReleaseLease(run.RunID, step.ID, w.workerID)
				continue
			}
			attempt := 1
			for _, us := range updatedStates {
				if us.StepID == step.ID {
					attempt = us.Attempt
					break
				}
			}

			if step.Wait != nil {
				if preLeaseStatus == engine.StepPending || preLeaseStatus == "" {
					dur, err := time.ParseDuration(step.Wait.Duration)
					if err != nil {
						dur = 0
					}
					timerID := uuid.New().String()
					fireAt := time.Now().UTC().Add(dur).Format(time.RFC3339Nano)

					_ = w.store.CreateTimer(&store.Timer{
						TimerID:     timerID,
						RunID:       run.RunID,
						StepID:      step.ID,
						FireAt:      fireAt,
						Status:      "PENDING",
						PayloadJSON: "{}",
					})

					_ = w.store.UpsertStepState(&store.StepState{
						RunID:       run.RunID,
						StepID:      step.ID,
						Status:      engine.StepWaiting,
						Attempt:     1,
						MaxAttempts: 1,
						StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
						WorkerID:    w.workerID,
					})

					_ = w.eng.AppendEvent(&store.Event{
						RunID:        run.RunID,
						WorkflowName: def.Name,
						EventType:    engine.EventTimerCreated,
						StepID:       step.ID,
						WorkerID:     w.workerID,
						Attempt:      1,
						PayloadJSON:  fmt.Sprintf(`{"duration":%q,"fire_at":%q}`, step.Wait.Duration, fireAt),
					})

					_ = w.store.ReleaseLease(run.RunID, step.ID, w.workerID)
					continue
				}

				if preLeaseStatus == engine.StepWaiting {
					tRecord, err := w.store.GetTimer(run.RunID, step.ID)
					if err == nil && tRecord != nil && tRecord.Status == "PENDING" {
						_ = w.store.FireTimer(tRecord.TimerID)
						_ = w.eng.AppendEvent(&store.Event{
							RunID:        run.RunID,
							WorkflowName: def.Name,
							EventType:    engine.EventTimerFired,
							StepID:       step.ID,
							WorkerID:     w.workerID,
							Attempt:      1,
							PayloadJSON:  "{}",
						})
					}
					w.wg.Add(1)
					go func(rID string, d *workflow.WorkflowDef, s workflow.StepDef, att int) {
						defer w.wg.Done()
						_ = w.eng.ExecuteStepAttempt(w.ctx, rID, d, s, att)
						_ = w.store.ReleaseLease(rID, s.ID, w.workerID)
					}(run.RunID, def, step, attempt)
					continue
				}
			}

			// Launch execution
			w.wg.Add(1)
			go w.executeStep(run.RunID, def, step, attempt)
		}
	}
}

func (w *WorkerDaemon) executeStep(runID string, def *workflow.WorkflowDef, step workflow.StepDef, attempt int) {
	defer w.wg.Done()

	jobKey := fmt.Sprintf("%s:%s", runID, step.ID)
	stopLeaseRenewal := make(chan struct{})
	w.activeJobs.Store(jobKey, stopLeaseRenewal)
	defer w.activeJobs.Delete(jobKey)

	// Start Lease Renewal Loop
	leaseDuration := 10 * time.Second
	go w.leaseRenewalLoop(runID, step.ID, leaseDuration, stopLeaseRenewal)

	// Execute the step attempt
	stepCtx, cancel := context.WithCancel(w.ctx)
	defer cancel()

	fmt.Printf("Worker %s starting step %s (attempt %d) of run %s\n", w.workerID, step.ID, attempt, runID)
	execErr := w.eng.ExecuteStepAttempt(stepCtx, runID, def, step, attempt)
	close(stopLeaseRenewal)

	if execErr != nil {
		fmt.Printf("Worker %s completed step %s of run %s with error: %v\n", w.workerID, step.ID, runID, execErr)
	} else {
		fmt.Printf("Worker %s successfully completed step %s of run %s\n", w.workerID, step.ID, runID)
	}

	// Always release the lease at the end
	_ = w.store.ReleaseLease(runID, step.ID, w.workerID)
}

func (w *WorkerDaemon) leaseRenewalLoop(runID, stepID string, duration time.Duration, stopChan chan struct{}) {
	ticker := time.NewTicker(duration / 3) // Renew at 1/3 of duration (e.g. every 3.3s for 10s duration)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-stopChan:
			return
		case <-ticker.C:
			renewed, err := w.store.RenewLease(runID, stepID, w.workerID, duration)
			if err != nil || !renewed {
				// Failed to renew lease (stolen, expired, db locked). We could abort but let's log.
				return
			}
		}
	}
}
