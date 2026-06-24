package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yumekaz/duraflow/internal/engine"
	"github.com/yumekaz/duraflow/internal/executor"
	"github.com/yumekaz/duraflow/internal/store"
	"github.com/yumekaz/duraflow/internal/workflow"
)

func TestWorkerDaemon_WaitStep(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-worker-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "duraflow.db")
	dbStore := store.NewSQLiteStore(dbPath)
	if err := dbStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer dbStore.Close()

	hostExec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(dbStore, hostExec)
	w := NewWorkerDaemon(dbStore, eng)

	// Register a worker to satisfy lease foreign key constraint
	hostname, _ := os.Hostname()
	err = dbStore.RegisterWorker(&store.Worker{
		WorkerID:        w.workerID,
		Hostname:        hostname,
		PID:             os.Getpid(),
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	yamlContent := `
name: test-wait-workflow
version: 1
steps:
  - id: wait-step
    wait:
      duration: "50ms"
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	// 1. Scan when step is pending -> should transition to WAITING and create timer
	w.scanAndRunEligibleSteps()

	// Verify step state is WAITING
	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected 1 step state, got %d", len(states))
	}
	if states[0].Status != engine.StepWaiting {
		t.Fatalf("expected step state to be WAITING, got %s", states[0].Status)
	}

	// Verify timer is PENDING
	timer, err := dbStore.GetTimer(runID, "wait-step")
	if err != nil {
		t.Fatalf("failed to get timer: %v", err)
	}
	if timer == nil || timer.Status != "PENDING" {
		t.Fatalf("expected pending timer, got: %+v", timer)
	}

	// 2. Scan immediately -> should not execute since timer hasn't fired yet
	w.scanAndRunEligibleSteps()
	states, _ = dbStore.GetStepStates(runID)
	if states[0].Status != engine.StepWaiting {
		t.Fatalf("expected step state to remain WAITING, got %s", states[0].Status)
	}

	// 3. Wait for timer duration to pass and scan again -> should fire and succeed
	time.Sleep(100 * time.Millisecond)

	w.scanAndRunEligibleSteps()

	// Wait for any async execution in scanAndRunEligibleSteps to complete
	w.wg.Wait()

	// Verify step is SUCCEEDED
	states, _ = dbStore.GetStepStates(runID)
	if states[0].Status != engine.StepSucceeded {
		t.Fatalf("expected step state to be SUCCEEDED, got %s", states[0].Status)
	}

	// Verify timer is FIRED
	timer, _ = dbStore.GetTimer(runID, "wait-step")
	if timer.Status != "FIRED" {
		t.Fatalf("expected timer to be FIRED, got %s", timer.Status)
	}

	// Run status should be COMPLETED since wait-step succeeded
	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != engine.StatusCompleted {
		t.Errorf("expected run status COMPLETED, got %s", run.Status)
	}
}

func TestWorkerDaemon_CronSchedules(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-worker-cron-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "duraflow.db")
	dbStore := store.NewSQLiteStore(dbPath)
	if err := dbStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer dbStore.Close()

	hostExec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(dbStore, hostExec)
	w := NewWorkerDaemon(dbStore, eng)

	// Register a worker
	hostname, _ := os.Hostname()
	err = dbStore.RegisterWorker(&store.Worker{
		WorkerID:        w.workerID,
		Hostname:        hostname,
		PID:             os.Getpid(),
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 1. Register a Cron Schedule with overlap_policy = "skip"
	yamlContent := `
name: cron-workflow
version: 1
steps:
  - id: step-1
    run: "echo 'hello'"
`
	cs := &store.CronSchedule{
		WorkflowName:   "cron-workflow",
		CronExpression: "*/5 * * * *",
		OverlapPolicy:  "skip",
		NextRunTime:    time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano),
		DefinitionYAML: yamlContent,
		Status:         "ACTIVE",
	}
	if err := dbStore.UpsertCronSchedule(cs); err != nil {
		t.Fatalf("failed to upsert cron schedule: %v", err)
	}

	// Trigger cron schedule manually using w.triggerCronSchedules()
	w.triggerCronSchedules()

	// Verify a run was created
	list, err := dbStore.ListCronSchedules()
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 cron schedule, got %v, err: %v", list, err)
	}
	firstRunID := list[0].LastRunID
	if firstRunID == "" {
		t.Fatalf("expected last run ID to be set, got empty")
	}

	run, err := dbStore.GetRun(firstRunID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != "RUNNING" && run.Status != "CREATED" {
		t.Fatalf("expected run to be RUNNING or CREATED, got %s", run.Status)
	}

	// 2. Overlap Policy: skip
	// Set the next run time to the past again, to simulate next cron occurrence
	cs.NextRunTime = time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano)
	cs.LastRunID = firstRunID
	if err := dbStore.UpsertCronSchedule(cs); err != nil {
		t.Fatalf("failed to update cron schedule next run time: %v", err)
	}

	// Trigger cron schedules again
	w.triggerCronSchedules()

	// Since overlap policy is 'skip' and the first run is still running, it should NOT trigger a new run.
	list, err = dbStore.ListCronSchedules()
	if err != nil {
		t.Fatalf("failed to list cron schedules: %v", err)
	}
	if list[0].LastRunID != firstRunID {
		t.Fatalf("expected last run ID to remain %s, got %s", firstRunID, list[0].LastRunID)
	}

	// 3. Overlap Policy: allow
	// Set the overlap policy to 'allow'
	cs.OverlapPolicy = "allow"
	cs.NextRunTime = time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano)
	if err := dbStore.UpsertCronSchedule(cs); err != nil {
		t.Fatalf("failed to update cron schedule to allow: %v", err)
	}

	// Trigger cron schedules again
	w.triggerCronSchedules()

	// Now it should trigger a new run!
	list, err = dbStore.ListCronSchedules()
	if err != nil {
		t.Fatalf("failed to list cron schedules: %v", err)
	}
	secondRunID := list[0].LastRunID
	if secondRunID == "" || secondRunID == firstRunID {
		t.Fatalf("expected new run ID to be triggered under 'allow' policy, got %s", secondRunID)
	}

	// Both runs should exist in database
	run1, err := dbStore.GetRun(firstRunID)
	if err != nil {
		t.Fatalf("run1 not found: %v", err)
	}
	run2, err := dbStore.GetRun(secondRunID)
	if err != nil {
		t.Fatalf("run2 not found: %v", err)
	}
	if run1.RunID == run2.RunID {
		t.Fatalf("expected different run IDs, got same: %s", run1.RunID)
	}
}

func TestWorkerDaemon_CompensationSuccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-worker-comp-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "duraflow.db")
	dbStore := store.NewSQLiteStore(dbPath)
	if err := dbStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer dbStore.Close()

	hostExec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(dbStore, hostExec)
	w := NewWorkerDaemon(dbStore, eng)

	// Register worker
	hostname, _ := os.Hostname()
	err = dbStore.RegisterWorker(&store.Worker{
		WorkerID:        w.workerID,
		Hostname:        hostname,
		PID:             os.Getpid(),
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	})
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	// Define workflow with on_failure: compensate: true
	yamlContent := `
name: comp-test-workflow
version: 1
on_failure:
  compensate: true
steps:
  - id: step-1
    run: "echo 'step-1 succeeded'"
    compensation:
      run: "echo 'step-1 compensated'"
  - id: step-2
    run: "echo 'step-2 succeeded'"
    depends_on: ["step-1"]
  - id: step-3
    run: "false" # Fails immediately
    depends_on: ["step-2"]
    compensation:
      run: "echo 'step-3 compensated'"
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	// 1. Execute step-1
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// 2. Execute step-2
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// 3. Execute step-3 (which will fail and trigger compensation)
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// Check that run transitioned to COMPENSATING status
	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != engine.StatusCompensating {
		t.Fatalf("expected run status COMPENSATING, got %s", run.Status)
	}

	// Check step states
	states, _ := dbStore.GetStepStates(runID)
	stateMap := make(map[string]string)
	for _, st := range states {
		stateMap[st.StepID] = st.Status
	}
	if stateMap["step-1"] != engine.StepSucceeded {
		t.Errorf("step-1 should be SUCCEEDED, got %s", stateMap["step-1"])
	}
	if stateMap["step-2"] != engine.StepSucceeded {
		t.Errorf("step-2 should be SUCCEEDED, got %s", stateMap["step-2"])
	}
	if stateMap["step-3"] != engine.StepFailedFinal {
		t.Errorf("step-3 should be FAILED_FINAL, got %s", stateMap["step-3"])
	}

	// 4. Run compensation.
	// Since step-3 failed (not succeeded), only step-1 has a compensation block and succeeded.
	// (step-2 has no compensation block).
	// So step-1's compensation should run and complete.
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// Call scan again to trigger run-level state transition after steps are compensated
	w.scanAndRunEligibleSteps()

	// Check step states again: step-1 should be COMPENSATED
	states, _ = dbStore.GetStepStates(runID)
	for _, st := range states {
		stateMap[st.StepID] = st.Status
	}
	if stateMap["step-1"] != engine.StepCompensated {
		t.Errorf("step-1 should be COMPENSATED, got %s", stateMap["step-1"])
	}

	// Check run status again: should be COMPENSATED
	run, _ = dbStore.GetRun(runID)
	if run.Status != engine.StatusCompensated {
		t.Fatalf("expected run status COMPENSATED, got %s", run.Status)
	}
}

func TestWorkerDaemon_CompensationFailure(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-worker-comp-fail-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "duraflow.db")
	dbStore := store.NewSQLiteStore(dbPath)
	if err := dbStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer dbStore.Close()

	hostExec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(dbStore, hostExec)
	w := NewWorkerDaemon(dbStore, eng)

	hostname, _ := os.Hostname()
	_ = dbStore.RegisterWorker(&store.Worker{
		WorkerID:        w.workerID,
		Hostname:        hostname,
		PID:             os.Getpid(),
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	})

	yamlContent := `
name: comp-fail-workflow
version: 1
on_failure:
  compensate: true
steps:
  - id: step-1
    run: "echo 'hello'"
    compensation:
      run: "false" # Fails!
  - id: step-2
    run: "false"
    depends_on: ["step-1"]
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	// Execute step-1
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// Execute step-2 (fails)
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// Verify run is COMPENSATING
	run, _ := dbStore.GetRun(runID)
	if run.Status != engine.StatusCompensating {
		t.Fatalf("expected run status COMPENSATING, got %s", run.Status)
	}

	// Run compensation (fails)
	w.scanAndRunEligibleSteps()
	w.wg.Wait()

	// Call scan again to trigger run-level state transition after step compensation fails
	w.scanAndRunEligibleSteps()

	// Verify step-1 status is COMPENSATION_FAILED
	states, _ := dbStore.GetStepStates(runID)
	for _, st := range states {
		if st.StepID == "step-1" && st.Status != engine.StepCompensationFailed {
			t.Errorf("expected step-1 status COMPENSATION_FAILED, got %s", st.Status)
		}
	}

	// Verify run status is COMPENSATION_FAILED
	run, _ = dbStore.GetRun(runID)
	if run.Status != engine.StatusCompensationFailed {
		t.Fatalf("expected run status COMPENSATION_FAILED, got %s", run.Status)
	}
}

func TestWorkerDaemon_ManualCancelAndRetry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-worker-cancel-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "duraflow.db")
	dbStore := store.NewSQLiteStore(dbPath)
	if err := dbStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer dbStore.Close()

	hostExec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: cancel-workflow
version: 1
steps:
  - id: step-1
    run: "echo 'hello'"
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	// Cancel the run
	if err := dbStore.CancelWorkflowRun(runID); err != nil {
		t.Fatalf("failed to cancel run: %v", err)
	}

	run, _ := dbStore.GetRun(runID)
	if run.Status != engine.StatusCancelled {
		t.Fatalf("expected status CANCELLED, got %s", run.Status)
	}

	// Verify cancel event is present
	events, _ := dbStore.GetEvents(runID)
	foundCancel := false
	for _, ev := range events {
		if ev.EventType == engine.EventWorkflowCancelled {
			foundCancel = true
			break
		}
	}
	if !foundCancel {
		t.Errorf("expected EventWorkflowCancelled, but not found")
	}

	// Reset workflow run for retry (simulating retry command)
	if err := dbStore.ResetWorkflowRunForRetry(runID, engine.StatusRunning); err != nil {
		t.Fatalf("failed to reset workflow run: %v", err)
	}
	run, _ = dbStore.GetRun(runID)
	if run.Status != engine.StatusRunning {
		t.Fatalf("expected status RUNNING, got %s", run.Status)
	}
}
