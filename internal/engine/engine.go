package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yumekaz/duraflow/internal/executor"
	"github.com/yumekaz/duraflow/internal/store"
	"github.com/yumekaz/duraflow/internal/workflow"
)

type WorkflowEngine struct {
	store    store.EventStore
	executor executor.Executor
	OnEvent  func(event *store.Event)
}

func NewWorkflowEngine(store store.EventStore, executor executor.Executor) *WorkflowEngine {
	return &WorkflowEngine{
		store:    store,
		executor: executor,
	}
}

func (e *WorkflowEngine) appendEvent(event *store.Event) error {
	if err := e.store.AppendEvent(event); err != nil {
		return err
	}
	if e.OnEvent != nil {
		e.OnEvent(event)
	}
	return nil
}

func (e *WorkflowEngine) AppendEvent(event *store.Event) error {
	return e.appendEvent(event)
}

func (e *WorkflowEngine) RunWorkflow(ctx context.Context, def *workflow.WorkflowDef, orderedSteps []workflow.StepDef, hash string, yamlContent string) (string, error) {
	runID := uuid.New().String()

	// 1. Register workflow definition
	if err := e.store.RegisterWorkflow(def, hash, yamlContent); err != nil {
		return "", fmt.Errorf("failed to register workflow: %w", err)
	}

	// 2. Create high-level run record
	now := time.Now().UTC().Format(time.RFC3339Nano)
	run := &store.WorkflowRun{
		RunID:           runID,
		WorkflowName:    def.Name,
		WorkflowVersion: def.Version,
		Status:          StatusCreated,
		CreatedAt:       now,
		MetadataJSON:    "{}",
	}
	if err := e.store.CreateRun(run); err != nil {
		return "", fmt.Errorf("failed to create workflow run: %w", err)
	}

	// 3. Append WorkflowRunCreated event
	err := e.appendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: def.Name,
		EventType:    EventWorkflowRunCreated,
		PayloadJSON:  "{}",
	})
	if err != nil {
		return runID, fmt.Errorf("failed to append run created event: %w", err)
	}

	// 4. Initialize step states to PENDING
	for _, step := range orderedSteps {
		st := &store.StepState{
			RunID:       runID,
			StepID:      step.ID,
			Status:      StepPending,
			Attempt:     0,
			MaxAttempts: step.Retry.MaxAttempts,
		}
		if err := e.store.UpsertStepState(st); err != nil {
			return runID, fmt.Errorf("failed to initialize step state for %s: %w", step.ID, err)
		}
	}

	// 5. Start workflow
	now = time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.store.UpdateRunStatus(runID, StatusRunning, map[string]string{"started_at": now}); err != nil {
		return runID, fmt.Errorf("failed to update run status to running: %w", err)
	}
	err = e.appendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: def.Name,
		EventType:    EventWorkflowStarted,
		PayloadJSON:  "{}",
	})
	if err != nil {
		return runID, fmt.Errorf("failed to append workflow started event: %w", err)
	}

	return runID, nil
}

func (e *WorkflowEngine) ResumeWorkflow(ctx context.Context, runID string) error {
	// 1. Fetch run details
	run, err := e.store.GetRun(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch run details: %w", err)
	}
	if run == nil {
		return fmt.Errorf("workflow run not found: %s", runID)
	}

	// 2. Check if run is in a resumable status
	if run.Status == StatusCompleted || run.Status == StatusCompensated {
		return fmt.Errorf("cannot resume workflow run %s: status is already %s", runID, run.Status)
	}

	// 3. Update status and append WorkflowResumed event
	targetStatus := StatusRunning
	if run.Status == StatusCompensating || run.Status == StatusCompensationFailed {
		targetStatus = StatusCompensating
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.store.UpdateRunStatus(runID, targetStatus, map[string]string{"started_at": now}); err != nil {
		return fmt.Errorf("failed to update run status to %s: %w", targetStatus, err)
	}

	err = e.appendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: run.WorkflowName,
		EventType:    EventWorkflowResumed,
		PayloadJSON:  "{}",
	})
	if err != nil {
		return fmt.Errorf("failed to append workflow resumed event: %w", err)
	}

	// 4. For any steps that were in RUNNING or RETRY_SCHEDULED state, append EventStepResumed event
	states, err := e.store.GetStepStates(runID)
	if err == nil {
		for _, st := range states {
			if st.Status == StepRunning || st.Status == StepRetryScheduled {
				err = e.appendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: run.WorkflowName,
					EventType:    EventStepResumed,
					StepID:       st.StepID,
					PayloadJSON:  "{}",
				})
				if err != nil {
					return fmt.Errorf("failed to append step resumed event for %s: %w", st.StepID, err)
				}
			}
		}
	}

	return nil
}

func (e *WorkflowEngine) ExecuteStepAttempt(ctx context.Context, runID string, def *workflow.WorkflowDef, step workflow.StepDef, attempt int) error {
	// 1. If this is the first attempt, append EventStepScheduled first
	if attempt == 1 {
		err := e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepScheduled,
			StepID:       step.ID,
			PayloadJSON:  "{}",
		})
		if err != nil {
			return fmt.Errorf("failed to append step scheduled event: %w", err)
		}
	}

	// 2. Event: StepStarted
	err := e.appendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: def.Name,
		EventType:    EventStepStarted,
		StepID:       step.ID,
		Attempt:      attempt,
		PayloadJSON:  "{}",
	})
	if err != nil {
		return fmt.Errorf("failed to append step started event: %w", err)
	}

	var res *executor.Result
	var execErr error
	if step.Wait != nil {
		res = &executor.Result{
			ExitCode: 0,
			Stdout:   "",
			Stderr:   "",
		}
	} else {
		res, execErr = e.executeStep(ctx, runID, def, step, attempt)
	}

	// Determine if it was a step timeout (deadline exceeded but parent context not cancelled)
	isTimeout := false
	if ctx.Err() == nil {
		if (res != nil && res.Error == context.DeadlineExceeded) || execErr == context.DeadlineExceeded {
			isTimeout = true
		}
	}

	stepErrStr := ""
	if execErr != nil {
		stepErrStr = execErr.Error()
	} else if res != nil && res.ExitCode != 0 {
		stepErrStr = fmt.Sprintf("exit status %d", res.ExitCode)
	} else if isTimeout {
		stepErrStr = "execution timeout"
	}

	// Append logs
	if res != nil {
		if res.Stdout != "" {
			_ = e.store.AppendLog(&store.LogEntry{
				RunID:   runID,
				StepID:  step.ID,
				Attempt: attempt,
				Stream:  "stdout",
				Content: res.Stdout,
			})
		}
		if res.Stderr != "" {
			_ = e.store.AppendLog(&store.LogEntry{
				RunID:   runID,
				StepID:  step.ID,
				Attempt: attempt,
				Stream:  "stderr",
				Content: res.Stderr,
			})
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	maxAttempts := 1
	if step.Retry != nil && step.Retry.MaxAttempts > 0 {
		maxAttempts = step.Retry.MaxAttempts
	}

	// If the parent context was cancelled/timed out, abort execution immediately
	if ctx.Err() != nil {
		st := &store.StepState{
			RunID:       runID,
			StepID:      step.ID,
			Status:      StepFailed,
			Attempt:     attempt,
			MaxAttempts: maxAttempts,
			LastError:   ctx.Err().Error(),
			CompletedAt: now,
		}
		_ = e.store.UpsertStepState(st)

		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepFailed,
			StepID:       step.ID,
			Attempt:      attempt,
			PayloadJSON:  fmt.Sprintf(`{"error":%q}`, ctx.Err().Error()),
		})
		return ctx.Err()
	}

	st := &store.StepState{
		RunID:       runID,
		StepID:      step.ID,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
	}

	if stepErrStr == "" {
		// Step succeeded!
		st.Status = StepSucceeded
		st.CompletedAt = now
		_ = e.store.UpsertStepState(st)

		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepSucceeded,
			StepID:       step.ID,
			Attempt:      attempt,
			PayloadJSON:  "{}",
		})

		// Check if all steps in the workflow are SUCCEEDED
		states, err := e.store.GetStepStates(runID)
		if err == nil {
			allSucceeded := true
			statusMap := make(map[string]string)
			for _, s := range states {
				statusMap[s.StepID] = s.Status
			}
			for _, stepDef := range def.Steps {
				if statusMap[stepDef.ID] != StepSucceeded {
					allSucceeded = false
					break
				}
			}
			if allSucceeded {
				// Mark workflow run as COMPLETED
				now = time.Now().UTC().Format(time.RFC3339Nano)
				_ = e.store.UpdateRunStatus(runID, StatusCompleted, map[string]string{"completed_at": now})
				_ = e.appendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: def.Name,
					EventType:    EventWorkflowCompleted,
					PayloadJSON:  "{}",
				})
			}
		}
		return nil
	}

	// It failed (or timed out). Determine if retry is possible
	exitCode := -1
	if res != nil {
		exitCode = res.ExitCode
	}

	retryable := isRetryable(step.Retry, exitCode)
	if isTimeout {
		retryable = true
	}

	if !retryable || attempt >= maxAttempts {
		// No more attempts or non-retryable error: Final Failure
		st.Status = StepFailedFinal
		st.LastError = stepErrStr
		st.CompletedAt = now
		_ = e.store.UpsertStepState(st)

		if isTimeout {
			_ = e.appendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepTimedOut,
				StepID:       step.ID,
				Attempt:      attempt,
				PayloadJSON:  fmt.Sprintf(`{"error":%q}`, stepErrStr),
			})
		} else {
			_ = e.appendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepFailed,
				StepID:       step.ID,
				Attempt:      attempt,
				PayloadJSON:  fmt.Sprintf(`{"error":%q}`, stepErrStr),
			})
		}

		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepFailedFinal,
			StepID:       step.ID,
			Attempt:      attempt,
			PayloadJSON:  fmt.Sprintf(`{"error":%q}`, stepErrStr),
		})

		// Check if we need to compensate
		if def.OnFailure != nil && def.OnFailure.Compensate {
			states, err := e.store.GetStepStates(runID)
			hasCompensation := false
			if err == nil {
				for _, stepDef := range def.Steps {
					if stepDef.Compensation != nil && stepDef.Compensation.Run != "" {
						for _, stState := range states {
							if stState.StepID == stepDef.ID && stState.Status == StepSucceeded {
								hasCompensation = true
								break
							}
						}
					}
					if hasCompensation {
						break
					}
				}
			}

			if hasCompensation {
				now = time.Now().UTC().Format(time.RFC3339Nano)
				_ = e.store.UpdateRunStatus(runID, StatusCompensating, nil)
				_ = e.appendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: def.Name,
					EventType:    EventWorkflowCompensationStarted,
					PayloadJSON:  fmt.Sprintf(`{"failed_step":%q,"error":%q}`, step.ID, stepErrStr),
				})
				return fmt.Errorf("step execution failed: %s; entering compensation", stepErrStr)
			}
		}

		// Mark workflow run as FAILED
		now = time.Now().UTC().Format(time.RFC3339Nano)
		_ = e.store.UpdateRunStatus(runID, StatusFailed, map[string]string{"failed_at": now})
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowFailed,
			PayloadJSON:  fmt.Sprintf(`{"failed_step":%q,"error":%q}`, step.ID, stepErrStr),
		})

		return fmt.Errorf("step execution failed: %s", stepErrStr)
	}

	// Retry is scheduled
	delay := CalculateBackoff(step.Retry, attempt)
	nextRetryAt := time.Now().Add(delay).UTC().Format(time.RFC3339Nano)

	st.Status = StepRetryScheduled
	st.LastError = stepErrStr
	st.NextRetryAt = nextRetryAt
	_ = e.store.UpsertStepState(st)

	if isTimeout {
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepTimedOut,
			StepID:       step.ID,
			Attempt:      attempt,
			PayloadJSON:  fmt.Sprintf(`{"error":%q}`, stepErrStr),
		})
	} else {
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepFailed,
			StepID:       step.ID,
			Attempt:      attempt,
			PayloadJSON:  fmt.Sprintf(`{"error":%q}`, stepErrStr),
		})
	}

	_ = e.appendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: def.Name,
		EventType:    EventStepRetryScheduled,
		StepID:       step.ID,
		Attempt:      attempt,
		PayloadJSON:  fmt.Sprintf(`{"delay_ms":%d,"next_retry_at":%q}`, delay.Milliseconds(), nextRetryAt),
	})

	return fmt.Errorf("step execution failed, retry scheduled: %s", stepErrStr)
}

func (e *WorkflowEngine) executeStep(ctx context.Context, runID string, def *workflow.WorkflowDef, step workflow.StepDef, attempt int) (*executor.Result, error) {
	stepCtx := ctx
	if step.TimeoutMs > 0 {
		var cancel context.CancelFunc
		stepCtx, cancel = context.WithTimeout(ctx, time.Duration(step.TimeoutMs)*time.Millisecond)
		defer cancel()
	}

	stepEnv := make(map[string]string)
	for k, v := range def.Env {
		stepEnv[k] = v
	}
	stepEnv["DURAFLOW_RUN_ID"] = runID
	stepEnv["DURAFLOW_STEP_ID"] = step.ID
	stepEnv["DURAFLOW_ATTEMPT"] = fmt.Sprintf("%d", attempt)

	req := executor.ExecutionRequest{
		Executor:  step.Executor,
		Command:   step.Run,
		Env:       stepEnv,
		Image:     step.Image,
		CPU:       step.CPU,
		Memory:    step.Memory,
		TimeoutMs: step.TimeoutMs,
	}

	return e.executor.Execute(stepCtx, req)
}

func isRetryable(policy *workflow.RetryPolicy, exitCode int) bool {
	if policy == nil {
		return true
	}

	if len(policy.NoRetryOnExitCodes) > 0 {
		for _, code := range policy.NoRetryOnExitCodes {
			if code == exitCode {
				return false
			}
		}
	}

	if len(policy.RetryOnExitCodes) > 0 {
		for _, code := range policy.RetryOnExitCodes {
			if code == exitCode {
				return true
			}
		}
		return false
	}

	return true
}

func (e *WorkflowEngine) ExecuteCompensationStep(ctx context.Context, runID string, def *workflow.WorkflowDef, step workflow.StepDef) (*executor.Result, error) {
	if step.Compensation == nil || step.Compensation.Run == "" {
		return &executor.Result{ExitCode: 0}, nil
	}

	stepEnv := make(map[string]string)
	for k, v := range def.Env {
		stepEnv[k] = v
	}
	stepEnv["DURAFLOW_RUN_ID"] = runID
	stepEnv["DURAFLOW_STEP_ID"] = step.ID

	req := executor.ExecutionRequest{
		Executor:  step.Executor,
		Command:   step.Compensation.Run,
		Env:       stepEnv,
		Image:     step.Image,
		CPU:       step.CPU,
		Memory:    step.Memory,
		TimeoutMs: step.TimeoutMs,
	}

	return e.executor.Execute(ctx, req)
}
