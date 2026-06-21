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

	// 6. Execute steps sequentially in topological order
	workflowFailed, failedStepID, lastErr, err := e.executeWorkflowSteps(ctx, runID, def, orderedSteps, nil)
	if err != nil {
		return runID, err
	}

	// 7. Complete or Fail workflow run
	now = time.Now().UTC().Format(time.RFC3339Nano)
	if workflowFailed {
		_ = e.store.UpdateRunStatus(runID, StatusFailed, map[string]string{"failed_at": now})
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowFailed,
			PayloadJSON:  fmt.Sprintf(`{"failed_step":%q,"error":%q}`, failedStepID, lastErr),
		})
	} else {
		_ = e.store.UpdateRunStatus(runID, StatusCompleted, map[string]string{"completed_at": now})
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowCompleted,
			PayloadJSON:  "{}",
		})
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
	if run.Status == StatusCompleted || run.Status == StatusFailed {
		return fmt.Errorf("cannot resume workflow run %s: status is already %s", runID, run.Status)
	}

	// 3. Fetch workflow definition
	yamlContent, err := e.store.GetWorkflowYAML(run.WorkflowName, run.WorkflowVersion)
	if err != nil {
		return fmt.Errorf("failed to fetch workflow definition: %w", err)
	}

	def, _, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		return fmt.Errorf("failed to parse stored workflow definition: %w", err)
	}

	// 4. Update status to RUNNING if not already, and append WorkflowResumed event
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := e.store.UpdateRunStatus(runID, StatusRunning, map[string]string{"started_at": now}); err != nil {
		return fmt.Errorf("failed to update run status to running: %w", err)
	}

	err = e.appendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: def.Name,
		EventType:    EventWorkflowResumed,
		PayloadJSON:  "{}",
	})
	if err != nil {
		return fmt.Errorf("failed to append workflow resumed event: %w", err)
	}

	// 5. Fetch existing step states to determine where to resume
	states, err := e.store.GetStepStates(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch step states: %w", err)
	}

	stateMap := make(map[string]*store.StepState)
	for _, st := range states {
		stateMap[st.StepID] = st
	}

	// 6. Execute remaining steps
	workflowFailed, failedStepID, lastErr, err := e.executeWorkflowSteps(ctx, runID, def, orderedSteps, stateMap)
	if err != nil {
		return fmt.Errorf("failed to execute workflow steps: %w", err)
	}

	// 7. Complete or Fail workflow run
	now = time.Now().UTC().Format(time.RFC3339Nano)
	if workflowFailed {
		_ = e.store.UpdateRunStatus(runID, StatusFailed, map[string]string{"failed_at": now})
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowFailed,
			PayloadJSON:  fmt.Sprintf(`{"failed_step":%q,"error":%q}`, failedStepID, lastErr),
		})
	} else {
		_ = e.store.UpdateRunStatus(runID, StatusCompleted, map[string]string{"completed_at": now})
		_ = e.appendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowCompleted,
			PayloadJSON:  "{}",
		})
	}

	return nil
}

func (e *WorkflowEngine) executeWorkflowSteps(
	ctx context.Context,
	runID string,
	def *workflow.WorkflowDef,
	orderedSteps []workflow.StepDef,
	stateMap map[string]*store.StepState,
) (workflowFailed bool, failedStepID string, lastErr string, err error) {
	for _, step := range orderedSteps {
		st, exists := stateMap[step.ID]
		startAttempt := 1
		isResumedStep := false

		if exists {
			if st.Status == StepSucceeded {
				continue // Already succeeded, skip
			}
			if st.Status == StepRunning {
				// Interrupted mid-execution; re-run the same attempt
				startAttempt = st.Attempt
				if startAttempt < 1 {
					startAttempt = 1
				}
				isResumedStep = true
			} else if st.Status == StepRetryScheduled {
				// Interrupted during retry delay; proceed to next attempt
				startAttempt = st.Attempt + 1
				isResumedStep = true
			}
		}

		if isResumedStep {
			err = e.appendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepResumed,
				StepID:       step.ID,
				Attempt:      startAttempt,
				PayloadJSON:  fmt.Sprintf(`{"previous_status":%q}`, st.Status),
			})
		} else {
			// Event: StepScheduled
			err = e.appendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepScheduled,
				StepID:       step.ID,
				PayloadJSON:  "{}",
			})
		}
		if err != nil {
			return false, "", "", fmt.Errorf("failed to append step scheduling event: %w", err)
		}

		maxAttempts := 1
		if step.Retry != nil && step.Retry.MaxAttempts > 0 {
			maxAttempts = step.Retry.MaxAttempts
		}

		var stepSucceeded bool

		for attempt := startAttempt; attempt <= maxAttempts; attempt++ {
			// Update state to RUNNING
			now := time.Now().UTC().Format(time.RFC3339Nano)
			stepState := &store.StepState{
				RunID:       runID,
				StepID:      step.ID,
				Status:      StepRunning,
				Attempt:     attempt,
				MaxAttempts: maxAttempts,
				StartedAt:   now,
			}
			if err := e.store.UpsertStepState(stepState); err != nil {
				return false, "", "", fmt.Errorf("failed to update step state to running: %w", err)
			}

			// Event: StepStarted
			err = e.appendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepStarted,
				StepID:       step.ID,
				Attempt:      attempt,
				PayloadJSON:  "{}",
			})
			if err != nil {
				return false, "", "", fmt.Errorf("failed to append step started event: %w", err)
			}

			// Execute the command
			res, execErr := e.executeStep(ctx, runID, def, step, attempt)

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

			now = time.Now().UTC().Format(time.RFC3339Nano)

			// If the parent context was cancelled/timed out, abort execution immediately
			if ctx.Err() != nil {
				workflowFailed = true
				failedStepID = step.ID
				lastErr = ctx.Err().Error()

				stepState.Status = StepFailed
				stepState.LastError = lastErr
				stepState.CompletedAt = now
				_ = e.store.UpsertStepState(stepState)

				_ = e.appendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: def.Name,
					EventType:    EventStepFailed,
					StepID:       step.ID,
					Attempt:      attempt,
					PayloadJSON:  fmt.Sprintf(`{"error":%q}`, lastErr),
				})
				break
			}

			if stepErrStr == "" {
				// Step succeeded!
				stepSucceeded = true
				stepState.Status = StepSucceeded
				stepState.CompletedAt = now
				_ = e.store.UpsertStepState(stepState)

				// Event: StepSucceeded
				_ = e.appendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: def.Name,
					EventType:    EventStepSucceeded,
					StepID:       step.ID,
					Attempt:      attempt,
					PayloadJSON:  "{}",
				})
				break
			}

			// It failed (or timed out). Determine if retry is possible
			exitCode := -1
			if res != nil {
				exitCode = res.ExitCode
			}

			retryable := isRetryable(step.Retry, exitCode)
			if isTimeout {
				retryable = true // timeouts are retryable by default
			}

			if !retryable || attempt >= maxAttempts {
				// No more attempts or non-retryable error: Final Failure
				stepState.Status = StepFailedFinal
				stepState.LastError = stepErrStr
				stepState.CompletedAt = now
				_ = e.store.UpsertStepState(stepState)

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

				workflowFailed = true
				failedStepID = step.ID
				lastErr = stepErrStr
				if res != nil && res.Stderr != "" {
					lastErr += ": " + res.Stderr
				}
				break
			}

			// Retry is scheduled
			delay := CalculateBackoff(step.Retry, attempt)
			nextRetryAt := time.Now().Add(delay).UTC().Format(time.RFC3339Nano)

			stepState.Status = StepRetryScheduled
			stepState.LastError = stepErrStr
			stepState.NextRetryAt = nextRetryAt
			_ = e.store.UpsertStepState(stepState)

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

			// Delay sleep, yielding to context cancel/timeout
			select {
			case <-ctx.Done():
				workflowFailed = true
				failedStepID = step.ID
				lastErr = ctx.Err().Error()
			case <-time.After(delay):
			}

			if workflowFailed {
				break
			}
		}

		if workflowFailed || !stepSucceeded {
			workflowFailed = true
			break
		}
	}

	return workflowFailed, failedStepID, lastErr, nil
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

	return e.executor.Execute(stepCtx, step.Run, stepEnv)
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
