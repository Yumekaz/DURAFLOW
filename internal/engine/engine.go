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
}

func NewWorkflowEngine(store store.EventStore, executor executor.Executor) *WorkflowEngine {
	return &WorkflowEngine{
		store:    store,
		executor: executor,
	}
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
	err := e.store.AppendEvent(&store.Event{
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
	err = e.store.AppendEvent(&store.Event{
		RunID:        runID,
		WorkflowName: def.Name,
		EventType:    EventWorkflowStarted,
		PayloadJSON:  "{}",
	})
	if err != nil {
		return runID, fmt.Errorf("failed to append workflow started event: %w", err)
	}

	// 6. Execute steps sequentially in topological order
	var workflowFailed bool
	var failedStepID string
	var lastErr string

	for _, step := range orderedSteps {
		// Event: StepScheduled
		err = e.store.AppendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepScheduled,
			StepID:       step.ID,
			PayloadJSON:  "{}",
		})
		if err != nil {
			return runID, fmt.Errorf("failed to append step scheduled event: %w", err)
		}

		// Update state to RUNNING
		now = time.Now().UTC().Format(time.RFC3339Nano)
		st := &store.StepState{
			RunID:       runID,
			StepID:      step.ID,
			Status:      StepRunning,
			Attempt:     1,
			MaxAttempts: step.Retry.MaxAttempts,
			StartedAt:   now,
		}
		if err := e.store.UpsertStepState(st); err != nil {
			return runID, fmt.Errorf("failed to update step state to running: %w", err)
		}

		// Event: StepStarted
		err = e.store.AppendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventStepStarted,
			StepID:       step.ID,
			Attempt:      1,
			PayloadJSON:  "{}",
		})
		if err != nil {
			return runID, fmt.Errorf("failed to append step started event: %w", err)
		}

		// Execute the command using helper method to scope context timeout defer
		res, execErr := e.executeStep(ctx, runID, def, step)
		
		stepErrStr := ""
		if execErr != nil {
			stepErrStr = execErr.Error()
		} else if res.ExitCode != 0 {
			stepErrStr = fmt.Sprintf("exit status %d", res.ExitCode)
		}

		// Append logs
		if res != nil {
			if res.Stdout != "" {
				_ = e.store.AppendLog(&store.LogEntry{
					RunID:   runID,
					StepID:  step.ID,
					Attempt: 1,
					Stream:  "stdout",
					Content: res.Stdout,
				})
			}
			if res.Stderr != "" {
				_ = e.store.AppendLog(&store.LogEntry{
					RunID:   runID,
					StepID:  step.ID,
					Attempt: 1,
					Stream:  "stderr",
					Content: res.Stderr,
				})
			}
		}

		now = time.Now().UTC().Format(time.RFC3339Nano)

		if stepErrStr != "" {
			workflowFailed = true
			failedStepID = step.ID
			lastErr = stepErrStr
			if res != nil && res.Stderr != "" {
				lastErr += ": " + res.Stderr
			}

			// Update state to FAILED
			st.Status = StepFailed
			st.LastError = stepErrStr
			st.CompletedAt = now
			_ = e.store.UpsertStepState(st)

			// Event: StepAttemptFailed (which serves as final failure in Phase 1)
			_ = e.store.AppendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepFailed,
				StepID:       step.ID,
				Attempt:      1,
				PayloadJSON:  fmt.Sprintf(`{"error":%q}`, stepErrStr),
			})
			break
		} else {
			// Update state to SUCCEEDED
			st.Status = StepSucceeded
			st.CompletedAt = now
			_ = e.store.UpsertStepState(st)

			// Event: StepSucceeded
			_ = e.store.AppendEvent(&store.Event{
				RunID:        runID,
				WorkflowName: def.Name,
				EventType:    EventStepSucceeded,
				StepID:       step.ID,
				Attempt:      1,
				PayloadJSON:  "{}",
			})
		}
	}

	// 7. Complete or Fail workflow run
	now = time.Now().UTC().Format(time.RFC3339Nano)
	if workflowFailed {
		_ = e.store.UpdateRunStatus(runID, StatusFailed, map[string]string{"failed_at": now})
		_ = e.store.AppendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowFailed,
			PayloadJSON:  fmt.Sprintf(`{"failed_step":%q,"error":%q}`, failedStepID, lastErr),
		})
	} else {
		_ = e.store.UpdateRunStatus(runID, StatusCompleted, map[string]string{"completed_at": now})
		_ = e.store.AppendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: def.Name,
			EventType:    EventWorkflowCompleted,
			PayloadJSON:  "{}",
		})
	}

	return runID, nil
}

func (e *WorkflowEngine) executeStep(ctx context.Context, runID string, def *workflow.WorkflowDef, step workflow.StepDef) (*executor.Result, error) {
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
	stepEnv["DURAFLOW_ATTEMPT"] = "1"

	return e.executor.Execute(stepCtx, step.Run, stepEnv)
}
