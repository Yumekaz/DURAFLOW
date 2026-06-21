package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yumekaz/duraflow/internal/executor"
	"github.com/yumekaz/duraflow/internal/store"
	"github.com/yumekaz/duraflow/internal/workflow"
)

func TestWorkflowEngine_SuccessRun(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-success
version: 1
env:
  TEST_KEY: "test_val"
steps:
  - id: step-1
    run: "echo $TEST_KEY"
  - id: step-2
    run: "echo 'hello step 2'"
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

	// Verify run record
	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Errorf("expected run status COMPLETED, got %q", run.Status)
	}

	// Verify step states
	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 step states, got %d", len(states))
	}
	for _, state := range states {
		if state.Status != StepSucceeded {
			t.Errorf("expected step %s to be SUCCEEDED, got %s", state.StepID, state.Status)
		}
	}

	// Verify events
	events, err := dbStore.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	expectedEventTypes := []string{
		EventWorkflowRunCreated,
		EventWorkflowStarted,
		EventStepScheduled,
		EventStepStarted,
		EventStepSucceeded,
		EventStepScheduled,
		EventStepStarted,
		EventStepSucceeded,
		EventWorkflowCompleted,
	}
	if len(events) != len(expectedEventTypes) {
		t.Fatalf("expected %d events, got %d", len(expectedEventTypes), len(events))
	}
	for i, ev := range events {
		if ev.EventType != expectedEventTypes[i] {
			t.Errorf("expected event %d type %s, got %s", i, expectedEventTypes[i], ev.EventType)
		}
	}

	// Verify logs
	logs1, err := dbStore.GetLogs(runID, "step-1")
	if err != nil {
		t.Fatalf("failed to get logs for step-1: %v", err)
	}
	if len(logs1) != 1 || logs1[0].Content != "test_val\n" {
		t.Errorf("unexpected logs for step-1: %+v", logs1)
	}

	logs2, err := dbStore.GetLogs(runID, "step-2")
	if err != nil {
		t.Fatalf("failed to get logs for step-2: %v", err)
	}
	if len(logs2) != 1 || logs2[0].Content != "hello step 2\n" {
		t.Errorf("unexpected logs for step-2: %+v", logs2)
	}
}

func TestWorkflowEngine_FailureAbort(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-fail
version: 1
steps:
  - id: step-1
    run: "echo 'failed' >&2; exit 1"
  - id: step-2
    run: "echo 'should not run'"
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

	// Verify run record shows failed
	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != StatusFailed {
		t.Errorf("expected run status FAILED, got %q", run.Status)
	}

	// Verify step states (step-1 failed final, step-2 remains pending)
	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 step states, got %d", len(states))
	}
	for _, state := range states {
		if state.StepID == "step-1" && state.Status != StepFailedFinal {
			t.Errorf("expected step-1 to be FAILED_FINAL, got %s", state.Status)
		}
		if state.StepID == "step-2" && state.Status != StepPending {
			t.Errorf("expected step-2 to remain PENDING, got %s", state.Status)
		}
	}

	// Verify events show workflow failed
	events, err := dbStore.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	expectedEventTypes := []string{
		EventWorkflowRunCreated,
		EventWorkflowStarted,
		EventStepScheduled,
		EventStepStarted,
		EventStepFailed,
		EventStepFailedFinal,
		EventWorkflowFailed,
	}
	if len(events) != len(expectedEventTypes) {
		t.Fatalf("expected %d events, got %d", len(expectedEventTypes), len(events))
	}
	for i, ev := range events {
		if ev.EventType != expectedEventTypes[i] {
			t.Errorf("expected event %d type %s, got %s", i, expectedEventTypes[i], ev.EventType)
		}
	}
}

func TestRetry_SucceedOnThirdAttempt(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	countFile := filepath.Join(tmpDir, "count.txt")
	yamlContent := fmt.Sprintf(`
name: test-retry-succeed
version: 1
steps:
  - id: flaky
    run: "if [ ! -f %s ]; then echo 1 > %s; exit 1; elif [ $(cat %s) -eq 1 ]; then echo 2 > %s; exit 1; else exit 0; fi"
    retry:
      max_attempts: 3
      backoff: fixed
      delay_ms: 10
`, countFile, countFile, countFile, countFile)

	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != StatusCompleted {
		t.Errorf("expected run status COMPLETED, got %q", run.Status)
	}

	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	if len(states) != 1 {
		t.Fatalf("expected 1 step state, got %d", len(states))
	}
	st := states[0]
	if st.Status != StepSucceeded {
		t.Errorf("expected step to be SUCCEEDED, got %s", st.Status)
	}
	if st.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", st.Attempt)
	}

	events, err := dbStore.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	expectedEventTypes := []string{
		EventWorkflowRunCreated,
		EventWorkflowStarted,
		EventStepScheduled,
		EventStepStarted,        // attempt 1
		EventStepFailed,         // attempt 1 failed
		EventStepRetryScheduled, // attempt 1 scheduled
		EventStepStarted,        // attempt 2
		EventStepFailed,         // attempt 2 failed
		EventStepRetryScheduled, // attempt 2 scheduled
		EventStepStarted,        // attempt 3
		EventStepSucceeded,      // attempt 3 succeeded
		EventWorkflowCompleted,
	}
	if len(events) != len(expectedEventTypes) {
		t.Fatalf("expected %d events, got %d", len(expectedEventTypes), len(events))
	}
	for i, ev := range events {
		if ev.EventType != expectedEventTypes[i] {
			t.Errorf("expected event %d type %s, got %s", i, expectedEventTypes[i], ev.EventType)
		}
	}
}

func TestRetry_ExhaustedAllAttempts(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-retry-exhausted
version: 1
steps:
  - id: failing
    run: "exit 5"
    retry:
      max_attempts: 3
      backoff: fixed
      delay_ms: 10
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != StatusFailed {
		t.Errorf("expected run status FAILED, got %q", run.Status)
	}

	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	st := states[0]
	if st.Status != StepFailedFinal {
		t.Errorf("expected step status FAILED_FINAL, got %s", st.Status)
	}
	if st.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", st.Attempt)
	}

	events, err := dbStore.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	expectedEventTypes := []string{
		EventWorkflowRunCreated,
		EventWorkflowStarted,
		EventStepScheduled,
		EventStepStarted,        // attempt 1
		EventStepFailed,         // attempt 1 failed
		EventStepRetryScheduled, // attempt 1 scheduled
		EventStepStarted,        // attempt 2
		EventStepFailed,         // attempt 2 failed
		EventStepRetryScheduled, // attempt 2 scheduled
		EventStepStarted,        // attempt 3
		EventStepFailed,         // attempt 3 failed
		EventStepFailedFinal,    // final fail
		EventWorkflowFailed,
	}
	if len(events) != len(expectedEventTypes) {
		t.Fatalf("expected %d events, got %d", len(expectedEventTypes), len(events))
	}
	for i, ev := range events {
		if ev.EventType != expectedEventTypes[i] {
			t.Errorf("expected event %d type %s, got %s", i, expectedEventTypes[i], ev.EventType)
		}
	}
}

func TestRetry_ExponentialBackoff(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-retry-exponential
version: 1
steps:
  - id: failing
    run: "exit 5"
    retry:
      max_attempts: 4
      backoff: exponential
      initial_delay_ms: 10
      max_delay_ms: 100
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	events, err := dbStore.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}

	var delays []int64
	for _, ev := range events {
		if ev.EventType == EventStepRetryScheduled {
			var p struct {
				DelayMs int64 `json:"delay_ms"`
			}
			if err := json.Unmarshal([]byte(ev.PayloadJSON), &p); err == nil {
				delays = append(delays, p.DelayMs)
			}
		}
	}

	expectedDelays := []int64{10, 20, 40}
	if len(delays) != len(expectedDelays) {
		t.Fatalf("expected %d retry events, got %d: %+v", len(expectedDelays), len(delays), delays)
	}
	for i, d := range delays {
		if d != expectedDelays[i] {
			t.Errorf("attempt %d: expected delay %d ms, got %d ms", i+1, expectedDelays[i], d)
		}
	}
}

func TestTimeout_StepKilled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-timeout
version: 1
steps:
  - id: slow-step
    run: "sleep 2"
    timeout_ms: 100
    retry:
      max_attempts: 2
      backoff: fixed
      delay_ms: 10
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	run, err := dbStore.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if run.Status != StatusFailed {
		t.Errorf("expected run status FAILED, got %q", run.Status)
	}

	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	st := states[0]
	if st.Status != StepFailedFinal {
		t.Errorf("expected step status FAILED_FINAL, got %s", st.Status)
	}
	if st.Attempt != 2 {
		t.Errorf("expected attempt 2, got %d", st.Attempt)
	}

	events, err := dbStore.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}

	var hasTimeoutEvent bool
	for _, ev := range events {
		if ev.EventType == EventStepTimedOut {
			hasTimeoutEvent = true
		}
	}
	if !hasTimeoutEvent {
		t.Error("expected at least one StepTimedOut event, but found none")
	}
}

func TestRetry_NonRetryableExitCode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-non-retryable
version: 1
steps:
  - id: non-retryable
    run: "exit 42"
    retry:
      max_attempts: 5
      backoff: fixed
      delay_ms: 10
      no_retry_on_exit_codes: [42]
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	st := states[0]
	if st.Status != StepFailedFinal {
		t.Errorf("expected step status FAILED_FINAL, got %s", st.Status)
	}
	if st.Attempt != 1 {
		t.Errorf("expected attempt 1 (no retries), got %d", st.Attempt)
	}
}

func TestRetry_OnlyRetryOnSpecificExitCodes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-engine-test-*")
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
	eng := NewWorkflowEngine(dbStore, hostExec)

	yamlContent := `
name: test-retry-specific
version: 1
steps:
  - id: non-matching
    run: "exit 5"
    retry:
      max_attempts: 5
      backoff: fixed
      delay_ms: 10
      retry_on_exit_codes: [7]
`
	def, hash, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		t.Fatalf("failed to parse workflow: %v", err)
	}

	runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, yamlContent)
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}

	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	st := states[0]
	if st.Status != StepFailedFinal {
		t.Errorf("expected step status FAILED_FINAL, got %s", st.Status)
	}
	if st.Attempt != 1 {
		t.Errorf("expected attempt 1 (no retries), got %d", st.Attempt)
	}
}
