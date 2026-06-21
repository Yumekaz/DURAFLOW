package engine

import (
	"context"
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

	// Verify step states (step-1 failed, step-2 remains pending)
	states, err := dbStore.GetStepStates(runID)
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected 2 step states, got %d", len(states))
	}
	for _, state := range states {
		if state.StepID == "step-1" && state.Status != StepFailed {
			t.Errorf("expected step-1 to be FAILED, got %s", state.Status)
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
