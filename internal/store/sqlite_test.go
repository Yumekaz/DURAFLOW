package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yumekaz/duraflow/internal/workflow"
)

func TestSQLiteStore_LifecycleAndOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	store := NewSQLiteStore(dbPath)

	if err := store.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer store.Close()

	// 1. Test RegisterWorkflow and GetWorkflowDef
	def := &workflow.WorkflowDef{
		Name:    "test-workflow",
		Version: 1,
		Steps: []workflow.StepDef{
			{ID: "step-1", Run: "echo 'hello'"},
		},
	}
	yamlContent := `
name: test-workflow
version: 1
steps:
  - id: step-1
    run: "echo 'hello'"
`
	err = store.RegisterWorkflow(def, "somehash123", yamlContent)
	if err != nil {
		t.Fatalf("failed to register workflow: %v", err)
	}

	fetchedDef, err := store.GetWorkflowDef("test-workflow", 1)
	if err != nil {
		t.Fatalf("failed to get workflow definition: %v", err)
	}
	if fetchedDef.Name != "test-workflow" || len(fetchedDef.Steps) != 1 || fetchedDef.Steps[0].ID != "step-1" {
		t.Errorf("fetched definition does not match registered: %+v", fetchedDef)
	}

	// 2. Test CreateRun and GetRun / ListRuns
	run := &WorkflowRun{
		RunID:           "run-1",
		WorkflowName:    "test-workflow",
		WorkflowVersion: 1,
		Status:          "CREATED",
		MetadataJSON:    "{}",
	}
	err = store.CreateRun(run)
	if err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	fetchedRun, err := store.GetRun("run-1")
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if fetchedRun.RunID != "run-1" || fetchedRun.Status != "CREATED" {
		t.Errorf("fetched run does not match created: %+v", fetchedRun)
	}

	// UpdateRunStatus
	err = store.UpdateRunStatus("run-1", "RUNNING", map[string]string{"started_at": "2026-06-21T00:00:00Z"})
	if err != nil {
		t.Fatalf("failed to update run status: %v", err)
	}

	fetchedRun, err = store.GetRun("run-1")
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}
	if fetchedRun.Status != "RUNNING" || fetchedRun.StartedAt != "2026-06-21T00:00:00Z" {
		t.Errorf("failed to update run status or started_at: %+v", fetchedRun)
	}

	runs, err := store.ListRuns(10)
	if err != nil {
		t.Fatalf("failed to list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != "run-1" {
		t.Errorf("expected 1 run with ID 'run-1', got: %d runs", len(runs))
	}

	// 3. Test AppendEvent and GetEvents
	event := &Event{
		RunID:        "run-1",
		WorkflowName: "test-workflow",
		EventType:    "WorkflowStarted",
		PayloadJSON:  "{}",
	}
	err = store.AppendEvent(event)
	if err != nil {
		t.Fatalf("failed to append event: %v", err)
	}

	events, err := store.GetEvents("run-1")
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "WorkflowStarted" {
		t.Errorf("expected 1 event with type 'WorkflowStarted', got %+v", events)
	}

	// 4. Test UpsertStepState and GetStepStates
	stepState := &StepState{
		RunID:       "run-1",
		StepID:      "step-1",
		Status:      "RUNNING",
		Attempt:     1,
		MaxAttempts: 3,
		StartedAt:   "2026-06-21T00:01:00Z",
	}
	err = store.UpsertStepState(stepState)
	if err != nil {
		t.Fatalf("failed to upsert step state: %v", err)
	}

	states, err := store.GetStepStates("run-1")
	if err != nil {
		t.Fatalf("failed to get step states: %v", err)
	}
	if len(states) != 1 || states[0].StepID != "step-1" || states[0].Status != "RUNNING" {
		t.Errorf("expected 1 step state with ID 'step-1' and status 'RUNNING', got %+v", states)
	}

	// 5. Test AppendLog and GetLogs
	logEntry := &LogEntry{
		RunID:   "run-1",
		StepID:  "step-1",
		Attempt: 1,
		Stream:  "stdout",
		Content: "hello output",
	}
	err = store.AppendLog(logEntry)
	if err != nil {
		t.Fatalf("failed to append log: %v", err)
	}

	logs, err := store.GetLogs("run-1", "step-1")
	if err != nil {
		t.Fatalf("failed to get logs: %v", err)
	}
	if len(logs) != 1 || logs[0].Content != "hello output" || logs[0].Stream != "stdout" {
		t.Errorf("expected 1 log entry with content 'hello output', got %+v", logs)
	}
}
