package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/yumekaz/duraflow/pkg/engine"
	"github.com/yumekaz/duraflow/pkg/executor"
	"github.com/yumekaz/duraflow/pkg/store"
)

func TestServer_WorkflowRoutes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	sStore := store.NewSQLiteStore(dbPath)
	if err := sStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer sStore.Close()

	exec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(sStore, exec)

	srv := NewServer(sStore, eng, "127.0.0.1:0", false)
	handler := srv.Handler()

	workflowYAML := `
name: test-workflow
version: 1
steps:
  - id: step-1
    run: "echo hello"
  - id: step-2
    run: "echo world"
`

	// 1. Trigger workflow run
	body := map[string]string{"yaml": workflowYAML}
	bodyBytes, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", "/api/v1/workflows/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var runResp struct {
		RunID        string `json:"run_id"`
		WorkflowName string `json:"workflow_name"`
		Status       string `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &runResp); err != nil {
		t.Fatalf("failed to unmarshal run response: %v", err)
	}

	if runResp.RunID == "" {
		t.Fatal("expected non-empty run_id")
	}
	if runResp.WorkflowName != "test-workflow" {
		t.Errorf("expected workflow name 'test-workflow', got %q", runResp.WorkflowName)
	}

	runID := runResp.RunID

	// 2. GET /api/v1/runs (list runs)
	req = httptest.NewRequest("GET", "/api/v1/runs", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var runs []*store.WorkflowRun
	if err := json.Unmarshal(w.Body.Bytes(), &runs); err != nil {
		t.Fatalf("failed to unmarshal runs list: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected 1 run in list, got %d", len(runs))
	}
	if runs[0].RunID != runID {
		t.Errorf("expected run ID %q, got %q", runID, runs[0].RunID)
	}

	// 3. GET /api/v1/runs/{run_id} (inspect run)
	req = httptest.NewRequest("GET", "/api/v1/runs/"+runID, nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var inspectResp struct {
		Run   store.WorkflowRun   `json:"run"`
		Steps []*store.StepState `json:"steps"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &inspectResp); err != nil {
		t.Fatalf("failed to unmarshal inspect response: %v", err)
	}
	if inspectResp.Run.RunID != runID {
		t.Errorf("expected run ID %q, got %q", runID, inspectResp.Run.RunID)
	}
	if len(inspectResp.Steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(inspectResp.Steps))
	}

	// 4. GET /api/v1/runs/{run_id}/events (inspect events)
	req = httptest.NewRequest("GET", "/api/v1/runs/"+runID+"/events", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var events []*store.Event
	if err := json.Unmarshal(w.Body.Bytes(), &events); err != nil {
		t.Fatalf("failed to unmarshal events: %v", err)
	}
	if len(events) == 0 {
		t.Error("expected at least one event")
	}

	// 5. GET /api/v1/runs/{run_id}/logs (inspect logs - should be empty initially)
	req = httptest.NewRequest("GET", "/api/v1/runs/"+runID+"/logs", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	var logs []*store.LogEntry
	_ = json.Unmarshal(w.Body.Bytes(), &logs)
	if len(logs) != 0 {
		t.Errorf("expected 0 logs, got %d", len(logs))
	}

	// Write mock log entry to test logs endpoint
	_ = sStore.AppendLog(&store.LogEntry{
		RunID:     runID,
		StepID:    "step-1",
		Attempt:   1,
		Stream:    "stdout",
		Content:   "test content",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})

	req = httptest.NewRequest("GET", "/api/v1/runs/"+runID+"/logs", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	_ = json.Unmarshal(w.Body.Bytes(), &logs)
	if len(logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(logs))
	}

	// 6. POST /api/v1/runs/{run_id}/cancel
	req = httptest.NewRequest("POST", "/api/v1/runs/"+runID+"/cancel", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}
	var cancelResp struct {
		Message string `json:"message"`
		RunID   string `json:"run_id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &cancelResp)
	if cancelResp.RunID != runID {
		t.Errorf("expected run ID %q, got %q", runID, cancelResp.RunID)
	}

	// Verify status in DB is CANCELLED
	run, _ := sStore.GetRun(runID)
	if run.Status != engine.StatusCancelled {
		t.Errorf("expected run status to be CANCELLED, got %s", run.Status)
	}

	// 7. POST /api/v1/runs/{run_id}/retry
	// Mark step-1 as FAILED_FINAL so it's a retry candidate
	statesList, _ := sStore.GetStepStates(runID)
	var st *store.StepState
	for _, s := range statesList {
		if s.StepID == "step-1" {
			st = s
			break
		}
	}
	st.Status = engine.StepFailedFinal
	_ = sStore.UpsertStepState(st)

	retryBody := map[string]string{"step_id": "step-1"}
	retryBodyBytes, _ := json.Marshal(retryBody)
	req = httptest.NewRequest("POST", "/api/v1/runs/"+runID+"/retry", bytes.NewBuffer(retryBodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	// Verify run status in DB is reset to RUNNING and step-1 is PENDING
	run, _ = sStore.GetRun(runID)
	if run.Status != engine.StatusRunning {
		t.Errorf("expected run status reset to RUNNING, got %s", run.Status)
	}
	statesList, _ = sStore.GetStepStates(runID)
	var stRetry *store.StepState
	for _, s := range statesList {
		if s.StepID == "step-1" {
			stRetry = s
			break
		}
	}
	if stRetry.Status != engine.StepPending {
		t.Errorf("expected step status reset to PENDING, got %s", stRetry.Status)
	}

	// 8. POST /api/v1/runs/{run_id}/resume
	// First mark run as failed
	_ = sStore.UpdateRunStatus(runID, engine.StatusFailed, nil)

	req = httptest.NewRequest("POST", "/api/v1/runs/"+runID+"/resume", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	run, _ = sStore.GetRun(runID)
	if run.Status != engine.StatusRunning {
		t.Errorf("expected run status resume to RUNNING, got %s", run.Status)
	}
}

func TestServer_WorkflowRoutes_Cron(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	sStore := store.NewSQLiteStore(dbPath)
	if err := sStore.Init(); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	defer sStore.Close()

	exec := executor.NewHostExecutor()
	eng := engine.NewWorkflowEngine(sStore, exec)

	srv := NewServer(sStore, eng, "127.0.0.1:0", false)
	handler := srv.Handler()

	cronWorkflowYAML := `
name: cron-workflow
version: 1
schedule:
  cron: "*/5 * * * *"
  overlap: skip
steps:
  - id: step-1
    run: "echo hello"
`

	// Trigger run with cron schedule
	req := httptest.NewRequest("POST", "/api/v1/workflows/run", bytes.NewBuffer([]byte(cronWorkflowYAML)))
	req.Header.Set("Content-Type", "application/x-yaml")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d, body: %s", w.Code, w.Body.String())
	}

	var cronResp struct {
		WorkflowName  string `json:"workflow_name"`
		Cron          string `json:"cron"`
		OverlapPolicy string `json:"overlap_policy"`
		NextRunTime   string `json:"next_run_time"`
		Status        string `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &cronResp); err != nil {
		t.Fatalf("failed to unmarshal cron response: %v", err)
	}

	if cronResp.WorkflowName != "cron-workflow" {
		t.Errorf("expected workflow name 'cron-workflow', got %q", cronResp.WorkflowName)
	}
	if cronResp.Status != "CRON_REGISTERED" {
		t.Errorf("expected status 'CRON_REGISTERED', got %q", cronResp.Status)
	}

	schedules, err := sStore.ListCronSchedules()
	if err != nil {
		t.Fatalf("failed to list cron schedules: %v", err)
	}
	if len(schedules) != 1 {
		t.Fatalf("expected 1 cron schedule, got %d", len(schedules))
	}
	if schedules[0].WorkflowName != "cron-workflow" {
		t.Errorf("expected registered cron name 'cron-workflow', got %q", schedules[0].WorkflowName)
	}
}
