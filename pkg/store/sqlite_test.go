package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yumekaz/duraflow/pkg/workflow"
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

	// 6. Test GetWorkflowYAML
	yamlContentFetched, err := store.GetWorkflowYAML("test-workflow", 1)
	if err != nil {
		t.Fatalf("failed to get workflow YAML: %v", err)
	}
	if yamlContentFetched != yamlContent {
		t.Errorf("expected workflow YAML %q, got %q", yamlContent, yamlContentFetched)
	}

	// 7. Test GetIncompleteRuns
	incomplete, err := store.GetIncompleteRuns()
	if err != nil {
		t.Fatalf("failed to get incomplete runs: %v", err)
	}
	if len(incomplete) != 1 || incomplete[0].RunID != "run-1" {
		t.Errorf("expected 1 incomplete run with ID 'run-1', got: %+v", incomplete)
	}

	// Change run-1 to COMPLETED and verify incomplete is empty
	err = store.UpdateRunStatus("run-1", "COMPLETED", nil)
	if err != nil {
		t.Fatalf("failed to update status: %v", err)
	}
	incomplete, err = store.GetIncompleteRuns()
	if err != nil {
		t.Fatalf("failed to get incomplete runs: %v", err)
	}
	if len(incomplete) != 0 {
		t.Errorf("expected 0 incomplete runs, got: %+v", incomplete)
	}
}

func TestSQLiteStore_WorkersAndLeases(t *testing.T) {
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

	// 1. Worker Registration and Heartbeats
	w1 := &Worker{
		WorkerID:        "worker-1",
		Hostname:        "localhost",
		PID:             1001,
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	}
	err = store.RegisterWorker(w1)
	if err != nil {
		t.Fatalf("failed to register w1: %v", err)
	}

	w2 := &Worker{
		WorkerID:        "worker-2",
		Hostname:        "localhost",
		PID:             1002,
		StartedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		LastHeartbeatAt: time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "ACTIVE",
	}
	err = store.RegisterWorker(w2)
	if err != nil {
		t.Fatalf("failed to register w2: %v", err)
	}

	active, err := store.GetActiveWorkers()
	if err != nil {
		t.Fatalf("failed to get active workers: %v", err)
	}
	if len(active) != 2 {
		t.Errorf("expected 2 active workers, got %d", len(active))
	}

	err = store.HeartbeatWorker("worker-1")
	if err != nil {
		t.Fatalf("failed to heartbeat worker-1: %v", err)
	}

	// 2. Lease Acquisition & Contention
	// Setup run and step
	run := &WorkflowRun{
		RunID:           "run-1",
		WorkflowName:    "test-workflow",
		WorkflowVersion: 1,
		Status:          "RUNNING",
	}
	_ = store.CreateRun(run)
	_ = store.UpsertStepState(&StepState{
		RunID:       "run-1",
		StepID:      "step-1",
		Status:      "PENDING",
		Attempt:     0,
		MaxAttempts: 3,
	})

	// Acquire w1 lease
	acquired, err := store.AcquireLease("run-1", "step-1", "worker-1", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Errorf("expected lease to be acquired by worker-1")
	}

	// Double check step state status updated to RUNNING and worker_id set
	states, _ := store.GetStepStates("run-1")
	if len(states) != 1 || states[0].Status != "RUNNING" || states[0].WorkerID != "worker-1" || states[0].Attempt != 1 {
		t.Errorf("expected step state to be RUNNING/worker-1/attempt-1, got: %+v", states)
	}

	// Try acquire w2 lease (should fail/contend)
	acquired, err = store.AcquireLease("run-1", "step-1", "worker-2", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if acquired {
		t.Errorf("expected lease acquisition to fail due to contention")
	}

	// 3. Lease Renewal
	renewed, err := store.RenewLease("run-1", "step-1", "worker-1", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to renew lease: %v", err)
	}
	if !renewed {
		t.Errorf("expected lease to be renewed for worker-1")
	}

	// Try renew for worker-2 on same step (should fail)
	renewed, err = store.RenewLease("run-1", "step-1", "worker-2", 5*time.Second)
	if err != nil {
		t.Fatalf("failed to renew lease: %v", err)
	}
	if renewed {
		t.Errorf("expected lease renewal for worker-2 to fail")
	}

	// 4. Lease Release
	err = store.ReleaseLease("run-1", "step-1", "worker-1")
	if err != nil {
		t.Fatalf("failed to release lease: %v", err)
	}

	// Now worker-2 should be able to acquire lease (it is released)
	acquired, err = store.AcquireLease("run-1", "step-1", "worker-2", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Errorf("expected lease to be acquired by worker-2 after release")
	}

	// 5. Lease Expiration
	// Set lease duration very short and wait
	acquired, err = store.AcquireLease("run-1", "step-2", "worker-2", 2*time.Millisecond)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Errorf("expected lease to be acquired by worker-2")
	}

	time.Sleep(5 * time.Millisecond)

	// Now worker-1 should steal it (expired)
	acquired, err = store.AcquireLease("run-1", "step-2", "worker-1", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Errorf("expected lease on step-2 to be acquired by worker-1 after expiration")
	}

	// 6. Dead Worker Reclamation
	// w1 owns step-2 now. Manually make w1's heartbeat stale (20 seconds ago)
	staleHeartbeat := time.Now().Add(-20 * time.Second).UTC().Format(time.RFC3339Nano)
	w1.LastHeartbeatAt = staleHeartbeat
	_ = store.RegisterWorker(w1)

	// worker-2 should reclaim the lease even though it has not expired in time
	acquired, err = store.AcquireLease("run-1", "step-2", "worker-2", 1*time.Second)
	if err != nil {
		t.Fatalf("failed to acquire lease: %v", err)
	}
	if !acquired {
		t.Errorf("expected lease on step-2 to be reclaimed by worker-2 due to dead worker-1")
	}
}

func TestTimersAndCronSchedules(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duraflow-store-test-*")
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

	// 1. Timers
	// Create workflow run first to satisfy foreign key constraint
	run := &WorkflowRun{
		RunID:           "run-1",
		WorkflowName:    "test-workflow",
		WorkflowVersion: 1,
		Status:          "CREATED",
		MetadataJSON:    "{}",
	}
	if err := store.CreateRun(run); err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	timer := &Timer{
		TimerID:     "t-1",
		RunID:       "run-1",
		StepID:      "step-1",
		FireAt:      time.Now().Add(5 * time.Second).UTC().Format(time.RFC3339Nano),
		Status:      "PENDING",
		PayloadJSON: "{}",
	}

	if err := store.CreateTimer(timer); err != nil {
		t.Fatalf("failed to create timer: %v", err)
	}

	got, err := store.GetTimer("run-1", "step-1")
	if err != nil {
		t.Fatalf("failed to get timer: %v", err)
	}
	if got == nil || got.TimerID != "t-1" || got.Status != "PENDING" {
		t.Errorf("expected timer t-1 in status PENDING, got: %+v", got)
	}

	if err := store.FireTimer("t-1"); err != nil {
		t.Fatalf("failed to fire timer: %v", err)
	}
	got, _ = store.GetTimer("run-1", "step-1")
	if got.Status != "FIRED" {
		t.Errorf("expected timer to be FIRED, got: %s", got.Status)
	}

	// 2. Cron Schedules
	cs := &CronSchedule{
		WorkflowName:   "cron-workflow",
		CronExpression: "*/5 * * * *",
		OverlapPolicy:  "skip",
		NextRunTime:    time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano),
		DefinitionYAML: `
name: cron-workflow
version: 1
steps:
  - id: step-1
    run: "echo 'hello'"
`,
		Status:         "ACTIVE",
	}

	if err := store.UpsertCronSchedule(cs); err != nil {
		t.Fatalf("failed to upsert cron schedule: %v", err)
	}

	list, err := store.ListCronSchedules()
	if err != nil || len(list) != 1 || list[0].WorkflowName != "cron-workflow" {
		t.Errorf("failed to list cron schedules: %v, list: %+v", err, list)
	}

	due, err := store.GetDueCronSchedules(time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil || len(due) != 1 {
		t.Errorf("expected 1 due cron schedule, got %d, err: %v", len(due), err)
	}

	// Trigger cron schedule (first trigger should create the run)
	runID, triggered, err := store.TriggerCronSchedule("cron-workflow", time.Now())
	if err != nil {
		t.Fatalf("failed to trigger cron: %v", err)
	}
	if !triggered || runID == "" {
		t.Errorf("expected cron to trigger successfully")
	}

	// Double check the run is created in running status
	run, err = store.GetRun(runID)
	if err != nil || run.Status != "RUNNING" {
		t.Errorf("expected run to be RUNNING, got status: %s, err: %v", run.Status, err)
	}

	// Trigger again immediately (overlap policy is skip, run is still RUNNING, so it should skip!)
	_, triggered, err = store.TriggerCronSchedule("cron-workflow", time.Now())
	if err != nil {
		t.Fatalf("failed to trigger cron second time: %v", err)
	}
	if triggered {
		t.Errorf("expected cron triggering to be skipped due to overlap policy 'skip'")
	}
}


