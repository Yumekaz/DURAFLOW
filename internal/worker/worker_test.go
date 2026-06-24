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
