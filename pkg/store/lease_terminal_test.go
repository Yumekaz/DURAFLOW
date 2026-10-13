package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yumekaz/duraflow/pkg/workflow"
)

func testLeaseCannotReplaySucceededStep(t *testing.T, st EventStore) {
	t.Helper()
	def := &workflow.WorkflowDef{Name: "lease-terminal-proof", Version: 1, Steps: []workflow.StepDef{{ID: "step", Run: "echo proof"}}}
	if err := st.RegisterWorkflow(def, "hash", "name: lease-terminal-proof\nversion: 1\nsteps:\n  - id: step\n    run: echo proof\n"); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(&WorkflowRun{RunID: "run", WorkflowName: def.Name, WorkflowVersion: 1, Status: "RUNNING", MetadataJSON: "{}"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := st.RegisterWorker(&Worker{WorkerID: "worker", StartedAt: now, LastHeartbeatAt: now, Status: "ACTIVE"}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertStepState(&StepState{RunID: "run", StepID: "step", Status: "PENDING", MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	if acquired, err := st.AcquireLease("run", "step", "worker", time.Second); err != nil || !acquired {
		t.Fatalf("initial claim=%v error=%v", acquired, err)
	}
	// A scanner may retain its old RUNNING/PENDING view while this finishes.
	if err := st.UpsertStepState(&StepState{RunID: "run", StepID: "step", Status: "SUCCEEDED", Attempt: 1, MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReleaseLease("run", "step", "worker"); err != nil {
		t.Fatal(err)
	}
	if acquired, err := st.AcquireLease("run", "step", "worker", time.Second); err != nil || acquired {
		t.Fatalf("stale scan replayed succeeded step: acquired=%v error=%v", acquired, err)
	}
	states, err := st.GetStepStates("run")
	if err != nil || len(states) != 1 || states[0].Status != "SUCCEEDED" {
		t.Fatalf("terminal state overwritten: %+v %v", states, err)
	}
	// Compensation is a distinct, deliberate execution; do not break it.
	if err := st.UpdateRunStatus("run", "COMPENSATING", nil); err != nil {
		t.Fatal(err)
	}
	if acquired, err := st.AcquireLease("run", "step", "worker", time.Second); err != nil || !acquired {
		t.Fatalf("legitimate compensation claim=%v error=%v", acquired, err)
	}
}

func TestSQLiteLeaseCannotReplaySucceededStep(t *testing.T) {
	st := NewSQLiteStore(filepath.Join(t.TempDir(), "proof.db"))
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	testLeaseCannotReplaySucceededStep(t, st)
}

func TestPostgresLeaseCannotReplaySucceededStep(t *testing.T) {
	st := getTestPostgresStore(t)
	defer st.Close()
	cleanupPostgres(t, st)
	testLeaseCannotReplaySucceededStep(t, st)
}
