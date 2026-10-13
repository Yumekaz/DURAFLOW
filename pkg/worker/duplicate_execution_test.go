package worker

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yumekaz/duraflow/pkg/engine"
	"github.com/yumekaz/duraflow/pkg/executor"
	"github.com/yumekaz/duraflow/pkg/store"
	"github.com/yumekaz/duraflow/pkg/workflow"
)

type blockedProofExecutor struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (e *blockedProofExecutor) Execute(ctx context.Context, request executor.ExecutionRequest) (*executor.Result, error) {
	if e.calls.Add(1) == 1 {
		close(e.entered)
	}
	select {
	case <-e.release:
		return &executor.Result{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestWorkerDoesNotReplayItsOwnInFlightStepWithStaleHeartbeat(t *testing.T) {
	st := store.NewSQLiteStore(filepath.Join(t.TempDir(), "proof.db"))
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	exec := &blockedProofExecutor{entered: make(chan struct{}), release: make(chan struct{})}
	eng := engine.NewWorkflowEngine(st, exec)
	w := NewWorkerDaemon(st, eng)
	defer w.Stop()
	stale := time.Now().Add(-20 * time.Second).UTC().Format(time.RFC3339Nano)
	if err := st.RegisterWorker(&store.Worker{WorkerID: w.workerID, StartedAt: stale, LastHeartbeatAt: stale, Status: "ACTIVE"}); err != nil {
		t.Fatal(err)
	}
	yaml := "name: duplicate-proof\nversion: 1\nsteps:\n  - id: blocked\n    run: blocked\n"
	def, hash, steps, err := workflow.ParseAndValidate([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := eng.RunWorkflow(context.Background(), def, steps, hash, yaml); err != nil {
		t.Fatal(err)
	}
	w.scanAndRunEligibleSteps()
	select {
	case <-exec.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first execution never entered")
	}
	// The persisted lease sees a stale heartbeat, but this very worker still
	// knows it owns a live execution. Another scan must not launch a second.
	w.scanAndRunEligibleSteps()
	close(exec.release)
	w.wg.Wait()
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("same worker executed the in-flight step %d times", got)
	}
}
