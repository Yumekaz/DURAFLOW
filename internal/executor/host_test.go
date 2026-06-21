package executor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestHostExecutor_Success(t *testing.T) {
	exec := NewHostExecutor()
	res, err := exec.Execute(context.Background(), "echo 'hello'", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", res.ExitCode)
	}

	if strings.TrimSpace(res.Stdout) != "hello" {
		t.Errorf("expected stdout 'hello', got %q", res.Stdout)
	}

	if res.Stderr != "" {
		t.Errorf("expected empty stderr, got %q", res.Stderr)
	}

	if res.Duration <= 0 {
		t.Errorf("expected positive duration, got %v", res.Duration)
	}
}

func TestHostExecutor_Failure(t *testing.T) {
	exec := NewHostExecutor()
	res, err := exec.Execute(context.Background(), "echo 'failed step' >&2; exit 42", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 42 {
		t.Errorf("expected exit code 42, got %d", res.ExitCode)
	}

	if strings.TrimSpace(res.Stderr) != "failed step" {
		t.Errorf("expected stderr 'failed step', got %q", res.Stderr)
	}
}

func TestHostExecutor_Env(t *testing.T) {
	exec := NewHostExecutor()
	res, err := exec.Execute(context.Background(), "echo $TEST_VAR", map[string]string{
		"TEST_VAR": "env_value_123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", res.ExitCode)
	}

	if strings.TrimSpace(res.Stdout) != "env_value_123" {
		t.Errorf("expected stdout 'env_value_123', got %q", res.Stdout)
	}
}

func TestHostExecutor_Timeout(t *testing.T) {
	exec := NewHostExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	res, err := exec.Execute(ctx, "sleep 2", nil)
	if err != nil {
		t.Fatalf("unexpected execution error: %v", err)
	}

	if res.ExitCode != -1 {
		t.Errorf("expected exit code -1 for timeout, got %d", res.ExitCode)
	}

	if !strings.Contains(res.Stderr, "timed out") {
		t.Errorf("expected stderr to contain 'timed out', got %q", res.Stderr)
	}
}
