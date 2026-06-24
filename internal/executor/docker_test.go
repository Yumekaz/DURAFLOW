package executor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDockerExecutor_Success(t *testing.T) {
	exec := NewDockerExecutor()
	res, err := exec.Execute(context.Background(), ExecutionRequest{
		Image:   "alpine:latest",
		Command: "echo 'hello from docker'",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d, stderr: %s", res.ExitCode, res.Stderr)
	}

	if strings.TrimSpace(res.Stdout) != "hello from docker" {
		t.Errorf("expected stdout 'hello from docker', got %q", res.Stdout)
	}
}

func TestDockerExecutor_EnvAndLimits(t *testing.T) {
	exec := NewDockerExecutor()
	res, err := exec.Execute(context.Background(), ExecutionRequest{
		Image:   "alpine:latest",
		Command: "echo $TEST_VAR",
		Env: map[string]string{
			"TEST_VAR": "val123",
		},
		CPU:    "0.2",
		Memory: "128m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d, stderr: %s", res.ExitCode, res.Stderr)
	}

	if strings.TrimSpace(res.Stdout) != "val123" {
		t.Errorf("expected stdout 'val123', got %q", res.Stdout)
	}
}

func TestDockerExecutor_Timeout(t *testing.T) {
	exec := NewDockerExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	res, err := exec.Execute(ctx, ExecutionRequest{
		Image:   "alpine:latest",
		Command: "sleep 2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != -1 {
		t.Errorf("expected exit code -1 for timeout, got %d", res.ExitCode)
	}
}
