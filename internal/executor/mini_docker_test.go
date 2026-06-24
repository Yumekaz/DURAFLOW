package executor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMiniDockerExecutor_Success(t *testing.T) {
	exec := NewMiniDockerExecutor()
	res, err := exec.Execute(context.Background(), ExecutionRequest{
		Image:   "/home/yumekaz/Desktop/Mini-Docker/rootfs",
		Command: "echo 'hello from mini-docker'",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d, stderr: %s", res.ExitCode, res.Stderr)
	}

	if strings.TrimSpace(res.Stdout) != "hello from mini-docker" {
		t.Errorf("expected stdout 'hello from mini-docker', got %q", res.Stdout)
	}
}

func TestMiniDockerExecutor_EnvAndLimits(t *testing.T) {
	exec := NewMiniDockerExecutor()
	res, err := exec.Execute(context.Background(), ExecutionRequest{
		Image:   "/home/yumekaz/Desktop/Mini-Docker/rootfs",
		Command: "echo $TEST_VAR",
		Env: map[string]string{
			"TEST_VAR": "minival",
		},
		CPU:    "50",
		Memory: "128M",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d, stderr: %s", res.ExitCode, res.Stderr)
	}

	if strings.TrimSpace(res.Stdout) != "minival" {
		t.Errorf("expected stdout 'minival', got %q", res.Stdout)
	}
}

func TestMiniDockerExecutor_Timeout(t *testing.T) {
	exec := NewMiniDockerExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	res, err := exec.Execute(ctx, ExecutionRequest{
		Image:   "/home/yumekaz/Desktop/Mini-Docker/rootfs",
		Command: "sleep 2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != -1 {
		t.Errorf("expected exit code -1 for timeout, got %d", res.ExitCode)
	}
}
