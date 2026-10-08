package executor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func miniDockerRootfs(t *testing.T) string {
	t.Helper()
	if os.Geteuid() != 0 && os.Getenv("DURAFLOW_TEST_MINIDOCKER") != "1" {
		t.Skip("real Mini-Docker execution requires privileged namespaces; set DURAFLOW_TEST_MINIDOCKER=1 to explicitly test a capable rootless host")
	}
	_, modulePath := resolveMiniDockerRuntime()
	if modulePath == "" {
		t.Skip("Mini-Docker sibling checkout is not available")
	}
	// --no-overlay must never run tests against the shared runtime rootfs.
	// Use the same minimal static BusyBox layout as Mini-Docker's root proof.
	busybox, err := os.ReadFile(filepath.Join(modulePath, "rootfs", "bin", "busybox"))
	if err != nil || len(busybox) == 0 {
		t.Fatalf("test BusyBox prerequisite is missing/empty: %v", err)
	}
	rootfs := t.TempDir()
	for _, dir := range []string{"bin", "dev", "proc", "sys", "tmp", "etc"} {
		if err := os.MkdirAll(filepath.Join(rootfs, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(rootfs, "bin", "busybox"), busybox, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"sh", "echo", "sleep"} {
		if err := os.Symlink("busybox", filepath.Join(rootfs, "bin", name)); err != nil {
			t.Fatal(err)
		}
	}
	return rootfs
}

func TestMiniDockerExecutor_Success(t *testing.T) {
	exec := NewMiniDockerExecutor()
	res, err := exec.Execute(context.Background(), ExecutionRequest{
		Image:   miniDockerRootfs(t),
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
		Image:   miniDockerRootfs(t),
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
		Image:   miniDockerRootfs(t),
		Command: "sleep 2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if res.ExitCode != -1 {
		t.Errorf("expected exit code -1 for timeout, got %d", res.ExitCode)
	}
}

func TestMiniDockerRuntime_ResolvesSiblingCheckoutPortably(t *testing.T) {
	t.Setenv(miniDockerPythonPathEnv, "")
	t.Setenv(miniDockerPythonEnv, "")
	t.Setenv(miniDockerSourceEnv, "")
	t.Setenv(miniDockerPathEnv, "")

	parent := t.TempDir()
	sibling := filepath.Join(parent, "Mini-Docker")
	root := filepath.Join(parent, "DURAFLOW", "pkg", "executor")
	if err := os.MkdirAll(filepath.Join(sibling, "mini_docker"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sibling, "mini_docker", "__init__.py"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	python := filepath.Join(sibling, "venv", "bin", "python3")
	if err := os.MkdirAll(filepath.Dir(python), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(python, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	gotPython, gotModule := resolveMiniDockerRuntimeFrom([]string{root})
	if gotPython != python {
		t.Fatalf("expected sibling interpreter %q, got %q", python, gotPython)
	}
	if gotModule != sibling {
		t.Fatalf("expected sibling module path %q, got %q", sibling, gotModule)
	}
}

func TestMiniDockerRuntime_UsesPortableEnvOverrides(t *testing.T) {
	t.Setenv(miniDockerPythonPathEnv, "/opt/mini-docker/bin/python3")
	t.Setenv(miniDockerSourceEnv, t.TempDir())
	source := os.Getenv(miniDockerSourceEnv)
	if err := os.MkdirAll(filepath.Join(source, "mini_docker"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "mini_docker", "__init__.py"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	gotPython, gotModule := resolveMiniDockerRuntimeFrom(nil)
	if gotPython != "/opt/mini-docker/bin/python3" {
		t.Fatalf("expected environment interpreter, got %q", gotPython)
	}
	if gotModule != source {
		t.Fatalf("expected environment module path %q, got %q", source, gotModule)
	}
}
