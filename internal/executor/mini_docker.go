package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type MiniDockerExecutor struct {
	pythonPath string
}

func NewMiniDockerExecutor() *MiniDockerExecutor {
	pythonPath := os.Getenv("MINI_DOCKER_PYTHON_PATH")
	if pythonPath == "" {
		pythonPath = "/home/yumekaz/Desktop/Mini-Docker/venv/bin/python3"
		if _, err := os.Stat(pythonPath); err != nil {
			pythonPath = "python3"
		}
	}
	return &MiniDockerExecutor{pythonPath: pythonPath}
}

func (m *MiniDockerExecutor) Execute(ctx context.Context, req ExecutionRequest) (*Result, error) {
	start := time.Now()

	if req.Image == "" {
		return nil, fmt.Errorf("mini-docker executor requires a rootfs path specified in the image field")
	}

	rootfsPath := req.Image
	if !filepath.IsAbs(rootfsPath) {
		absPath, err := filepath.Abs(rootfsPath)
		if err == nil {
			rootfsPath = absPath
		}
	}

	args := []string{"-m", "mini_docker", "run", "--rootless", "--no-overlay", "--rm"}

	// Environment variables
	for k, v := range req.Env {
		args = append(args, "--env", fmt.Sprintf("%s=%s", k, v))
	}

	// CPU limit (e.g. "50")
	if req.CPU != "" {
		args = append(args, "--cpu", req.CPU)
	}

	// Memory limit (e.g. "128M")
	if req.Memory != "" {
		args = append(args, "--memory", req.Memory)
	}

	// Add `--` to separate options from positional arguments
	args = append(args, "--", rootfsPath, "/bin/sh", "-c", req.Command)

	cmd := exec.Command(m.pythonPath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return &Result{
			ExitCode: -1,
			Duration: time.Since(start),
			Error:    err,
		}, nil
	}

	doneChan := make(chan struct{})
	defer close(doneChan)

	go func() {
		select {
		case <-ctx.Done():
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		case <-doneChan:
		}
	}()

	err := cmd.Wait()
	duration := time.Since(start)

	if ctx.Err() != nil {
		return &Result{
			ExitCode: -1,
			Stdout:   stdoutBuf.String(),
			Stderr:   stderrBuf.String() + "\n[MINI-DOCKER] Container execution timed out or was canceled",
			Duration: duration,
			Error:    ctx.Err(),
		}, nil
	}

	exitCode := 0
	var execErr error
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				exitCode = status.ExitStatus()
			} else {
				exitCode = -1
			}
		} else {
			exitCode = -1
			execErr = err
		}
	}

	stdoutStr := stdoutBuf.String()
	var filteredLines []string
	for _, line := range strings.Split(stdoutStr, "\n") {
		if strings.HasPrefix(line, "Created container:") {
			continue
		}
		filteredLines = append(filteredLines, line)
	}
	stdoutStr = strings.Join(filteredLines, "\n")

	stderrStr := stderrBuf.String()
	if stderrStr != "" {
		stderrStr = "[MINI-DOCKER] " + stderrStr
	}

	return &Result{
		ExitCode: exitCode,
		Stdout:   stdoutStr,
		Stderr:   stderrStr,
		Duration: duration,
		Error:    execErr,
	}, nil
}
