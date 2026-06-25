package executor

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type DockerExecutor struct{}

func NewDockerExecutor() *DockerExecutor {
	return &DockerExecutor{}
}

func (d *DockerExecutor) Execute(ctx context.Context, req ExecutionRequest) (*Result, error) {
	start := time.Now()

	if req.Image == "" {
		return nil, fmt.Errorf("docker executor requires a non-empty container image")
	}

	args := []string{"run", "--rm", "-i"}

	// Environment variables
	for k, v := range req.Env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	// CPU limit (e.g. "0.5")
	if req.CPU != "" {
		args = append(args, fmt.Sprintf("--cpus=%s", req.CPU))
	}

	// Memory limit (e.g. "256m")
	if req.Memory != "" {
		args = append(args, "-m", req.Memory)
	}

	// Add image and shell command
	args = append(args, req.Image, "sh", "-c", req.Command)

	cmd := exec.Command("docker", args...)
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
			// Kill the process group to abort the docker run command
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
			Stderr:   stderrBuf.String() + "\n[DOCKER] Container execution timed out or was canceled",
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

	stderrStr := stderrBuf.String()
	if stderrStr != "" {
		stderrStr = "[DOCKER] " + stderrStr
	}

	return &Result{
		ExitCode: exitCode,
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrStr,
		Duration: duration,
		Error:    execErr,
	}, nil
}
