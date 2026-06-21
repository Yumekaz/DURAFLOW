package executor

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type HostExecutor struct{}

func NewHostExecutor() *HostExecutor {
	return &HostExecutor{}
}

func (h *HostExecutor) Execute(ctx context.Context, command string, env map[string]string) (*Result, error) {
	start := time.Now()

	// Use standard exec.Command without context so we can manage termination ourselves
	cmd := exec.Command("sh", "-c", command)
	
	// Create a new process group for the command on Unix-like systems
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Set up environment variables
	cmd.Env = os.Environ() // Start with current environment
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

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

	// Monitor context cancellation in the background
	doneChan := make(chan struct{})
	defer close(doneChan)

	go func() {
		select {
		case <-ctx.Done():
			// Kill the entire process group (-PID)
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		case <-doneChan:
		}
	}()

	err := cmd.Wait()
	duration := time.Since(start)

	// Check if context was cancelled or timed out
	if ctx.Err() != nil {
		return &Result{
			ExitCode: -1,
			Stdout:   stdoutBuf.String(),
			Stderr:   stderrBuf.String() + "\n[DURAFLOW] Command execution timed out or was canceled",
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

	return &Result{
		ExitCode: exitCode,
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
		Duration: duration,
		Error:    execErr,
	}, nil
}
