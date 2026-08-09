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
	modulePath string
}

func NewMiniDockerExecutor() *MiniDockerExecutor {
	pythonPath, modulePath := resolveMiniDockerRuntime()
	return &MiniDockerExecutor{pythonPath: pythonPath, modulePath: modulePath}
}

const (
	miniDockerPythonPathEnv = "MINI_DOCKER_PYTHON_PATH"
	miniDockerPythonEnv     = "MINI_DOCKER_PYTHON"
	miniDockerSourceEnv     = "MINI_DOCKER_SRC"
	miniDockerPathEnv       = "MINI_DOCKER_PATH"
)

func resolveMiniDockerRuntime() (string, string) {
	return resolveMiniDockerRuntimeFrom(searchRoots())
}

func resolveMiniDockerRuntimeFrom(roots []string) (string, string) {
	modulePath := miniDockerSourceFromEnv()
	if modulePath == "" {
		modulePath = findSiblingMiniDocker(roots)
	}

	if pythonPath := os.Getenv(miniDockerPythonPathEnv); pythonPath != "" {
		return pythonPath, modulePath
	}
	if pythonPath := os.Getenv(miniDockerPythonEnv); pythonPath != "" {
		return pythonPath, modulePath
	}

	if modulePath != "" {
		for _, candidate := range []string{
			filepath.Join(modulePath, "venv", "bin", "python3"),
			filepath.Join(modulePath, "venv", "bin", "python"),
			filepath.Join(modulePath, ".venv", "bin", "python3"),
			filepath.Join(modulePath, ".venv", "bin", "python"),
		} {
			if isExecutable(candidate) {
				return candidate, modulePath
			}
		}
	}

	if pythonPath, err := exec.LookPath("python3"); err == nil {
		return pythonPath, modulePath
	}
	if pythonPath, err := exec.LookPath("python"); err == nil {
		return pythonPath, modulePath
	}
	// Preserve the previous fallback and let Execute return the command error.
	return "python3", modulePath
}

func miniDockerSourceFromEnv() string {
	for _, name := range []string{miniDockerSourceEnv, miniDockerPathEnv} {
		if source := os.Getenv(name); source != "" && hasMiniDockerModule(source) {
			return source
		}
	}
	return ""
}

func findSiblingMiniDocker(roots []string) string {
	for _, root := range roots {
		current, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		for {
			candidate := filepath.Join(current, "Mini-Docker")
			if hasMiniDockerModule(candidate) {
				return candidate
			}
			if filepath.Base(current) == "DURAFLOW" {
				candidate = filepath.Join(filepath.Dir(current), "Mini-Docker")
				if hasMiniDockerModule(candidate) {
					return candidate
				}
			}
			parent := filepath.Dir(current)
			if parent == current {
				break
			}
			current = parent
		}
	}
	return ""
}

func searchRoots() []string {
	roots := make([]string, 0, 2)
	if cwd, err := os.Getwd(); err == nil {
		roots = append(roots, cwd)
	}
	if executable, err := os.Executable(); err == nil {
		roots = append(roots, filepath.Dir(executable))
	}
	return roots
}

func hasMiniDockerModule(path string) bool {
	info, err := os.Stat(filepath.Join(path, "mini_docker", "__init__.py"))
	return err == nil && !info.IsDir()
}

func isExecutable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
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
	if m.modulePath != "" {
		cmd.Env = append(os.Environ(), "PYTHONPATH="+prependPythonPath(os.Getenv("PYTHONPATH"), m.modulePath))
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

func prependPythonPath(existing, modulePath string) string {
	if existing == "" {
		return modulePath
	}
	return modulePath + string(os.PathListSeparator) + existing
}
