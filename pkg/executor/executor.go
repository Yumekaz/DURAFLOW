package executor

import (
	"context"
	"fmt"
	"time"
)

type Result struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
	Error    error
}

type ExecutionRequest struct {
	Executor  string
	Command   string
	Env       map[string]string
	Image     string
	CPU       string
	Memory    string
	TimeoutMs int64
}

type Executor interface {
	Execute(ctx context.Context, req ExecutionRequest) (*Result, error)
}

type Registry struct {
	executors map[string]Executor
}

func NewRegistry() *Registry {
	return &Registry{executors: make(map[string]Executor)}
}

func (r *Registry) Register(name string, exec Executor) {
	r.executors[name] = exec
}

func (r *Registry) Execute(ctx context.Context, req ExecutionRequest) (*Result, error) {
	name := req.Executor
	if name == "" {
		name = "host"
	}
	exec, ok := r.executors[name]
	if !ok {
		// Fallback to host executor
		exec = r.executors["host"]
	}
	if exec == nil {
		return nil, fmt.Errorf("no executor registered for %q and no host fallback", name)
	}
	return exec.Execute(ctx, req)
}
