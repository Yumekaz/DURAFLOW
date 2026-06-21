package executor

import (
	"context"
	"time"
)

type Result struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Duration time.Duration
	Error    error
}

type Executor interface {
	Execute(ctx context.Context, command string, env map[string]string) (*Result, error)
}
