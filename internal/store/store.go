package store

import (
	"github.com/yumekaz/duraflow/internal/workflow"
)

type WorkflowRun struct {
	RunID           string
	WorkflowName    string
	WorkflowVersion int
	Status          string
	CreatedAt       string
	StartedAt       string
	CompletedAt     string
	FailedAt        string
	MetadataJSON    string
}

type Event struct {
	ID           int64
	RunID        string
	WorkflowName string
	EventType    string
	StepID       string
	WorkerID     string
	Attempt      int
	PayloadJSON  string
	CreatedAt    string
}

type StepState struct {
	RunID       string
	StepID      string
	Status      string
	Attempt     int
	MaxAttempts int
	LastError   string
	NextRetryAt string
	StartedAt   string
	CompletedAt string
	WorkerID    string
}

type LogEntry struct {
	ID        int64
	RunID     string
	StepID    string
	Attempt   int
	Stream    string // "stdout" | "stderr"
	Content   string
	CreatedAt string
}

type EventStore interface {
	Init() error
	Close() error

	// Workflow definitions
	RegisterWorkflow(def *workflow.WorkflowDef, hash string, yamlContent string) error
	GetWorkflowDef(name string, version int) (*workflow.WorkflowDef, error)

	// Workflow runs
	CreateRun(run *WorkflowRun) error
	UpdateRunStatus(runID string, status string, timestamps map[string]string) error
	GetRun(runID string) (*WorkflowRun, error)
	ListRuns(limit int) ([]*WorkflowRun, error)

	// Events (append-only)
	AppendEvent(event *Event) error
	GetEvents(runID string) ([]*Event, error)

	// Step states
	UpsertStepState(state *StepState) error
	GetStepStates(runID string) ([]*StepState, error)

	// Logs
	AppendLog(entry *LogEntry) error
	GetLogs(runID string, stepID string) ([]*LogEntry, error)
}
