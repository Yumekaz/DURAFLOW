package store

import (
	"time"

	"github.com/yumekaz/duraflow/pkg/workflow"
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

type Worker struct {
	WorkerID        string
	Hostname        string
	PID             int
	StartedAt       string
	LastHeartbeatAt string
	Status          string
}

type Lease struct {
	RunID     string
	StepID    string
	WorkerID  string
	ExpiresAt string
	Status    string
}

type Timer struct {
	TimerID     string
	RunID       string
	StepID      string
	FireAt      string
	Status      string // "PENDING" | "FIRED" | "CANCELLED"
	PayloadJSON string
}

type CronSchedule struct {
	WorkflowName   string
	CronExpression string
	OverlapPolicy  string // "skip" | "allow"
	LastRunID      string
	LastRunTime    string
	NextRunTime    string
	DefinitionYAML string
	Status         string // "ACTIVE" | "PAUSED"
}

type EventStore interface {
	Init() error
	Close() error

	// Workflow definitions
	RegisterWorkflow(def *workflow.WorkflowDef, hash string, yamlContent string) error
	GetWorkflowDef(name string, version int) (*workflow.WorkflowDef, error)
	GetWorkflowYAML(name string, version int) (string, error)

	// Workflow runs
	CreateRun(run *WorkflowRun) error
	UpdateRunStatus(runID string, status string, timestamps map[string]string) error
	GetRun(runID string) (*WorkflowRun, error)
	ListRuns(limit int) ([]*WorkflowRun, error)
	GetIncompleteRuns() ([]*WorkflowRun, error)
	ResetStepState(runID, stepID string) error
	CancelWorkflowRun(runID string) error
	ResetWorkflowRunForRetry(runID string, status string) error

	// Events (append-only)
	AppendEvent(event *Event) error
	GetEvents(runID string) ([]*Event, error)

	// Step states
	UpsertStepState(state *StepState) error
	GetStepStates(runID string) ([]*StepState, error)

	// Logs
	AppendLog(entry *LogEntry) error
	GetLogs(runID string, stepID string) ([]*LogEntry, error)

	// Workers
	RegisterWorker(w *Worker) error
	HeartbeatWorker(workerID string) error
	GetActiveWorkers() ([]*Worker, error)

	// Leases
	AcquireLease(runID, stepID, workerID string, duration time.Duration) (bool, error)
	RenewLease(runID, stepID, workerID string, duration time.Duration) (bool, error)
	ReleaseLease(runID, stepID, workerID string) error

	// Timers
	CreateTimer(t *Timer) error
	GetTimer(runID, stepID string) (*Timer, error)
	FireTimer(timerID string) error
	CancelTimer(timerID string) error

	// Cron Schedules
	UpsertCronSchedule(cs *CronSchedule) error
	GetDueCronSchedules(nowStr string) ([]*CronSchedule, error)
	UpdateCronScheduleNextRun(workflowName, lastRunID, lastRunTime, nextRunTime string) error
	ListCronSchedules() ([]*CronSchedule, error)
	TriggerCronSchedule(workflowName string, now time.Time) (string, bool, error)
}
