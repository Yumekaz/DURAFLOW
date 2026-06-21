package engine

// Workflow statuses
const (
	StatusCreated   = "CREATED"
	StatusRunning   = "RUNNING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// Step statuses
const (
	StepPending   = "PENDING"
	StepRunning   = "RUNNING"
	StepSucceeded = "SUCCEEDED"
	StepFailed    = "FAILED"
)

// Event types
const (
	EventWorkflowRunCreated = "WorkflowRunCreated"
	EventWorkflowStarted    = "WorkflowStarted"
	EventWorkflowCompleted  = "WorkflowCompleted"
	EventWorkflowFailed     = "WorkflowFailed"
	EventStepScheduled      = "StepScheduled"
	EventStepStarted        = "StepStarted"
	EventStepSucceeded      = "StepSucceeded"
	EventStepFailed         = "StepAttemptFailed"
)
