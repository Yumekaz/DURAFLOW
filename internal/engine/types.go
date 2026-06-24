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
	StepPending        = "PENDING"
	StepRunning        = "RUNNING"
	StepSucceeded      = "SUCCEEDED"
	StepFailed         = "FAILED" // Legacy / simple failed status
	StepRetryScheduled = "RETRY_SCHEDULED"
	StepTimedOut       = "TIMED_OUT"
	StepFailedFinal    = "FAILED_FINAL"
	StepWaiting        = "WAITING"
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
	EventStepRetryScheduled = "StepRetryScheduled"
	EventStepTimedOut       = "StepTimedOut"
	EventStepFailedFinal    = "StepFailedFinal"
	EventWorkflowResumed    = "WorkflowResumed"
	EventStepResumed        = "StepResumed"
	EventTimerCreated       = "TimerCreated"
	EventTimerFired         = "TimerFired"
	EventTimerCancelled     = "TimerCancelled"
	EventCronScheduleReg    = "CronScheduleRegistered"
	EventCronWorkflowTrig   = "CronWorkflowTriggered"
)
