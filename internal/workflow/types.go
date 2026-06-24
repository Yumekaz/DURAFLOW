package workflow

type WorkflowDef struct {
	Name      string            `yaml:"name"`
	Version   int               `yaml:"version"`
	Metadata  map[string]string `yaml:"metadata"`
	Env       map[string]string `yaml:"env"`
	Steps     []StepDef         `yaml:"steps"`
	OnFailure *OnFailureDef     `yaml:"on_failure"`
	Schedule  *ScheduleDef      `yaml:"schedule"`
}

type ScheduleDef struct {
	Cron    string `yaml:"cron"`
	Overlap string `yaml:"overlap"` // "skip" | "allow" (default: "skip")
}

type StepDef struct {
	ID             string       `yaml:"id"`
	Run            string       `yaml:"run"`
	DependsOn      []string     `yaml:"depends_on"`
	TimeoutMs      int64        `yaml:"timeout_ms"`
	Retry          *RetryPolicy `yaml:"retry"`
	IdempotencyKey string       `yaml:"idempotency_key"`
	Compensation   *CompStep    `yaml:"compensation"`
	Wait           *WaitDef     `yaml:"wait"`
}

type WaitDef struct {
	Duration string `yaml:"duration"` // e.g. "5s", "10m"
}

type RetryPolicy struct {
	MaxAttempts        int    `yaml:"max_attempts"`
	Backoff            string `yaml:"backoff"` // "fixed" | "exponential"
	InitialDelayMs     int64  `yaml:"initial_delay_ms"`
	MaxDelayMs         int64  `yaml:"max_delay_ms"`
	DelayMs            int64  `yaml:"delay_ms"` // for fixed backoff
	RetryOnExitCodes   []int  `yaml:"retry_on_exit_codes"`
	NoRetryOnExitCodes []int  `yaml:"no_retry_on_exit_codes"`
}

type OnFailureDef struct {
	Compensate bool `yaml:"compensate"`
}

type CompStep struct {
	Run string `yaml:"run"`
}
