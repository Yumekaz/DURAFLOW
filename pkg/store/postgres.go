package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/robfig/cron/v3"
	"github.com/yumekaz/duraflow/pkg/workflow"
	"gopkg.in/yaml.v3"
)

const PostgresSchema = `
-- Workflow definitions
CREATE TABLE IF NOT EXISTS workflow_definitions (
    name            VARCHAR(255) NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    definition_hash VARCHAR(255) NOT NULL,
    definition_yaml TEXT NOT NULL,
    created_at      VARCHAR(255) NOT NULL DEFAULT to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    PRIMARY KEY (name, version)
);

-- Workflow runs
CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id           VARCHAR(255) PRIMARY KEY,
    workflow_name    VARCHAR(255) NOT NULL,
    workflow_version INTEGER NOT NULL DEFAULT 1,
    status           VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    created_at       VARCHAR(255) NOT NULL DEFAULT to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    started_at       VARCHAR(255),
    completed_at     VARCHAR(255),
    failed_at        VARCHAR(255),
    metadata_json    TEXT
);

-- Event log
CREATE TABLE IF NOT EXISTS events (
    id             SERIAL PRIMARY KEY,
    run_id         VARCHAR(255) NOT NULL,
    workflow_name  VARCHAR(255) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    step_id        VARCHAR(255),
    worker_id      VARCHAR(255),
    attempt        INTEGER,
    payload_json   TEXT,
    created_at     VARCHAR(255) NOT NULL DEFAULT to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
);

-- Step states
CREATE TABLE IF NOT EXISTS step_states (
    run_id       VARCHAR(255) NOT NULL,
    step_id      VARCHAR(255) NOT NULL,
    status       VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    attempt      INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    last_error   TEXT,
    next_retry_at VARCHAR(255),
    started_at   VARCHAR(255),
    completed_at VARCHAR(255),
    worker_id    VARCHAR(255),
    PRIMARY KEY (run_id, step_id)
);

-- Logs
CREATE TABLE IF NOT EXISTS logs (
    id         SERIAL PRIMARY KEY,
    run_id     VARCHAR(255) NOT NULL,
    step_id    VARCHAR(255) NOT NULL,
    attempt    INTEGER NOT NULL DEFAULT 1,
    stream     VARCHAR(50) NOT NULL,  -- 'stdout' | 'stderr'
    content    TEXT,
    created_at VARCHAR(255) NOT NULL DEFAULT to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_step_states_run ON step_states(run_id);
CREATE INDEX IF NOT EXISTS idx_logs_run_step ON logs(run_id, step_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status ON workflow_runs(status);

-- Workers registration
CREATE TABLE IF NOT EXISTS workers (
    worker_id          VARCHAR(255) PRIMARY KEY,
    hostname           VARCHAR(255) NOT NULL,
    pid                INTEGER NOT NULL,
    started_at         VARCHAR(255) NOT NULL,
    last_heartbeat_at  VARCHAR(255) NOT NULL,
    status             VARCHAR(50) NOT NULL DEFAULT 'ACTIVE'
);

-- Task leases
CREATE TABLE IF NOT EXISTS leases (
    run_id      VARCHAR(255) NOT NULL,
    step_id     VARCHAR(255) NOT NULL,
    worker_id   VARCHAR(255) NOT NULL,
    expires_at  VARCHAR(255) NOT NULL,
    status      VARCHAR(50) NOT NULL DEFAULT 'ACTIVE', -- 'ACTIVE' | 'RELEASED'
    PRIMARY KEY (run_id, step_id)
);

CREATE INDEX IF NOT EXISTS idx_leases_worker ON leases(worker_id);

-- Durable Timers
CREATE TABLE IF NOT EXISTS timers (
    timer_id     VARCHAR(255) PRIMARY KEY,
    run_id       VARCHAR(255) NOT NULL,
    step_id      VARCHAR(255) NOT NULL,
    fire_at      VARCHAR(255) NOT NULL,
    status       VARCHAR(50) NOT NULL DEFAULT 'PENDING', -- 'PENDING' | 'FIRED' | 'CANCELLED'
    payload_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_timers_fire_at ON timers(fire_at) WHERE status = 'PENDING';

-- Cron Schedules
CREATE TABLE IF NOT EXISTS cron_schedules (
    workflow_name     VARCHAR(255) PRIMARY KEY,
    cron_expression   VARCHAR(255) NOT NULL,
    overlap_policy    VARCHAR(50) NOT NULL DEFAULT 'skip', -- 'skip' | 'allow'
    last_run_id       VARCHAR(255),
    last_run_time     VARCHAR(255),
    next_run_time     VARCHAR(255) NOT NULL,
    definition_yaml   TEXT NOT NULL,
    status            VARCHAR(50) NOT NULL DEFAULT 'ACTIVE'
);

CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_time ON cron_schedules(next_run_time) WHERE status = 'ACTIVE';
`

type PostgresStore struct {
	dsn string
	db  *sql.DB
}

func NewPostgresStore(dsn string) *PostgresStore {
	return &PostgresStore{dsn: dsn}
}

func (p *PostgresStore) Init() error {
	db, err := sql.Open("postgres", p.dsn)
	if err != nil {
		return err
	}
	p.db = db

	if err := p.db.Ping(); err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	if _, err := p.db.Exec(PostgresSchema); err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	return nil
}

func (p *PostgresStore) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func (p *PostgresStore) translate(query string) string {
	parts := strings.Split(query, "?")
	if len(parts) == 1 {
		return query
	}
	var sb strings.Builder
	for i := 0; i < len(parts)-1; i++ {
		sb.WriteString(parts[i])
		sb.WriteString(fmt.Sprintf("$%d", i+1))
	}
	sb.WriteString(parts[len(parts)-1])
	return sb.String()
}

// Workflow definitions

func (p *PostgresStore) RegisterWorkflow(def *workflow.WorkflowDef, hash string, yamlContent string) error {
	query := `
		INSERT INTO workflow_definitions (name, version, definition_hash, definition_yaml)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(name, version) DO UPDATE SET
			definition_hash = excluded.definition_hash,
			definition_yaml = excluded.definition_yaml;
	`
	_, err := p.db.Exec(p.translate(query), def.Name, def.Version, hash, yamlContent)
	return err
}

func (p *PostgresStore) GetWorkflowDef(name string, version int) (*workflow.WorkflowDef, error) {
	query := `
		SELECT definition_yaml FROM workflow_definitions
		WHERE name = ? AND version = ?;
	`
	var yamlContent string
	err := p.db.QueryRow(p.translate(query), name, version).Scan(&yamlContent)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	_, _, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
	if err != nil {
		return nil, err
	}

	// Reconstruct the definition structure
	def := &workflow.WorkflowDef{
		Name:    name,
		Version: version,
		Steps:   orderedSteps,
	}
	return def, nil
}

func (p *PostgresStore) GetWorkflowYAML(name string, version int) (string, error) {
	query := `
		SELECT definition_yaml FROM workflow_definitions
		WHERE name = ? AND version = ?;
	`
	var yamlContent string
	err := p.db.QueryRow(p.translate(query), name, version).Scan(&yamlContent)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return yamlContent, err
}

// Workflow runs

func (p *PostgresStore) CreateRun(run *WorkflowRun) error {
	query := `
		INSERT INTO workflow_runs (run_id, workflow_name, workflow_version, status, created_at, started_at, completed_at, failed_at, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	_, err := p.db.Exec(p.translate(query), run.RunID, run.WorkflowName, run.WorkflowVersion, run.Status, run.CreatedAt, run.StartedAt, run.CompletedAt, run.FailedAt, run.MetadataJSON)
	return err
}

func (p *PostgresStore) UpdateRunStatus(runID string, status string, timestamps map[string]string) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update status
	queryStatus := `
		UPDATE workflow_runs SET status = ? WHERE run_id = ?;
	`
	_, err = tx.Exec(p.translate(queryStatus), status, runID)
	if err != nil {
		return err
	}

	// Update timestamps if provided
	for col, val := range timestamps {
		if col != "started_at" && col != "completed_at" && col != "failed_at" {
			continue
		}
		queryTime := fmt.Sprintf("UPDATE workflow_runs SET %s = $1 WHERE run_id = $2;", col)
		_, err = tx.Exec(queryTime, val, runID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (p *PostgresStore) GetRun(runID string) (*WorkflowRun, error) {
	query := `
		SELECT run_id, workflow_name, workflow_version, status, created_at, COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(failed_at, ''), COALESCE(metadata_json, '{}')
		FROM workflow_runs
		WHERE run_id = ?;
	`
	var run WorkflowRun
	err := p.db.QueryRow(p.translate(query), runID).Scan(
		&run.RunID, &run.WorkflowName, &run.WorkflowVersion, &run.Status,
		&run.CreatedAt, &run.StartedAt, &run.CompletedAt, &run.FailedAt, &run.MetadataJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &run, err
}

func (p *PostgresStore) ListRuns(limit int) ([]*WorkflowRun, error) {
	query := `
		SELECT run_id, workflow_name, workflow_version, status, created_at, COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(failed_at, ''), COALESCE(metadata_json, '{}')
		FROM workflow_runs
		ORDER BY created_at DESC
		LIMIT ?;
	`
	rows, err := p.db.Query(p.translate(query), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		err := rows.Scan(
			&r.RunID, &r.WorkflowName, &r.WorkflowVersion, &r.Status,
			&r.CreatedAt, &r.StartedAt, &r.CompletedAt, &r.FailedAt, &r.MetadataJSON,
		)
		if err != nil {
			return nil, err
		}
		runs = append(runs, &r)
	}
	return runs, nil
}

func (p *PostgresStore) GetIncompleteRuns() ([]*WorkflowRun, error) {
	query := `
		SELECT run_id, workflow_name, workflow_version, status, created_at, COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(failed_at, ''), COALESCE(metadata_json, '{}')
		FROM workflow_runs
		WHERE status = 'RUNNING' OR status = 'CREATED' OR status = 'COMPENSATING';
	`
	rows, err := p.db.Query(p.translate(query))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []*WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		err := rows.Scan(
			&r.RunID, &r.WorkflowName, &r.WorkflowVersion, &r.Status,
			&r.CreatedAt, &r.StartedAt, &r.CompletedAt, &r.FailedAt, &r.MetadataJSON,
		)
		if err != nil {
			return nil, err
		}
		runs = append(runs, &r)
	}
	return runs, nil
}

func (p *PostgresStore) ResetStepState(runID, stepID string) error {
	query := `
		UPDATE step_states
		SET status = 'PENDING', attempt = 0, last_error = NULL, completed_at = NULL, started_at = NULL, worker_id = NULL
		WHERE run_id = ? AND step_id = ?;
	`
	_, err := p.db.Exec(p.translate(query), runID, stepID)
	return err
}

func (p *PostgresStore) CancelWorkflowRun(runID string) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check if run is active
	queryRun := `
		SELECT workflow_name, status FROM workflow_runs WHERE run_id = ?;
	`
	var name, status string
	err = tx.QueryRow(p.translate(queryRun), runID).Scan(&name, &status)
	if err != nil {
		return err
	}
	if status == "COMPLETED" || status == "FAILED" || status == "CANCELLED" || status == "COMPENSATED" {
		return fmt.Errorf("cannot cancel workflow run %s: status is already %s", runID, status)
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	queryUpdate := `
		UPDATE workflow_runs SET status = 'CANCELLED', failed_at = ? WHERE run_id = ?;
	`
	_, err = tx.Exec(p.translate(queryUpdate), nowStr, runID)
	if err != nil {
		return err
	}

	queryEvent := `
		INSERT INTO events (run_id, workflow_name, event_type, payload_json)
		VALUES (?, ?, 'WorkflowCancelled', '{}');
	`
	_, err = tx.Exec(p.translate(queryEvent), runID, name)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (p *PostgresStore) ResetWorkflowRunForRetry(runID string, status string) error {
	query := `
		UPDATE workflow_runs
		SET status = ?, failed_at = NULL, completed_at = NULL
		WHERE run_id = ?;
	`
	_, err := p.db.Exec(p.translate(query), status, runID)
	return err
}

// Events (append-only)

func (p *PostgresStore) AppendEvent(event *Event) error {
	createdAt := event.CreatedAt
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	query := `
		INSERT INTO events (run_id, workflow_name, event_type, step_id, worker_id, attempt, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);
	`
	_, err := p.db.Exec(p.translate(query), event.RunID, event.WorkflowName, event.EventType, event.StepID, event.WorkerID, event.Attempt, event.PayloadJSON, createdAt)
	return err
}

func (p *PostgresStore) GetEvents(runID string) ([]*Event, error) {
	query := `
		SELECT id, run_id, workflow_name, event_type, COALESCE(step_id, ''), COALESCE(worker_id, ''), COALESCE(attempt, 0), COALESCE(payload_json, '{}'), created_at
		FROM events
		WHERE run_id = ?
		ORDER BY id ASC;
	`
	rows, err := p.db.Query(p.translate(query), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var ev Event
		err := rows.Scan(
			&ev.ID, &ev.RunID, &ev.WorkflowName, &ev.EventType,
			&ev.StepID, &ev.WorkerID, &ev.Attempt, &ev.PayloadJSON, &ev.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, &ev)
	}
	return events, nil
}

// Step states

func (p *PostgresStore) UpsertStepState(state *StepState) error {
	query := `
		INSERT INTO step_states (run_id, step_id, status, attempt, max_attempts, last_error, next_retry_at, started_at, completed_at, worker_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(run_id, step_id) DO UPDATE SET
			status = excluded.status,
			attempt = excluded.attempt,
			max_attempts = excluded.max_attempts,
			last_error = excluded.last_error,
			next_retry_at = excluded.next_retry_at,
			started_at = excluded.started_at,
			completed_at = excluded.completed_at,
			worker_id = excluded.worker_id;
	`
	_, err := p.db.Exec(
		p.translate(query),
		state.RunID, state.StepID, state.Status, state.Attempt, state.MaxAttempts,
		sqlNullString(state.LastError), sqlNullString(state.NextRetryAt),
		sqlNullString(state.StartedAt), sqlNullString(state.CompletedAt), sqlNullString(state.WorkerID),
	)
	return err
}

func (p *PostgresStore) GetStepStates(runID string) ([]*StepState, error) {
	query := `
		SELECT run_id, step_id, status, attempt, max_attempts, COALESCE(last_error, ''), COALESCE(next_retry_at, ''), COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(worker_id, '')
		FROM step_states
		WHERE run_id = ?;
	`
	rows, err := p.db.Query(p.translate(query), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var states []*StepState
	for rows.Next() {
		var s StepState
		err := rows.Scan(
			&s.RunID, &s.StepID, &s.Status, &s.Attempt, &s.MaxAttempts,
			&s.LastError, &s.NextRetryAt, &s.StartedAt, &s.CompletedAt, &s.WorkerID,
		)
		if err != nil {
			return nil, err
		}
		states = append(states, &s)
	}
	return states, nil
}

// Logs

func (p *PostgresStore) AppendLog(entry *LogEntry) error {
	createdAt := entry.CreatedAt
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	query := `
		INSERT INTO logs (run_id, step_id, attempt, stream, content, created_at)
		VALUES (?, ?, ?, ?, ?, ?);
	`
	_, err := p.db.Exec(p.translate(query), entry.RunID, entry.StepID, entry.Attempt, entry.Stream, entry.Content, createdAt)
	return err
}

func (p *PostgresStore) GetLogs(runID string, stepID string) ([]*LogEntry, error) {
	query := `
		SELECT id, run_id, step_id, attempt, stream, COALESCE(content, ''), created_at
		FROM logs
		WHERE run_id = ? AND step_id = ?
		ORDER BY id ASC;
	`
	rows, err := p.db.Query(p.translate(query), runID, stepID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []*LogEntry
	for rows.Next() {
		var l LogEntry
		err := rows.Scan(
			&l.ID, &l.RunID, &l.StepID, &l.Attempt, &l.Stream, &l.Content, &l.CreatedAt,
		)
		if err != nil {
			return nil, err
		}
		logs = append(logs, &l)
	}
	return logs, nil
}

// Workers

func (p *PostgresStore) RegisterWorker(w *Worker) error {
	query := `
		INSERT INTO workers (worker_id, hostname, pid, started_at, last_heartbeat_at, status)
		VALUES (?, ?, ?, ?, ?, 'ACTIVE')
		ON CONFLICT(worker_id) DO UPDATE SET
			last_heartbeat_at = excluded.last_heartbeat_at,
			status = 'ACTIVE';
	`
	_, err := p.db.Exec(p.translate(query), w.WorkerID, w.Hostname, w.PID, w.StartedAt, w.LastHeartbeatAt)
	return err
}

func (p *PostgresStore) HeartbeatWorker(workerID string) error {
	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	query := `
		UPDATE workers
		SET last_heartbeat_at = ?, status = 'ACTIVE'
		WHERE worker_id = ?;
	`
	_, err := p.db.Exec(p.translate(query), nowStr, workerID)
	return err
}

func (p *PostgresStore) GetActiveWorkers() ([]*Worker, error) {
	threshold := time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano)
	query := `
		SELECT worker_id, hostname, pid, started_at, last_heartbeat_at, status
		FROM workers
		WHERE last_heartbeat_at > ? AND status = 'ACTIVE';
	`
	rows, err := p.db.Query(p.translate(query), threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var workers []*Worker
	for rows.Next() {
		var w Worker
		err := rows.Scan(&w.WorkerID, &w.Hostname, &w.PID, &w.StartedAt, &w.LastHeartbeatAt, &w.Status)
		if err != nil {
			return nil, err
		}
		workers = append(workers, &w)
	}
	return workers, nil
}

// Leases

func (p *PostgresStore) AcquireLease(runID, stepID, workerID string, duration time.Duration) (bool, error) {
	tx, err := p.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	queryLease := `
		SELECT worker_id, expires_at, status FROM leases
		WHERE run_id = ? AND step_id = ?;
	`
	var currentWorkerID, expiresAt, status string
	err = tx.QueryRow(p.translate(queryLease), runID, stepID).Scan(&currentWorkerID, &expiresAt, &status)

	isEligible := false
	if err == sql.ErrNoRows {
		isEligible = true
	} else if err != nil {
		return false, err
	} else {
		nowStr := time.Now().UTC().Format(time.RFC3339Nano)
		if status == "RELEASED" {
			isEligible = true
		} else if expiresAt < nowStr {
			isEligible = true
		} else {
			queryWorker := `
				SELECT last_heartbeat_at, status FROM workers
				WHERE worker_id = ?;
			`
			var lastHeartbeat, workerStatus string
			err = tx.QueryRow(p.translate(queryWorker), currentWorkerID).Scan(&lastHeartbeat, &workerStatus)
			if err == sql.ErrNoRows {
				isEligible = true
			} else if err != nil {
				return false, err
			} else {
				heartbeatTime, parseErr := time.Parse(time.RFC3339Nano, lastHeartbeat)
				if parseErr != nil || workerStatus != "ACTIVE" || time.Since(heartbeatTime) > 10*time.Second {
					isEligible = true
				}
			}
		}
	}

	if !isEligible {
		return false, nil
	}

	newExpiresAt := time.Now().Add(duration).UTC().Format(time.RFC3339Nano)
	queryUpsertLease := `
		INSERT INTO leases (run_id, step_id, worker_id, expires_at, status)
		VALUES (?, ?, ?, ?, 'ACTIVE')
		ON CONFLICT(run_id, step_id) DO UPDATE SET
			worker_id = excluded.worker_id,
			expires_at = excluded.expires_at,
			status = 'ACTIVE';
	`
	_, err = tx.Exec(p.translate(queryUpsertLease), runID, stepID, workerID, newExpiresAt)
	if err != nil {
		return false, err
	}

	queryStepState := `
		SELECT status, attempt, max_attempts FROM step_states
		WHERE run_id = ? AND step_id = ?;
	`
	var stepStatus string
	var attempt, maxAttempts int
	err = tx.QueryRow(p.translate(queryStepState), runID, stepID).Scan(&stepStatus, &attempt, &maxAttempts)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	nextAttempt := attempt
	if stepStatus == "PENDING" || stepStatus == "" {
		nextAttempt = 1
	} else if stepStatus == "RETRY_SCHEDULED" {
		nextAttempt = attempt + 1
	} else if stepStatus == "RUNNING" {
		if nextAttempt < 1 {
			nextAttempt = 1
		}
	}

	queryUpsertStepState := `
		INSERT INTO step_states (run_id, step_id, status, attempt, max_attempts, started_at, worker_id)
		VALUES (?, ?, 'RUNNING', ?, ?, ?, ?)
		ON CONFLICT(run_id, step_id) DO UPDATE SET
			status = 'RUNNING',
			attempt = excluded.attempt,
			max_attempts = excluded.max_attempts,
			started_at = excluded.started_at,
			worker_id = excluded.worker_id;
	`
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	_, err = tx.Exec(p.translate(queryUpsertStepState), runID, stepID, nextAttempt, maxAttempts, nowStr, workerID)
	if err != nil {
		return false, err
	}

	return true, tx.Commit()
}

func (p *PostgresStore) RenewLease(runID, stepID, workerID string, duration time.Duration) (bool, error) {
	newExpiresAt := time.Now().Add(duration).UTC().Format(time.RFC3339Nano)
	query := `
		UPDATE leases
		SET expires_at = ?
		WHERE run_id = ? AND step_id = ? AND worker_id = ? AND status = 'ACTIVE';
	`
	res, err := p.db.Exec(p.translate(query), newExpiresAt, runID, stepID, workerID)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (p *PostgresStore) ReleaseLease(runID, stepID, workerID string) error {
	query := `
		UPDATE leases
		SET status = 'RELEASED'
		WHERE run_id = ? AND step_id = ? AND worker_id = ?;
	`
	_, err := p.db.Exec(p.translate(query), runID, stepID, workerID)
	return err
}

// Timers

func (p *PostgresStore) CreateTimer(t *Timer) error {
	query := `
		INSERT INTO timers (timer_id, run_id, step_id, fire_at, status, payload_json)
		VALUES (?, ?, ?, ?, ?, ?);
	`
	_, err := p.db.Exec(p.translate(query), t.TimerID, t.RunID, t.StepID, t.FireAt, t.Status, t.PayloadJSON)
	return err
}

func (p *PostgresStore) GetTimer(runID, stepID string) (*Timer, error) {
	query := `
		SELECT timer_id, run_id, step_id, fire_at, status, COALESCE(payload_json, '{}')
		FROM timers
		WHERE run_id = ? AND step_id = ?;
	`
	var t Timer
	err := p.db.QueryRow(p.translate(query), runID, stepID).Scan(&t.TimerID, &t.RunID, &t.StepID, &t.FireAt, &t.Status, &t.PayloadJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &t, err
}

func (p *PostgresStore) FireTimer(timerID string) error {
	query := `
		UPDATE timers SET status = 'FIRED' WHERE timer_id = ?;
	`
	_, err := p.db.Exec(p.translate(query), timerID)
	return err
}

func (p *PostgresStore) CancelTimer(timerID string) error {
	query := `
		UPDATE timers SET status = 'CANCELLED' WHERE timer_id = ?;
	`
	_, err := p.db.Exec(p.translate(query), timerID)
	return err
}

// Cron Schedules

func (p *PostgresStore) UpsertCronSchedule(cs *CronSchedule) error {
	lastRunID := sqlNullString(cs.LastRunID)
	lastRunTime := sqlNullString(cs.LastRunTime)

	query := `
		INSERT INTO cron_schedules (workflow_name, cron_expression, overlap_policy, last_run_id, last_run_time, next_run_time, definition_yaml, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workflow_name) DO UPDATE SET
			cron_expression = excluded.cron_expression,
			overlap_policy = excluded.overlap_policy,
			next_run_time = excluded.next_run_time,
			definition_yaml = excluded.definition_yaml,
			status = excluded.status;
	`
	_, err := p.db.Exec(p.translate(query), cs.WorkflowName, cs.CronExpression, cs.OverlapPolicy, lastRunID, lastRunTime, cs.NextRunTime, cs.DefinitionYAML, cs.Status)
	return err
}

func (p *PostgresStore) GetDueCronSchedules(nowStr string) ([]*CronSchedule, error) {
	query := `
		SELECT workflow_name, cron_expression, overlap_policy, COALESCE(last_run_id, ''), COALESCE(last_run_time, ''), next_run_time, definition_yaml, status
		FROM cron_schedules
		WHERE next_run_time <= ? AND status = 'ACTIVE';
	`
	rows, err := p.db.Query(p.translate(query), nowStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*CronSchedule
	for rows.Next() {
		var cs CronSchedule
		err := rows.Scan(&cs.WorkflowName, &cs.CronExpression, &cs.OverlapPolicy, &cs.LastRunID, &cs.LastRunTime, &cs.NextRunTime, &cs.DefinitionYAML, &cs.Status)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, &cs)
	}
	return schedules, nil
}

func (p *PostgresStore) UpdateCronScheduleNextRun(workflowName, lastRunID, lastRunTime, nextRunTime string) error {
	query := `
		UPDATE cron_schedules
		SET last_run_id = ?, last_run_time = ?, next_run_time = ?
		WHERE workflow_name = ?;
	`
	_, err := p.db.Exec(p.translate(query), sqlNullString(lastRunID), sqlNullString(lastRunTime), nextRunTime, workflowName)
	return err
}

func (p *PostgresStore) ListCronSchedules() ([]*CronSchedule, error) {
	query := `
		SELECT workflow_name, cron_expression, overlap_policy, COALESCE(last_run_id, ''), COALESCE(last_run_time, ''), next_run_time, definition_yaml, status
		FROM cron_schedules
		ORDER BY workflow_name ASC;
	`
	rows, err := p.db.Query(p.translate(query))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*CronSchedule
	for rows.Next() {
		var cs CronSchedule
		err := rows.Scan(&cs.WorkflowName, &cs.CronExpression, &cs.OverlapPolicy, &cs.LastRunID, &cs.LastRunTime, &cs.NextRunTime, &cs.DefinitionYAML, &cs.Status)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, &cs)
	}
	return schedules, nil
}

func (p *PostgresStore) TriggerCronSchedule(workflowName string, now time.Time) (string, bool, error) {
	tx, err := p.db.Begin()
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()

	query := `
		SELECT cron_expression, overlap_policy, COALESCE(last_run_id, ''), next_run_time, definition_yaml, status
		FROM cron_schedules
		WHERE workflow_name = ?;
	`
	var cronExpr, overlapPolicy, lastRunID, nextRunTimeStr, definitionYAML, status string
	err = tx.QueryRow(p.translate(query), workflowName).Scan(&cronExpr, &overlapPolicy, &lastRunID, &nextRunTimeStr, &definitionYAML, &status)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, err
	}

	if status != "ACTIVE" {
		return "", false, nil
	}

	nowStr := now.UTC().Format(time.RFC3339Nano)
	if nextRunTimeStr > nowStr {
		return "", false, nil
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return "", false, err
	}
	nextRunTime := sched.Next(now).UTC().Format(time.RFC3339Nano)

	var def workflow.WorkflowDef
	if err := yaml.Unmarshal([]byte(definitionYAML), &def); err != nil {
		return "", false, err
	}

	if overlapPolicy == "skip" && lastRunID != "" {
		var lastStatus string
		err = tx.QueryRow(p.translate("SELECT status FROM workflow_runs WHERE run_id = ?"), lastRunID).Scan(&lastStatus)
		if err == nil && (lastStatus == "RUNNING" || lastStatus == "CREATED") {
			updateQuery := `
				UPDATE cron_schedules
				SET next_run_time = ?
				WHERE workflow_name = ?;
			`
			_, err = tx.Exec(p.translate(updateQuery), nextRunTime, workflowName)
			if err != nil {
				return "", false, err
			}
			return "", false, tx.Commit()
		}
	}

	_, _, orderedSteps, err := workflow.ParseAndValidate([]byte(definitionYAML))
	if err != nil {
		return "", false, err
	}

	// Trigger run
	runID := uuid.New().String()

	insertRun := `
		INSERT INTO workflow_runs (run_id, workflow_name, workflow_version, status, created_at, metadata_json)
		VALUES (?, ?, ?, 'CREATED', ?, '{}');
	`
	_, err = tx.Exec(p.translate(insertRun), runID, def.Name, def.Version, nowStr)
	if err != nil {
		return "", false, err
	}

	insertEvent := `
		INSERT INTO events (run_id, workflow_name, event_type, payload_json)
		VALUES (?, ?, ?, ?);
	`
	_, err = tx.Exec(p.translate(insertEvent), runID, def.Name, "WorkflowRunCreated", "{}")
	if err != nil {
		return "", false, err
	}

	payloadJSON := fmt.Sprintf(`{"cron_expression":%q,"overlap_policy":%q}`, cronExpr, overlapPolicy)
	_, err = tx.Exec(p.translate(insertEvent), runID, def.Name, "CronWorkflowTriggered", payloadJSON)
	if err != nil {
		return "", false, err
	}

	insertStepState := `
		INSERT INTO step_states (run_id, step_id, status, attempt, max_attempts)
		VALUES (?, ?, 'PENDING', 0, ?);
	`
	for _, step := range orderedSteps {
		maxAttempts := step.Retry.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		_, err = tx.Exec(p.translate(insertStepState), runID, step.ID, maxAttempts)
		if err != nil {
			return "", false, err
		}
	}

	updateRunStatus := `
		UPDATE workflow_runs
		SET status = 'RUNNING', started_at = ?
		WHERE run_id = ?;
	`
	_, err = tx.Exec(p.translate(updateRunStatus), nowStr, runID)
	if err != nil {
		return "", false, err
	}

	_, err = tx.Exec(p.translate(insertEvent), runID, def.Name, "WorkflowStarted", "{}")
	if err != nil {
		return "", false, err
	}

	updateCron := `
		UPDATE cron_schedules
		SET last_run_id = ?, last_run_time = ?, next_run_time = ?
		WHERE workflow_name = ?;
	`
	_, err = tx.Exec(p.translate(updateCron), runID, nowStr, nextRunTime, workflowName)
	if err != nil {
		return "", false, err
	}

	return runID, true, tx.Commit()
}

// Helpers

func sqlNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: s, Valid: true}
}
