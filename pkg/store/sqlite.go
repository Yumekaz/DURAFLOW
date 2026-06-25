package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	_ "modernc.org/sqlite"
	"github.com/yumekaz/duraflow/pkg/workflow"
	"gopkg.in/yaml.v3"
)

type SQLiteStore struct {
	dbPath string
	db     *sql.DB
}

func NewSQLiteStore(dbPath string) *SQLiteStore {
	return &SQLiteStore{
		dbPath: dbPath,
	}
}

func (s *SQLiteStore) Init() error {
	dir := filepath.Dir(s.dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	db, err := sql.Open("sqlite", s.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	s.db = db

	// Enable WAL mode, busy timeout, and foreign keys
	if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		return fmt.Errorf("failed to set WAL mode: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		return fmt.Errorf("failed to set busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON;"); err != nil {
		return fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	// Create tables
	if _, err := db.Exec(Schema); err != nil {
		return fmt.Errorf("failed to execute schema: %w", err)
	}

	return nil
}

func (s *SQLiteStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SQLiteStore) RegisterWorkflow(def *workflow.WorkflowDef, hash string, yamlContent string) error {
	query := `
		INSERT INTO workflow_definitions (name, version, definition_hash, definition_yaml)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(name, version) DO UPDATE SET
			definition_hash = excluded.definition_hash,
			definition_yaml = excluded.definition_yaml;
	`
	_, err := s.db.Exec(query, def.Name, def.Version, hash, yamlContent)
	if err != nil {
		return fmt.Errorf("failed to register workflow: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetWorkflowDef(name string, version int) (*workflow.WorkflowDef, error) {
	query := `
		SELECT definition_yaml FROM workflow_definitions
		WHERE name = ? AND version = ?;
	`
	var yamlContent string
	err := s.db.QueryRow(query, name, version).Scan(&yamlContent)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("workflow definition not found: %s v%d", name, version)
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch workflow definition: %w", err)
	}

	var def workflow.WorkflowDef
	if err := yaml.Unmarshal([]byte(yamlContent), &def); err != nil {
		return nil, fmt.Errorf("failed to parse yaml definition from store: %w", err)
	}

	return &def, nil
}

func (s *SQLiteStore) CreateRun(run *WorkflowRun) error {
	query := `
		INSERT INTO workflow_runs (run_id, workflow_name, workflow_version, status, metadata_json)
		VALUES (?, ?, ?, ?, ?);
	`
	_, err := s.db.Exec(query, run.RunID, run.WorkflowName, run.WorkflowVersion, run.Status, run.MetadataJSON)
	if err != nil {
		return fmt.Errorf("failed to create workflow run: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpdateRunStatus(runID string, status string, timestamps map[string]string) error {
	query := "UPDATE workflow_runs SET status = ?"
	args := []interface{}{status}

	if val, ok := timestamps["started_at"]; ok {
		query += ", started_at = ?"
		args = append(args, val)
	}
	if val, ok := timestamps["completed_at"]; ok {
		query += ", completed_at = ?"
		args = append(args, val)
	}
	if val, ok := timestamps["failed_at"]; ok {
		query += ", failed_at = ?"
		args = append(args, val)
	}

	query += " WHERE run_id = ?"
	args = append(args, runID)

	_, err := s.db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to update run status: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetRun(runID string) (*WorkflowRun, error) {
	query := `
		SELECT run_id, workflow_name, workflow_version, status, created_at,
		       COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(failed_at, ''), COALESCE(metadata_json, '')
		FROM workflow_runs
		WHERE run_id = ?;
	`
	var run WorkflowRun
	err := s.db.QueryRow(query, runID).Scan(
		&run.RunID, &run.WorkflowName, &run.WorkflowVersion, &run.Status, &run.CreatedAt,
		&run.StartedAt, &run.CompletedAt, &run.FailedAt, &run.MetadataJSON,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("workflow run not found: %s", runID)
	} else if err != nil {
		return nil, fmt.Errorf("failed to get workflow run: %w", err)
	}
	return &run, nil
}

func (s *SQLiteStore) ListRuns(limit int) ([]*WorkflowRun, error) {
	query := `
		SELECT run_id, workflow_name, workflow_version, status, created_at,
		       COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(failed_at, ''), COALESCE(metadata_json, '')
		FROM workflow_runs
		ORDER BY created_at DESC
		LIMIT ?;
	`
	rows, err := s.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflow runs: %w", err)
	}
	defer rows.Close()

	var runs []*WorkflowRun
	for rows.Next() {
		var run WorkflowRun
		err := rows.Scan(
			&run.RunID, &run.WorkflowName, &run.WorkflowVersion, &run.Status, &run.CreatedAt,
			&run.StartedAt, &run.CompletedAt, &run.FailedAt, &run.MetadataJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow run: %w", err)
		}
		runs = append(runs, &run)
	}
	return runs, nil
}

func (s *SQLiteStore) AppendEvent(event *Event) error {
	query := `
		INSERT INTO events (run_id, workflow_name, event_type, step_id, worker_id, attempt, payload_json)
		VALUES (?, ?, ?, ?, ?, ?, ?);
	`
	res, err := s.db.Exec(query, event.RunID, event.WorkflowName, event.EventType, event.StepID, event.WorkerID, event.Attempt, event.PayloadJSON)
	if err != nil {
		return fmt.Errorf("failed to append event: %w", err)
	}
	id, err := res.LastInsertId()
	if err == nil {
		event.ID = id
	}
	return nil
}

func (s *SQLiteStore) GetEvents(runID string) ([]*Event, error) {
	query := `
		SELECT id, run_id, workflow_name, event_type, COALESCE(step_id, ''), COALESCE(worker_id, ''), COALESCE(attempt, 0), COALESCE(payload_json, ''), created_at
		FROM events
		WHERE run_id = ?
		ORDER BY id ASC;
	`
	rows, err := s.db.Query(query, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var ev Event
		err := rows.Scan(&ev.ID, &ev.RunID, &ev.WorkflowName, &ev.EventType, &ev.StepID, &ev.WorkerID, &ev.Attempt, &ev.PayloadJSON, &ev.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan event: %w", err)
		}
		events = append(events, &ev)
	}
	return events, nil
}

func (s *SQLiteStore) UpsertStepState(state *StepState) error {
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
	_, err := s.db.Exec(query,
		state.RunID, state.StepID, state.Status, state.Attempt, state.MaxAttempts,
		state.LastError, state.NextRetryAt, state.StartedAt, state.CompletedAt, state.WorkerID,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert step state: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetStepStates(runID string) ([]*StepState, error) {
	query := `
		SELECT run_id, step_id, status, attempt, max_attempts, COALESCE(last_error, ''),
		       COALESCE(next_retry_at, ''), COALESCE(started_at, ''), COALESCE(completed_at, ''), COALESCE(worker_id, '')
		FROM step_states
		WHERE run_id = ?;
	`
	rows, err := s.db.Query(query, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query step states: %w", err)
	}
	defer rows.Close()

	var states []*StepState
	for rows.Next() {
		var st StepState
		err := rows.Scan(
			&st.RunID, &st.StepID, &st.Status, &st.Attempt, &st.MaxAttempts, &st.LastError,
			&st.NextRetryAt, &st.StartedAt, &st.CompletedAt, &st.WorkerID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan step state: %w", err)
		}
		states = append(states, &st)
	}
	return states, nil
}

func (s *SQLiteStore) AppendLog(entry *LogEntry) error {
	query := `
		INSERT INTO logs (run_id, step_id, attempt, stream, content)
		VALUES (?, ?, ?, ?, ?);
	`
	res, err := s.db.Exec(query, entry.RunID, entry.StepID, entry.Attempt, entry.Stream, entry.Content)
	if err != nil {
		return fmt.Errorf("failed to append log: %w", err)
	}
	id, err := res.LastInsertId()
	if err == nil {
		entry.ID = id
	}
	return nil
}

func (s *SQLiteStore) GetLogs(runID string, stepID string) ([]*LogEntry, error) {
	query := `
		SELECT id, run_id, step_id, attempt, stream, COALESCE(content, ''), created_at
		FROM logs
		WHERE run_id = ? AND step_id = ?
		ORDER BY id ASC;
	`
	rows, err := s.db.Query(query, runID, stepID)
	if err != nil {
		return nil, fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	var entries []*LogEntry
	for rows.Next() {
		var le LogEntry
		err := rows.Scan(&le.ID, &le.RunID, &le.StepID, &le.Attempt, &le.Stream, &le.Content, &le.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log entry: %w", err)
		}
		entries = append(entries, &le)
	}
	return entries, nil
}

func (s *SQLiteStore) GetWorkflowYAML(name string, version int) (string, error) {
	query := `
		SELECT definition_yaml FROM workflow_definitions
		WHERE name = ? AND version = ?;
	`
	var yamlContent string
	err := s.db.QueryRow(query, name, version).Scan(&yamlContent)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("workflow definition not found: %s v%d", name, version)
	} else if err != nil {
		return "", fmt.Errorf("failed to fetch workflow definition: %w", err)
	}
	return yamlContent, nil
}

func (s *SQLiteStore) GetIncompleteRuns() ([]*WorkflowRun, error) {
	query := `
		SELECT run_id, workflow_name, workflow_version, status, created_at, started_at, completed_at, failed_at, metadata_json
		FROM workflow_runs
		WHERE status = 'RUNNING' OR status = 'CREATED' OR status = 'COMPENSATING'
		ORDER BY created_at ASC;
	`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query incomplete runs: %w", err)
	}
	defer rows.Close()

	var runs []*WorkflowRun
	for rows.Next() {
		var r WorkflowRun
		var startedAt, completedAt, failedAt sql.NullString
		err := rows.Scan(
			&r.RunID, &r.WorkflowName, &r.WorkflowVersion, &r.Status,
			&r.CreatedAt, &startedAt, &completedAt, &failedAt, &r.MetadataJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow run: %w", err)
		}
		if startedAt.Valid {
			r.StartedAt = startedAt.String
		}
		if completedAt.Valid {
			r.CompletedAt = completedAt.String
		}
		if failedAt.Valid {
			r.FailedAt = failedAt.String
		}
		runs = append(runs, &r)
	}
	return runs, nil
}

func (s *SQLiteStore) ResetStepState(runID, stepID string) error {
	query := `
		UPDATE step_states
		SET status = 'PENDING', attempt = 0, last_error = NULL, completed_at = NULL, started_at = NULL, worker_id = NULL
		WHERE run_id = ? AND step_id = ?;
	`
	_, err := s.db.Exec(query, runID, stepID)
	if err != nil {
		return fmt.Errorf("failed to reset step state: %w", err)
	}
	return nil
}

func (s *SQLiteStore) CancelWorkflowRun(runID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)

	res, err := tx.Exec(`
		UPDATE workflow_runs
		SET status = 'CANCELLED', failed_at = ?
		WHERE run_id = ? AND (status = 'RUNNING' OR status = 'CREATED' OR status = 'COMPENSATING');
	`, now, runID)
	if err != nil {
		return fmt.Errorf("failed to update run status for cancellation: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("workflow run not active or not found: %s", runID)
	}

	var name string
	err = tx.QueryRow("SELECT workflow_name FROM workflow_runs WHERE run_id = ?", runID).Scan(&name)
	if err != nil {
		return fmt.Errorf("failed to fetch workflow name: %w", err)
	}

	_, err = tx.Exec(`
		INSERT INTO events (run_id, workflow_name, event_type, payload_json)
		VALUES (?, ?, 'WorkflowCancelled', '{}');
	`, runID, name)
	if err != nil {
		return fmt.Errorf("failed to append cancellation event: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) ResetWorkflowRunForRetry(runID string, status string) error {
	query := `
		UPDATE workflow_runs
		SET status = ?, failed_at = NULL, completed_at = NULL
		WHERE run_id = ?;
	`
	_, err := s.db.Exec(query, status, runID)
	if err != nil {
		return fmt.Errorf("failed to reset workflow run for retry: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RegisterWorker(w *Worker) error {
	query := `
		INSERT INTO workers (worker_id, hostname, pid, started_at, last_heartbeat_at, status)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(worker_id) DO UPDATE SET
			hostname = excluded.hostname,
			pid = excluded.pid,
			last_heartbeat_at = excluded.last_heartbeat_at,
			status = excluded.status;
	`
	_, err := s.db.Exec(query, w.WorkerID, w.Hostname, w.PID, w.StartedAt, w.LastHeartbeatAt, w.Status)
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}
	return nil
}

func (s *SQLiteStore) HeartbeatWorker(workerID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := `
		UPDATE workers SET last_heartbeat_at = ?, status = 'ACTIVE'
		WHERE worker_id = ?;
	`
	_, err := s.db.Exec(query, now, workerID)
	if err != nil {
		return fmt.Errorf("failed to heartbeat worker: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetActiveWorkers() ([]*Worker, error) {
	threshold := time.Now().Add(-10 * time.Second).UTC().Format(time.RFC3339Nano)
	query := `
		SELECT worker_id, hostname, pid, started_at, last_heartbeat_at, status
		FROM workers
		WHERE status = 'ACTIVE' AND last_heartbeat_at >= ?;
	`
	rows, err := s.db.Query(query, threshold)
	if err != nil {
		return nil, fmt.Errorf("failed to query active workers: %w", err)
	}
	defer rows.Close()

	var workers []*Worker
	for rows.Next() {
		var w Worker
		err := rows.Scan(&w.WorkerID, &w.Hostname, &w.PID, &w.StartedAt, &w.LastHeartbeatAt, &w.Status)
		if err != nil {
			return nil, fmt.Errorf("failed to scan worker: %w", err)
		}
		workers = append(workers, &w)
	}
	return workers, nil
}

func (s *SQLiteStore) AcquireLease(runID, stepID, workerID string, duration time.Duration) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	queryLease := `
		SELECT worker_id, expires_at, status FROM leases
		WHERE run_id = ? AND step_id = ?;
	`
	var currentWorkerID, expiresAt, status string
	err = tx.QueryRow(queryLease, runID, stepID).Scan(&currentWorkerID, &expiresAt, &status)

	isEligible := false
	if err == sql.ErrNoRows {
		isEligible = true
	} else if err != nil {
		return false, fmt.Errorf("failed to query lease: %w", err)
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
			err = tx.QueryRow(queryWorker, currentWorkerID).Scan(&lastHeartbeat, &workerStatus)
			if err == sql.ErrNoRows {
				isEligible = true
			} else if err != nil {
				return false, fmt.Errorf("failed to query owning worker: %w", err)
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
	_, err = tx.Exec(queryUpsertLease, runID, stepID, workerID, newExpiresAt)
	if err != nil {
		return false, fmt.Errorf("failed to upsert lease: %w", err)
	}

	queryStepState := `
		SELECT status, attempt, max_attempts FROM step_states
		WHERE run_id = ? AND step_id = ?;
	`
	var stepStatus string
	var attempt, maxAttempts int
	err = tx.QueryRow(queryStepState, runID, stepID).Scan(&stepStatus, &attempt, &maxAttempts)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("failed to query step state: %w", err)
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
	_, err = tx.Exec(queryUpsertStepState, runID, stepID, nextAttempt, maxAttempts, nowStr, workerID)
	if err != nil {
		return false, fmt.Errorf("failed to update step state to running: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return true, nil
}

func (s *SQLiteStore) RenewLease(runID, stepID, workerID string, duration time.Duration) (bool, error) {
	expiresAt := time.Now().Add(duration).UTC().Format(time.RFC3339Nano)
	query := `
		UPDATE leases SET expires_at = ?
		WHERE run_id = ? AND step_id = ? AND worker_id = ? AND status = 'ACTIVE';
	`
	res, err := s.db.Exec(query, expiresAt, runID, stepID, workerID)
	if err != nil {
		return false, fmt.Errorf("failed to renew lease: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *SQLiteStore) ReleaseLease(runID, stepID, workerID string) error {
	query := `
		UPDATE leases SET status = 'RELEASED'
		WHERE run_id = ? AND step_id = ? AND worker_id = ? AND status = 'ACTIVE';
	`
	_, err := s.db.Exec(query, runID, stepID, workerID)
	if err != nil {
		return fmt.Errorf("failed to release lease: %w", err)
	}
	return nil
}

func (s *SQLiteStore) CreateTimer(t *Timer) error {
	query := `
		INSERT INTO timers (timer_id, run_id, step_id, fire_at, status, payload_json)
		VALUES (?, ?, ?, ?, 'PENDING', ?)
		ON CONFLICT(timer_id) DO UPDATE SET
			fire_at = excluded.fire_at,
			status = 'PENDING',
			payload_json = excluded.payload_json;
	`
	if t.PayloadJSON == "" {
		t.PayloadJSON = "{}"
	}
	_, err := s.db.Exec(query, t.TimerID, t.RunID, t.StepID, t.FireAt, t.PayloadJSON)
	if err != nil {
		return fmt.Errorf("failed to create timer: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetTimer(runID, stepID string) (*Timer, error) {
	query := `
		SELECT timer_id, run_id, step_id, fire_at, status, payload_json
		FROM timers
		WHERE run_id = ? AND step_id = ?;
	`
	var t Timer
	err := s.db.QueryRow(query, runID, stepID).Scan(&t.TimerID, &t.RunID, &t.StepID, &t.FireAt, &t.Status, &t.PayloadJSON)
	if err == sql.ErrNoRows {
		return nil, nil // not found
	} else if err != nil {
		return nil, fmt.Errorf("failed to get timer: %w", err)
	}
	return &t, nil
}

func (s *SQLiteStore) FireTimer(timerID string) error {
	query := `
		UPDATE timers SET status = 'FIRED'
		WHERE timer_id = ? AND status = 'PENDING';
	`
	_, err := s.db.Exec(query, timerID)
	if err != nil {
		return fmt.Errorf("failed to fire timer: %w", err)
	}
	return nil
}

func (s *SQLiteStore) CancelTimer(timerID string) error {
	query := `
		UPDATE timers SET status = 'CANCELLED'
		WHERE timer_id = ? AND status = 'PENDING';
	`
	_, err := s.db.Exec(query, timerID)
	if err != nil {
		return fmt.Errorf("failed to cancel timer: %w", err)
	}
	return nil
}

func (s *SQLiteStore) UpsertCronSchedule(cs *CronSchedule) error {
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
	var lastRunID, lastRunTime interface{}
	if cs.LastRunID != "" {
		lastRunID = cs.LastRunID
	}
	if cs.LastRunTime != "" {
		lastRunTime = cs.LastRunTime
	}
	_, err := s.db.Exec(query, cs.WorkflowName, cs.CronExpression, cs.OverlapPolicy, lastRunID, lastRunTime, cs.NextRunTime, cs.DefinitionYAML, cs.Status)
	if err != nil {
		return fmt.Errorf("failed to upsert cron schedule: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetDueCronSchedules(nowStr string) ([]*CronSchedule, error) {
	query := `
		SELECT workflow_name, cron_expression, overlap_policy, COALESCE(last_run_id, ''), COALESCE(last_run_time, ''), next_run_time, definition_yaml, status
		FROM cron_schedules
		WHERE status = 'ACTIVE' AND next_run_time <= ?;
	`
	rows, err := s.db.Query(query, nowStr)
	if err != nil {
		return nil, fmt.Errorf("failed to query due cron schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*CronSchedule
	for rows.Next() {
		var cs CronSchedule
		err := rows.Scan(&cs.WorkflowName, &cs.CronExpression, &cs.OverlapPolicy, &cs.LastRunID, &cs.LastRunTime, &cs.NextRunTime, &cs.DefinitionYAML, &cs.Status)
		if err != nil {
			return nil, fmt.Errorf("failed to scan cron schedule: %w", err)
		}
		schedules = append(schedules, &cs)
	}
	return schedules, nil
}

func (s *SQLiteStore) UpdateCronScheduleNextRun(workflowName, lastRunID, lastRunTime, nextRunTime string) error {
	query := `
		UPDATE cron_schedules
		SET last_run_id = ?, last_run_time = ?, next_run_time = ?
		WHERE workflow_name = ?;
	`
	_, err := s.db.Exec(query, lastRunID, lastRunTime, nextRunTime, workflowName)
	if err != nil {
		return fmt.Errorf("failed to update cron next run: %w", err)
	}
	return nil
}

func (s *SQLiteStore) ListCronSchedules() ([]*CronSchedule, error) {
	query := `
		SELECT workflow_name, cron_expression, overlap_policy, COALESCE(last_run_id, ''), COALESCE(last_run_time, ''), next_run_time, definition_yaml, status
		FROM cron_schedules
		ORDER BY workflow_name ASC;
	`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to list cron schedules: %w", err)
	}
	defer rows.Close()

	var schedules []*CronSchedule
	for rows.Next() {
		var cs CronSchedule
		err := rows.Scan(&cs.WorkflowName, &cs.CronExpression, &cs.OverlapPolicy, &cs.LastRunID, &cs.LastRunTime, &cs.NextRunTime, &cs.DefinitionYAML, &cs.Status)
		if err != nil {
			return nil, fmt.Errorf("failed to scan cron schedule: %w", err)
		}
		schedules = append(schedules, &cs)
	}
	return schedules, nil
}

func (s *SQLiteStore) TriggerCronSchedule(workflowName string, now time.Time) (string, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return "", false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Query the cron schedule
	query := `
		SELECT cron_expression, overlap_policy, COALESCE(last_run_id, ''), next_run_time, definition_yaml, status
		FROM cron_schedules
		WHERE workflow_name = ?;
	`
	var cronExpr, overlapPolicy, lastRunID, nextRunTimeStr, definitionYAML, status string
	err = tx.QueryRow(query, workflowName).Scan(&cronExpr, &overlapPolicy, &lastRunID, &nextRunTimeStr, &definitionYAML, &status)
	if err == sql.ErrNoRows {
		return "", false, nil
	} else if err != nil {
		return "", false, fmt.Errorf("failed to query cron schedule: %w", err)
	}

	if status != "ACTIVE" {
		return "", false, nil
	}

	nowStr := now.UTC().Format(time.RFC3339Nano)
	if nextRunTimeStr > nowStr {
		return "", false, nil // not due yet
	}

	// Parse cron expression to calculate the next execution time
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(cronExpr)
	if err != nil {
		return "", false, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}
	nextRunTime := sched.Next(now).UTC().Format(time.RFC3339Nano)

	// Parse definition to get name, version, and steps
	var def workflow.WorkflowDef
	if err := yaml.Unmarshal([]byte(definitionYAML), &def); err != nil {
		return "", false, fmt.Errorf("failed to parse yaml: %w", err)
	}

	// Check overlap policy if skip
	if overlapPolicy == "skip" && lastRunID != "" {
		// Check status of last run
		var lastStatus string
		err = tx.QueryRow("SELECT status FROM workflow_runs WHERE run_id = ?", lastRunID).Scan(&lastStatus)
		if err == nil && (lastStatus == "RUNNING" || lastStatus == "CREATED") {
			// Skip triggering! Just update next_run_time
			updateQuery := `
				UPDATE cron_schedules
				SET next_run_time = ?
				WHERE workflow_name = ?;
			`
			_, err = tx.Exec(updateQuery, nextRunTime, workflowName)
			if err != nil {
				return "", false, fmt.Errorf("failed to update cron next_run_time on skip: %w", err)
			}
			if err := tx.Commit(); err != nil {
				return "", false, fmt.Errorf("failed to commit skip: %w", err)
			}
			return "", false, nil
		}
	}

	// Calculate hash and ordered steps for RunWorkflow preparation
	_, _, orderedSteps, err := workflow.ParseAndValidate([]byte(definitionYAML))
	if err != nil {
		return "", false, fmt.Errorf("failed to parse/validate: %w", err)
	}

	// Trigger run:
	runID := uuid.New().String()
	
	// 1. Create run record
	insertRun := `
		INSERT INTO workflow_runs (run_id, workflow_name, workflow_version, status, created_at, metadata_json)
		VALUES (?, ?, ?, 'CREATED', ?, '{}');
	`
	_, err = tx.Exec(insertRun, runID, def.Name, def.Version, nowStr)
	if err != nil {
		return "", false, fmt.Errorf("failed to create run: %w", err)
	}

	// 2. Append events: WorkflowRunCreated and CronWorkflowTriggered
	insertEvent := `
		INSERT INTO events (run_id, workflow_name, event_type, payload_json)
		VALUES (?, ?, ?, ?);
	`
	_, err = tx.Exec(insertEvent, runID, def.Name, "WorkflowRunCreated", "{}")
	if err != nil {
		return "", false, fmt.Errorf("failed to append run created event: %w", err)
	}

	payloadJSON := fmt.Sprintf(`{"cron_expression":%q,"overlap_policy":%q}`, cronExpr, overlapPolicy)
	_, err = tx.Exec(insertEvent, runID, def.Name, "CronWorkflowTriggered", payloadJSON)
	if err != nil {
		return "", false, fmt.Errorf("failed to append cron triggered event: %w", err)
	}

	// 3. Initialize step states to PENDING
	insertStepState := `
		INSERT INTO step_states (run_id, step_id, status, attempt, max_attempts)
		VALUES (?, ?, 'PENDING', 0, ?);
	`
	for _, step := range orderedSteps {
		maxAttempts := step.Retry.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 1
		}
		_, err = tx.Exec(insertStepState, runID, step.ID, maxAttempts)
		if err != nil {
			return "", false, fmt.Errorf("failed to init step state: %w", err)
		}
	}

	// 4. Update workflow run status to RUNNING
	updateRunStatus := `
		UPDATE workflow_runs
		SET status = 'RUNNING', started_at = ?
		WHERE run_id = ?;
	`
	_, err = tx.Exec(updateRunStatus, nowStr, runID)
	if err != nil {
		return "", false, fmt.Errorf("failed to start run: %w", err)
	}

	_, err = tx.Exec(insertEvent, runID, def.Name, "WorkflowStarted", "{}")
	if err != nil {
		return "", false, fmt.Errorf("failed to append started event: %w", err)
	}

	// 5. Update cron schedule table with last run details
	updateCron := `
		UPDATE cron_schedules
		SET last_run_id = ?, last_run_time = ?, next_run_time = ?
		WHERE workflow_name = ?;
	`
	_, err = tx.Exec(updateCron, runID, nowStr, nextRunTime, workflowName)
	if err != nil {
		return "", false, fmt.Errorf("failed to update cron schedule: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return runID, true, nil
}


