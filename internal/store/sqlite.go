package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
	"github.com/yumekaz/duraflow/internal/workflow"
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
		WHERE status = 'RUNNING' OR status = 'CREATED'
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


