package store

const Schema = `
-- Workflow definitions
CREATE TABLE IF NOT EXISTS workflow_definitions (
    name            TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    definition_hash TEXT NOT NULL,
    definition_yaml TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (name, version)
);

-- Workflow runs
CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id           TEXT PRIMARY KEY,
    workflow_name    TEXT NOT NULL,
    workflow_version INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'CREATED',
    created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at       TEXT,
    completed_at     TEXT,
    failed_at        TEXT,
    metadata_json    TEXT
);

-- Event log
CREATE TABLE IF NOT EXISTS events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT NOT NULL,
    workflow_name  TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    step_id        TEXT,
    worker_id      TEXT,
    attempt        INTEGER,
    payload_json   TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Step states
CREATE TABLE IF NOT EXISTS step_states (
    run_id       TEXT NOT NULL,
    step_id      TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',
    attempt      INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    last_error   TEXT,
    next_retry_at TEXT,
    started_at   TEXT,
    completed_at TEXT,
    worker_id    TEXT,
    PRIMARY KEY (run_id, step_id)
);

-- Logs
CREATE TABLE IF NOT EXISTS logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT NOT NULL,
    step_id    TEXT NOT NULL,
    attempt    INTEGER NOT NULL DEFAULT 1,
    stream     TEXT NOT NULL,  -- 'stdout' | 'stderr'
    content    TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_step_states_run ON step_states(run_id);
CREATE INDEX IF NOT EXISTS idx_logs_run_step ON logs(run_id, step_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status ON workflow_runs(status);
`
