# DuraFlow V1 Architecture

DuraFlow is a local-first durable execution engine that runs multi-step backend operations reliably.

## System Components

```text
                      ┌─────────────────┐
                      │   DuraFlow CLI   │
                      └────────┬────────┘
                               │
                       ┌────────▼────────┐
                       │   API / Core     │
                       └────────┬────────┘
                               │
         ┌──────────────────────┼──────────────────────┐
         │                      │                      │
 ┌───────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
 │ Workflow Engine │    │  Scheduler      │    │ Worker Manager  │
 └───────┬────────┘    └────────┬────────┘    └────────┬────────┘
         │                      │                      │
         └──────────────┬───────┴──────────────┬───────┘
                        │                      │
               ┌────────▼────────┐    ┌────────▼────────┐
               │ SQLite Event DB │    │ Step Executor    │
               └─────────────────┘    └─────────────────┘
```

1. **CLI**: The main interface for defining, executing, and inspecting workflows.
2. **Workflow Engine**: The orchestration brain. Manages the execution lifecycle, transitions step/run states, and appends history events to the SQLite Event Store.
3. **SQLite Event DB**: Persists event logs, workflow definitions, run states, and attempt logs. It acts as the single source of truth for the workflow's state.
4. **Step Executor**: The execution boundary. Runs commands as host subprocesses with process group isolation and timeout constraints.

## Event Model

Every state change during execution is recorded as an immutable event in the `events` table:

- **`WorkflowRunCreated`**: Emitted when a new run is initialized.
- **`WorkflowStarted`**: Emitted when the workflow transitions to `RUNNING`.
- **`StepScheduled`**: Emitted when a step is queued/ready for execution.
- **`StepStarted`**: Emitted when a step begins executing.
- **`StepSucceeded`**: Emitted when a step finishes with exit code `0`.
- **`StepAttemptFailed`**: Emitted when a step fails with a non-zero exit code or timeout.
- **`WorkflowCompleted`**: Emitted when all steps in the topological order succeed.
- **`WorkflowFailed`**: Emitted if any step fails, aborting the run.

## SQLite Database Schema

The database contains 5 tables for Phase 1:

1. **`workflow_definitions`**: Stores registered workflow YAMLs and version metadata.
2. **`workflow_runs`**: Materialized view tracking run statuses (`CREATED`, `RUNNING`, `COMPLETED`, `FAILED`).
3. **`events`**: Append-only event history log.
4. **`step_states`**: Materialized view of step statuses.
5. **`logs`**: Stores stdout/stderr outputs for each attempt.

## Process Isolation & Execution Semantics

Steps run as local subprocesses via `sh -c`.
To prevent orphaned processes when timeouts or cancellations occur, DuraFlow creates a dedicated process group (`setpgid`) for each step process, enabling immediate termination of the process and all its children via group SIGKILL.
