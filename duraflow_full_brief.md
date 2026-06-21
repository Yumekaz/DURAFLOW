# DuraFlow — Local-First Durable Workflow Engine

> **Status:** Future flagship project brief  
> **Primary role:** Durable operations engine  
> **Primary audience:** Backend/system engineers, platform builders, self-hosted operators, and small teams that need reliable long-running operations without heavy cloud infrastructure.  
> **Core identity:** Local-first, durable, inspectable, recoverable, zero-paid-API workflow execution for backend operations.

---

## 0. One-Line Definition

**DuraFlow is a local-first durable workflow engine that runs multi-step backend operations reliably across crashes, retries, worker failures, timers, idempotency issues, and rollback/compensation needs.**

It is not just a queue.  
It is not just cron.  
It is not just a script runner.  
It is not an AI wrapper.  
It is a **durable execution engine**.

---

## 1. Why DuraFlow Exists

Normal backend operations look simple when everything works:

```text
backup database → compress file → upload/copy → verify checksum → delete old backups
```

But real systems fail halfway:

```text
backup database → compress file → process crashes → restart → what now?
```

Or:

```text
deploy service → run migration → health check fails → rollback? retry? leave broken?
```

Most small backend systems use fragile solutions:

- shell scripts
- cron jobs
- ad-hoc queues
- background workers
- manual retries
- manually inspected logs
- half-written deployment scripts
- one-off backup scripts
- scripts with no recovery semantics

These work until:

- the machine restarts,
- the process crashes,
- the worker dies mid-task,
- the same step runs twice,
- a retry corrupts state,
- the network fails,
- a lock is lost,
- a step succeeds but completion is not recorded,
- a rollback is needed,
- the operator needs to know what actually happened.

DuraFlow exists to solve this:

> **Important backend operations should be durable, replayable in history, inspectable, recoverable, and explicit about failure.**

---

## 2. What DuraFlow Solves

DuraFlow solves the reliability gap between simple scripts and large-scale workflow platforms.

### 2.1 Problems It Handles

#### 2.1.1 Process Crash Mid-Workflow

Problem:

```text
Step 1 completed
Step 2 completed
Step 3 running
Process dies
```

Without durable state, the system does not know whether to restart from step 1, step 3, or give up.

DuraFlow persists workflow events so it can resume safely.

---

#### 2.1.2 Worker Death

Problem:

```text
Worker claimed task
Worker starts step
Worker dies before reporting status
```

DuraFlow uses worker heartbeats and leases so abandoned tasks can be detected and reclaimed.

---

#### 2.1.3 Step Runs Twice

Problem:

A worker completes a step, but crashes before recording completion. On recovery, the step may run again.

DuraFlow treats execution as **at-least-once** by default and requires explicit idempotency handling where needed.

---

#### 2.1.4 Retry Semantics

Problem:

A failed step should not always be retried forever.

DuraFlow supports:

- max attempts
- exponential backoff
- fixed delay
- retryable vs non-retryable errors
- timeout-based failure
- dead-letter/final failure state

---

#### 2.1.5 Long Delays and Timers

Problem:

Some workflows need to wait:

```text
send email → wait 24 hours → check status → retry if no response
```

DuraFlow persists timers so delays survive process restarts.

---

#### 2.1.6 Cron Jobs That Survive Restarts

Problem:

Traditional cron starts scripts, but does not naturally give durable execution history, step retries, compensation, or workflow inspection.

DuraFlow supports scheduled workflows with persisted run history.

---

#### 2.1.7 Rollback and Compensation

Problem:

Some operations cannot be truly rolled back, but can be compensated.

Example:

```text
create backup
apply migration
deploy new version
health check fails
restore old service config
```

DuraFlow supports compensation handlers that are explicit and trackable.

---

#### 2.1.8 Operational Visibility

Problem:

When something fails, operators need to know:

- which step failed,
- when it failed,
- how many times it retried,
- which worker ran it,
- what logs/errors were captured,
- whether it can resume,
- whether compensation ran,
- what the current state is.

DuraFlow provides CLI/API inspection backed by event history.

---

## 3. What DuraFlow Is Not

DuraFlow must avoid becoming vague or bloated.

### 3.1 Not Just a Queue

A queue stores tasks.

DuraFlow stores **workflow history and execution state**.

A queue says:

```text
Task pending / done / failed
```

DuraFlow says:

```text
WorkflowStarted
StepScheduled
StepLeaseAcquired
StepStarted
StepAttemptFailed
StepRetryScheduled
StepCompleted
TimerCreated
TimerFired
CompensationStarted
WorkflowCompleted
```

---

### 3.2 Not Just Cron

Cron triggers commands.

DuraFlow executes durable workflows with:

- retries
- event history
- crash recovery
- inspection
- compensation
- idempotency
- worker leases

---

### 3.3 Not a Full Temporal Clone

DuraFlow should learn from durable execution systems, but it must be scoped.

DuraFlow V1 should not attempt:

- arbitrary language SDKs,
- global-scale clusters,
- cloud service APIs,
- complex workflow determinism constraints,
- huge distributed deployment,
- enterprise UI,
- Kubernetes dependency.

The goal is:

> **small, understandable, local-first durable workflows.**

---

### 3.4 Not an AI Project

DuraFlow does not require LLMs, agents, OpenAI APIs, Claude, Gemini, Codex, or paid credits.

AI/agent integration can come later as an optional feature, but the engine must be useful without AI.

---

### 3.5 Not Autoforge / Not WebIDE

DuraFlow is independent.

It does not depend on Autoforge.  
It does not depend on WebIDE.  
It is a backend reliability engine.

---

## 4. Core Philosophy

### 4.1 Durability First

Every important workflow transition is recorded before DuraFlow moves forward.

If DuraFlow dies, it should be able to reconstruct the state of every run from stored history.

---

### 4.2 Explicit Failure Model

DuraFlow should never hide failure behind vague status.

It should show:

- step failed,
- why it failed,
- whether retry is allowed,
- whether compensation is needed,
- whether manual intervention is required.

---

### 4.3 Local-First

V1 must run on a normal developer machine using local storage.

No cloud required.  
No paid API required.  
No Kubernetes required.  
No SaaS account required.

---

### 4.4 Understandable Internals

The system should be inspectable.

A senior engineer should be able to read the architecture and understand:

- how workflows are persisted,
- how workers claim tasks,
- how retries are scheduled,
- how recovery works,
- where idempotency matters.

---

### 4.5 At-Least-Once Execution by Default

Exactly-once execution is usually an illusion in distributed/worker systems.

DuraFlow should be honest:

> A step may run more than once if failure occurs at unlucky boundaries.

Therefore:

- steps should be idempotent where possible,
- idempotency keys should be first-class,
- external side effects should be handled carefully,
- DuraFlow should document duplicate execution risks.

---

### 4.6 CLI-First, UI Later

V1 should have a strong CLI.

A dashboard can come later.

A reliable CLI is more important than a pretty UI.

---

## 5. Target Use Cases

### 5.1 Deployment Workflow

```text
clone/pull code
build artifact
run tests
create backup/snapshot
stop old service
start new service
run health check
update route
rollback if health check fails
```

---

### 5.2 Database Backup Workflow

```text
dump database
compress backup
compute checksum
verify checksum
copy to backup directory
update backup index
delete old backups
```

---

### 5.3 Restore Workflow

```text
verify backup file
stop service
restore database
run consistency check
start service
run health check
mark restore complete
```

---

### 5.4 Migration Workflow

```text
backup database
run migration
verify schema
run smoke test
mark migration complete
rollback/restore if failed
```

---

### 5.5 Cron Workflow

```text
every day at 02:00
run cleanup
archive logs
backup metadata
verify disk space
```

---

### 5.6 File Processing Pipeline

```text
scan input folder
validate file
process file
write output
move original to archive
retry failed files
```

---

### 5.7 Multi-Step Maintenance Operation

```text
check service health
rotate logs
clean temp files
restart service if needed
send/report summary
```

---

## 6. Main Personas

### 6.1 Solo Backend Builder

Needs reliable local automation for backups, deploys, migrations, and cron jobs.

Wants:

- simple install,
- local SQLite,
- CLI,
- no complex infrastructure.

---

### 6.2 Small Self-Hosted Team

Runs services on one or a few machines.

Wants:

- durable operational workflows,
- restart-safe jobs,
- visible history,
- safe backup/restore.

---

### 6.3 Platform Engineer

Wants a small durable workflow engine to embed inside a platform.

Wants:

- API,
- pluggable storage,
- worker model,
- reliable execution semantics.

---

### 6.4 Systems Student / Research Builder

Wants to understand durable execution, failure boundaries, retries, leases, and event history.

Wants:

- clear internals,
- deterministic tests,
- failure injection,
- design docs.

---

## 7. High-Level Architecture

### 7.1 V1 Architecture

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

---

### 7.2 Advanced Architecture

```text
                         ┌────────────────────┐
                         │ CLI / API / Future UI│
                         └──────────┬─────────┘
                                    │
                         ┌──────────▼─────────┐
                         │  Workflow Core      │
                         └──────────┬─────────┘
                                    │
      ┌─────────────────────────────┼─────────────────────────────┐
      │                             │                             │
┌─────▼──────┐              ┌───────▼────────┐             ┌──────▼───────┐
│ Event Store │              │ Coordinator    │             │ Step Executor │
│ SQLite / KV │              │ leases/heartbeats│           │ host/container│
└────────────┘              └────────────────┘             └──────────────┘
```

Advanced integration options:

```text
Mini Docker          → isolated step execution
Coordination Service → worker leases, heartbeats, failover
Mini Redis-Cassandra → distributed state/event backend
```

These are not required in V1.

---

## 8. Core Components

### 8.1 Workflow Definition Parser

Responsible for loading workflow definitions from YAML/JSON/code.

V1 should support YAML first because it is easy to inspect.

Example:

```yaml
name: backup-db
version: 1

steps:
  - id: dump
    run: "sqlite3 app.db '.backup backup.db'"
    retry:
      max_attempts: 3
      backoff: exponential
      initial_delay_ms: 1000

  - id: compress
    run: "gzip backup.db"
    depends_on: ["dump"]

  - id: verify
    run: "sha256sum backup.db.gz"
    depends_on: ["compress"]

compensation:
  - id: cleanup
    run: "rm -f backup.db backup.db.gz"
```

Responsibilities:

- parse workflow file,
- validate structure,
- validate step IDs,
- validate dependency graph,
- reject cycles,
- normalize retry policies,
- generate workflow version metadata.

---

### 8.2 Workflow Engine

The brain of DuraFlow.

Responsibilities:

- create workflow runs,
- schedule steps,
- track workflow state,
- apply event transitions,
- decide when a workflow is complete,
- decide when compensation should start,
- reconstruct state from event history.

---

### 8.3 Event Store

The durable heart of DuraFlow.

V1 storage:

```text
SQLite
```

Why SQLite:

- local-first,
- reliable,
- simple,
- transactional,
- zero server dependency,
- easy to inspect.

The event store persists every important transition.

---

### 8.4 Scheduler

Responsible for finding work that is ready to run.

Schedules:

- ready steps,
- retry attempts,
- timers,
- cron-triggered workflows,
- compensation steps.

---

### 8.5 Worker Manager

Responsible for worker lifecycle.

Tracks:

- worker registration,
- heartbeat time,
- active leases,
- stuck tasks,
- expired leases.

---

### 8.6 Step Executor

Runs the actual step.

V1 executor:

```text
host subprocess executor
```

Later executors:

```text
Mini Docker executor
Docker executor
remote worker executor
language SDK executor
```

Step executor responsibilities:

- run command,
- capture stdout/stderr,
- enforce timeout,
- return exit code,
- capture metadata,
- report success/failure.

---

### 8.7 Timer Service

Handles delayed events.

Responsibilities:

- store timers persistently,
- wake timers after due time,
- schedule delayed steps,
- trigger cron workflows,
- survive process restarts.

---

### 8.8 Retry Manager

Handles retry decisions.

Supports:

- max attempts,
- backoff policy,
- retryable errors,
- non-retryable failures,
- retry delay,
- attempt count tracking.

---

### 8.9 Compensation Manager

Runs compensation steps after failure when required.

Responsibilities:

- decide when compensation begins,
- determine compensation order,
- record compensation history,
- handle compensation failure,
- show final status clearly.

---

### 8.10 CLI

The primary user interface.

Must support:

```bash
duraflow run workflow.yaml
duraflow list
duraflow status <run_id>
duraflow events <run_id>
duraflow logs <run_id>
duraflow retry <run_id> --step <step_id>
duraflow cancel <run_id>
duraflow resume <run_id>
duraflow cron list
duraflow worker start
duraflow doctor
```

---

### 8.11 API Server

Optional in V1, stronger in V2.

Responsibilities:

- expose workflow operations over HTTP,
- allow external systems to start workflows,
- allow dashboards to inspect state,
- allow platform/server project to integrate with DuraFlow.

---

## 9. Workflow Lifecycle

### 9.1 Basic Lifecycle

```text
DEFINED
  ↓
RUN_CREATED
  ↓
READY
  ↓
RUNNING
  ↓
COMPLETED
```

Failure lifecycle:

```text
RUNNING
  ↓
FAILED
  ↓
COMPENSATING
  ↓
COMPENSATED / COMPENSATION_FAILED
```

Cancellation lifecycle:

```text
RUNNING
  ↓
CANCEL_REQUESTED
  ↓
CANCELLED
```

---

### 9.2 Step Lifecycle

```text
PENDING
  ↓
SCHEDULED
  ↓
LEASED
  ↓
RUNNING
  ↓
SUCCEEDED
```

Failure path:

```text
RUNNING
  ↓
FAILED_ATTEMPT
  ↓
RETRY_SCHEDULED
  ↓
SCHEDULED
```

Final failure:

```text
FAILED_ATTEMPT
  ↓
STEP_FAILED_FINAL
  ↓
WORKFLOW_FAILED
```

Timeout path:

```text
RUNNING
  ↓
TIMED_OUT
  ↓
RETRY_SCHEDULED / STEP_FAILED_FINAL
```

Lease expiry path:

```text
RUNNING
  ↓
LEASE_EXPIRED
  ↓
SCHEDULED
```

---

## 10. Event Model

### 10.1 Why Events Matter

DuraFlow should not only store latest state.

It should store **history**.

Current state can be rebuilt from events.

This gives:

- crash recovery,
- auditability,
- debugging,
- deterministic inspection,
- timeline view,
- future replay tools.

---

### 10.2 Core Event Types

#### Workflow Events

```text
WorkflowRegistered
WorkflowRunCreated
WorkflowStarted
WorkflowCompleted
WorkflowFailed
WorkflowCancelled
WorkflowRecovered
WorkflowCompensationStarted
WorkflowCompensationCompleted
WorkflowCompensationFailed
```

#### Step Events

```text
StepScheduled
StepLeaseAcquired
StepStarted
StepStdoutCaptured
StepStderrCaptured
StepSucceeded
StepAttemptFailed
StepRetryScheduled
StepTimedOut
StepLeaseExpired
StepFailedFinal
```

#### Worker Events

```text
WorkerRegistered
WorkerHeartbeatReceived
WorkerLeaseGranted
WorkerLeaseRenewed
WorkerLeaseExpired
WorkerMarkedDead
```

#### Timer/Cron Events

```text
TimerCreated
TimerFired
TimerCancelled
CronScheduleRegistered
CronWorkflowTriggered
```

#### Manual Operation Events

```text
ManualRetryRequested
ManualCancelRequested
ManualResumeRequested
ManualOverrideApplied
```

---

### 10.3 Event Record Schema

Conceptual event table:

```sql
CREATE TABLE events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  workflow_name TEXT NOT NULL,
  event_type TEXT NOT NULL,
  step_id TEXT,
  worker_id TEXT,
  attempt INTEGER,
  payload_json TEXT,
  created_at TEXT NOT NULL
);
```

---

## 11. State Reconstruction

DuraFlow reconstructs workflow state by reading events in order.

Example events:

```text
WorkflowRunCreated
StepScheduled(dump)
StepStarted(dump)
StepSucceeded(dump)
StepScheduled(compress)
StepStarted(compress)
StepAttemptFailed(compress)
StepRetryScheduled(compress)
```

Reconstructed state:

```text
workflow: RUNNING
dump: SUCCEEDED
compress: WAITING_FOR_RETRY
```

This is essential for recovery.

---

## 12. Storage Model

### 12.1 V1 SQLite Tables

Minimum tables:

```text
workflow_definitions
workflow_runs
events
step_states
workers
leases
timers
cron_schedules
logs
idempotency_keys
```

---

### 12.2 workflow_definitions

Stores registered workflow definitions.

Fields:

```text
name
version
definition_hash
definition_yaml
created_at
```

---

### 12.3 workflow_runs

Stores high-level run state for fast lookup.

Fields:

```text
run_id
workflow_name
workflow_version
status
created_at
started_at
completed_at
failed_at
current_attempt
metadata_json
```

---

### 12.4 step_states

Stores current state per step.

Fields:

```text
run_id
step_id
status
attempt
max_attempts
last_error
next_retry_at
started_at
completed_at
worker_id
```

---

### 12.5 workers

Stores worker metadata.

Fields:

```text
worker_id
hostname
pid
status
last_heartbeat_at
registered_at
```

---

### 12.6 leases

Stores task ownership.

Fields:

```text
lease_id
run_id
step_id
worker_id
expires_at
status
```

---

### 12.7 timers

Stores durable timers.

Fields:

```text
timer_id
run_id
step_id
fire_at
status
payload_json
```

---

### 12.8 logs

Stores captured command output metadata.

Fields:

```text
log_id
run_id
step_id
attempt
stream
content
created_at
```

For large logs, V1 can store logs in files and store paths in SQLite.

---

## 13. Execution Semantics

### 13.1 At-Least-Once

DuraFlow should clearly document:

> A step may run more than once during failure recovery.

This is unavoidable in many systems unless every external side effect is transactionally coordinated with event recording.

Therefore:

- steps should be idempotent,
- idempotency keys should be supported,
- duplicate execution should be visible,
- dangerous steps should have explicit protection.

---

### 13.2 Step Completion Boundary Problem

The hardest failure boundary:

```text
Step performs side effect successfully
Process dies before recording StepSucceeded
```

On recovery, DuraFlow may retry the step.

Mitigation:

- idempotency keys,
- external check commands,
- step output markers,
- compensating logic,
- manual confirmation mode for dangerous steps.

---

### 13.3 Idempotency Keys

Workflow steps may define idempotency keys:

```yaml
steps:
  - id: create-backup
    run: "python backup.py"
    idempotency_key: "backup:{workflow_run_id}:{step_id}"
```

DuraFlow stores idempotency records:

```text
key
run_id
step_id
status
result
created_at
```

---

### 13.4 External Side Effects

Examples:

- sending email,
- charging payment,
- applying migration,
- deleting files,
- uploading backup,
- changing service route.

DuraFlow must not pretend these are magically safe.

Dangerous steps should support:

```yaml
requires_approval: true
idempotency_key: "..."
compensation: "..."
```

---

## 14. Retry Model

### 14.1 Retry Policies

Supported policies:

```yaml
retry:
  max_attempts: 3
  backoff: exponential
  initial_delay_ms: 1000
  max_delay_ms: 30000
```

Other options:

```yaml
retry:
  max_attempts: 5
  backoff: fixed
  delay_ms: 5000
```

---

### 14.2 Retryable vs Non-Retryable Failure

V1 can start with exit-code based classification:

```yaml
retry:
  retry_on_exit_codes: [1, 2]
  no_retry_on_exit_codes: [10, 20]
```

Later:

- regex matching stderr,
- typed errors,
- plugin-based classifiers.

---

### 14.3 Backoff Calculation

Exponential backoff:

```text
delay = min(initial_delay * 2^(attempt - 1), max_delay)
```

Example:

```text
attempt 1 → 1s
attempt 2 → 2s
attempt 3 → 4s
attempt 4 → 8s
```

---

## 15. Timeout Model

Each step can define timeout:

```yaml
steps:
  - id: build
    run: "npm run build"
    timeout_ms: 120000
```

If timeout expires:

- kill process,
- record `StepTimedOut`,
- decide retry/final failure,
- capture partial logs.

---

## 16. Worker Model

### 16.1 Worker Registration

A worker starts:

```bash
duraflow worker start
```

It registers:

```text
worker_id
hostname
pid
started_at
capabilities
```

---

### 16.2 Heartbeats

Workers periodically send heartbeat events.

If heartbeat is missing beyond threshold:

```text
worker marked dead
leases expire
tasks become reclaimable
```

---

### 16.3 Worker Leases

Before running a step, a worker acquires a lease.

Lease means:

```text
worker owns this step until expires_at
```

If worker dies, lease expires and another worker may resume/retry.

---

### 16.4 Lease Renewal

Long-running steps require lease renewal.

If lease cannot be renewed, worker should stop or mark step uncertain.

---

## 17. Scheduler Model

Scheduler loop:

```text
1. find ready steps
2. find expired retries
3. find fired timers
4. find expired leases
5. assign eligible steps to workers
6. persist events before dispatch
```

Scheduler must avoid double-scheduling.

SQLite transactions are enough for V1.

---

## 18. Dependency Graph

Workflows may define dependencies:

```yaml
steps:
  - id: backup
    run: "python backup.py"

  - id: migrate
    run: "python migrate.py"
    depends_on: ["backup"]

  - id: healthcheck
    run: "curl http://localhost:8080/health"
    depends_on: ["migrate"]
```

A step is ready when all dependencies are successful.

---

## 19. Parallel Steps

V1 can support basic parallelism:

```yaml
steps:
  - id: test-unit
    run: "pytest tests/unit"

  - id: test-integration
    run: "pytest tests/integration"

  - id: package
    run: "python package.py"
    depends_on: ["test-unit", "test-integration"]
```

Parallelism must still respect:

- worker limits,
- lease acquisition,
- dependency checks.

---

## 20. Compensation Model

### 20.1 What Compensation Means

Compensation is not magic rollback.

It is a workflow-defined corrective action.

Example:

```text
If migration fails, restore backup.
If deploy fails, route traffic back to old version.
If file upload fails, delete partial file.
```

---

### 20.2 Compensation Definition

```yaml
steps:
  - id: backup
    run: "python backup.py"
    compensation:
      run: "rm -f backup.tmp"

  - id: migrate
    run: "python migrate.py"
    compensation:
      run: "python restore_backup.py"
```

---

### 20.3 Compensation Order

Usually reverse order of completed steps:

```text
step_1 completed
step_2 completed
step_3 failed

compensate step_2
compensate step_1
```

---

### 20.4 Compensation Failure

If compensation fails, final state:

```text
COMPENSATION_FAILED
```

DuraFlow must expose this clearly.

Do not hide it.

---

## 21. Timers and Cron

### 21.1 Durable Timers

Example:

```yaml
steps:
  - id: wait-for-cache
    wait:
      duration: "5m"
```

Timer is persisted.

If DuraFlow restarts, the timer still fires.

---

### 21.2 Cron Workflows

Example:

```yaml
schedule:
  cron: "0 2 * * *"
```

DuraFlow creates workflow runs at schedule time.

Need to decide overlap policy:

```yaml
schedule:
  cron: "0 2 * * *"
  overlap: "skip"
```

Overlap policies:

```text
allow
skip
queue
fail_existing
```

V1 can support:

```text
skip
allow
```

---

## 22. CLI Design

### 22.1 Core Commands

```bash
duraflow run workflow.yaml
duraflow list
duraflow status <run_id>
duraflow events <run_id>
duraflow logs <run_id>
duraflow cancel <run_id>
duraflow retry <run_id> --step <step_id>
duraflow resume <run_id>
duraflow worker start
duraflow doctor
```

---

### 22.2 Example CLI Output

```text
Run: backup-db/2026-05-08T10:22:31Z
Status: RUNNING

Steps:
  dump       SUCCEEDED   attempt=1 duration=2.3s
  compress   SUCCEEDED   attempt=1 duration=1.1s
  verify     RETRYING    attempt=2 next_retry=5s
  index      PENDING
```

---

### 22.3 Event Timeline Output

```text
[10:22:31] WorkflowRunCreated backup-db
[10:22:31] StepScheduled dump
[10:22:32] StepStarted dump worker=worker-1
[10:22:34] StepSucceeded dump
[10:22:34] StepScheduled compress
[10:22:35] StepSucceeded compress
[10:22:35] StepStarted verify
[10:22:36] StepAttemptFailed verify exit=1
[10:22:36] StepRetryScheduled verify next_retry=10:22:41
```

---

## 23. API Design

V2 can expose HTTP endpoints.

### 23.1 Workflow Endpoints

```text
POST   /workflows/register
GET    /workflows
GET    /workflows/{name}
POST   /workflows/{name}/runs
GET    /runs
GET    /runs/{run_id}
GET    /runs/{run_id}/events
GET    /runs/{run_id}/logs
POST   /runs/{run_id}/cancel
POST   /runs/{run_id}/resume
POST   /runs/{run_id}/retry
```

---

### 23.2 Worker Endpoints

```text
POST /workers/register
POST /workers/{worker_id}/heartbeat
POST /leases/acquire
POST /leases/{lease_id}/renew
POST /leases/{lease_id}/complete
POST /leases/{lease_id}/fail
```

---

## 24. Workflow Definition Format

### 24.1 Minimal Workflow

```yaml
name: hello
steps:
  - id: say-hello
    run: "echo hello"
```

---

### 24.2 Full Workflow Example

```yaml
name: deploy-service
version: 1

metadata:
  owner: "platform"
  description: "Build, deploy, health check, and rollback on failure."

env:
  SERVICE_NAME: "api"
  PORT: "8080"

steps:
  - id: build
    run: "npm run build"
    timeout_ms: 120000
    retry:
      max_attempts: 2
      backoff: fixed
      delay_ms: 5000

  - id: backup
    run: "python scripts/backup.py"
    depends_on: ["build"]
    idempotency_key: "backup:{run_id}"

  - id: migrate
    run: "python scripts/migrate.py"
    depends_on: ["backup"]
    retry:
      max_attempts: 1
    compensation:
      run: "python scripts/restore_backup.py"

  - id: start
    run: "python scripts/start_service.py"
    depends_on: ["migrate"]
    compensation:
      run: "python scripts/stop_service.py"

  - id: healthcheck
    run: "curl -f http://localhost:8080/health"
    depends_on: ["start"]
    retry:
      max_attempts: 5
      backoff: fixed
      delay_ms: 2000

on_failure:
  compensate: true
```

---

## 25. Configuration

Global config file:

```yaml
database:
  path: "~/.duraflow/duraflow.db"

worker:
  heartbeat_interval_ms: 5000
  lease_duration_ms: 30000
  max_concurrent_steps: 4

logs:
  mode: "file"
  directory: "~/.duraflow/logs"
  max_bytes_per_step: 1000000

scheduler:
  poll_interval_ms: 1000
```

---

## 26. Observability

### 26.1 Logs

Capture:

- stdout,
- stderr,
- exit code,
- duration,
- worker ID,
- command,
- environment metadata,
- attempt number.

---

### 26.2 Metrics

V2 metrics:

```text
workflow_runs_total
workflow_runs_failed_total
step_attempts_total
step_failures_total
step_retries_total
worker_heartbeats_total
lease_expirations_total
timer_fires_total
```

---

### 26.3 Traces / Timeline

DuraFlow should expose a human-readable timeline.

Future:

```bash
duraflow timeline <run_id>
```

---

## 27. Security Model

### 27.1 Local Execution Risk

DuraFlow runs commands.

This is dangerous.

V1 should clearly document:

> Do not run untrusted workflow definitions.

---

### 27.2 Command Allowlist

Optional policy:

```yaml
policy:
  allow_commands:
    - "python"
    - "npm"
    - "curl"
  block_patterns:
    - "rm -rf /"
    - "sudo"
    - "curl * | bash"
```

V1 can keep this simple.

---

### 27.3 Secrets

Secrets should not be logged.

Support masked env vars:

```yaml
secrets:
  - DATABASE_PASSWORD
```

DuraFlow should redact values in logs.

---

### 27.4 Future Isolation

Later, steps can run inside:

- Mini Docker,
- Docker,
- restricted subprocess,
- separate user account,
- container sandbox.

---

## 28. Failure Scenarios DuraFlow Must Handle

### 28.1 Engine Crash Before Step Starts

Expected behavior:

- event history shows step scheduled,
- no step started,
- step can be picked up after restart.

---

### 28.2 Engine Crash During Step

Expected behavior:

- worker heartbeat stops,
- lease expires,
- step becomes retryable,
- event history records recovery.

---

### 28.3 Step Succeeds but Completion Not Recorded

Expected behavior:

- DuraFlow may retry,
- user must rely on idempotency/verification,
- system documents duplicate risk.

---

### 28.4 Worker Dies

Expected behavior:

- worker marked dead,
- leases expire,
- task rescheduled.

---

### 28.5 Retry Exhausted

Expected behavior:

- step becomes final failed,
- workflow fails,
- compensation starts if configured.

---

### 28.6 Compensation Fails

Expected behavior:

- workflow ends in `COMPENSATION_FAILED`,
- manual intervention required.

---

### 28.7 Timer Created, Engine Restarts

Expected behavior:

- timer persists,
- timer fires after restart if due.

---

### 28.8 Cron Trigger Missed During Downtime

Decision needed.

Options:

```text
run missed schedule immediately
skip missed schedule
queue missed schedules
```

V1 should choose:

```text
skip missed schedule by default, optional catch_up=true later
```

---

## 29. Testing Strategy

### 29.1 Unit Tests

Test:

- workflow parser,
- dependency graph validation,
- retry policy calculation,
- state reconstruction from events,
- timer scheduling,
- compensation ordering.

---

### 29.2 Integration Tests

Test:

- run workflow successfully,
- step failure with retry,
- final failure,
- compensation success,
- cancellation,
- worker heartbeat,
- lease expiry,
- engine restart recovery.

---

### 29.3 Failure Injection Tests

Important tests:

```text
kill engine after StepScheduled
kill engine after StepStarted
kill worker during long-running step
expire lease manually
crash before StepSucceeded write
restart scheduler after timer creation
```

---

### 29.4 Property/Invariant Tests

Invariants:

- a completed workflow cannot later become running,
- a step cannot succeed before dependencies succeed,
- retry count cannot exceed max attempts,
- a lease cannot be owned by two active workers,
- compensation only runs after failure,
- event order must be monotonic per run.

---

## 30. Demo Requirements

A serious demo must show failure, not only happy path.

### 30.1 Demo 1: Backup Workflow with Crash Recovery

Steps:

```text
dump db
compress
verify
index
cleanup
```

During `verify`, kill DuraFlow.

Restart.

Expected:

```text
DuraFlow recovers and resumes/retries verify.
```

---

### 30.2 Demo 2: Retry with Backoff

Make a step fail twice, then succeed.

Show:

```text
attempt 1 failed
attempt 2 failed
attempt 3 succeeded
```

---

### 30.3 Demo 3: Compensation

Make deploy healthcheck fail.

Show:

```text
build succeeded
backup succeeded
migration succeeded
start failed healthcheck
compensation restored previous state
```

---

### 30.4 Demo 4: Worker Death

Start worker.

Run long step.

Kill worker.

Show lease expiry and task recovery.

---

## 31. Phased Development Plan

This section is critical. Do not build everything at once.

---

# Phase 0 — Design Freeze and Scope Control

## Goal

Define exactly what DuraFlow V1 is and is not.

## Deliverables

- project README draft,
- architecture doc,
- workflow YAML spec draft,
- event model doc,
- state machine doc,
- CLI command list,
- non-goals list.

## Cut From Phase 0

- dashboard,
- distributed cluster,
- Mini Docker integration,
- Mini Redis-Cassandra backend,
- advanced API server,
- AI integration.

## Exit Criteria

A senior engineer can read the docs and understand what V1 will build.

---

# Phase 1 — Minimal Local Workflow Runner

## Goal

Run a YAML-defined workflow locally with sequential steps.

## Features

- parse YAML workflow,
- validate step IDs,
- run steps in order,
- capture stdout/stderr,
- mark success/failure,
- basic CLI:

```bash
duraflow run workflow.yaml
```

## Storage

Can start with in-memory state for the first prototype, but must move to SQLite in Phase 2.

## Exit Criteria

A simple workflow runs:

```yaml
name: hello
steps:
  - id: one
    run: "echo one"
  - id: two
    run: "echo two"
```

---

# Phase 2 — SQLite Event Store

## Goal

Make workflow history persistent.

## Features

- SQLite schema,
- append events,
- reconstruct state from events,
- persist workflow runs,
- persist step states,
- CLI:

```bash
duraflow list
duraflow status <run_id>
duraflow events <run_id>
```

## Exit Criteria

If the CLI exits after workflow completion, the run history is still inspectable.

---

# Phase 3 — Retries, Timeouts, and Logs

## Goal

Make step failure handling real.

## Features

- retry policies,
- fixed/exponential backoff,
- timeout per step,
- attempt counter,
- log capture,
- final failure state,
- CLI logs command:

```bash
duraflow logs <run_id>
```

## Exit Criteria

A step can fail twice, retry with backoff, then succeed or fail finally.

---

# Phase 4 — Crash Recovery

## Goal

DuraFlow survives engine restart.

## Features

- resume incomplete runs,
- detect steps in uncertain state,
- reschedule eligible steps,
- record recovery events,
- startup recovery scan.

## Exit Criteria

Start a workflow, kill DuraFlow mid-run, restart, and see workflow resume or safely retry.

---

# Phase 5 — Workers, Heartbeats, and Leases

## Goal

Separate scheduler from worker execution.

## Features

- worker registration,
- heartbeat loop,
- lease acquisition,
- lease expiry,
- task reclaiming,
- worker CLI:

```bash
duraflow worker start
```

## Exit Criteria

Kill a worker while running a step. Another worker or restarted worker can reclaim after lease expiry.

---

# Phase 6 — Timers and Cron

## Goal

Support durable delayed execution and scheduled workflows.

## Features

- persistent timers,
- delayed retry scheduling,
- cron definitions,
- cron run creation,
- overlap policy: allow/skip,
- CLI:

```bash
duraflow cron list
```

## Exit Criteria

A timer survives process restart and fires correctly. A cron workflow creates scheduled runs.

---

# Phase 7 — Compensation and Manual Controls

## Goal

Support rollback-like behavior through explicit compensation.

## Features

- compensation definitions,
- reverse-order compensation,
- compensation events,
- manual retry,
- manual cancel,
- manual resume,
- CLI:

```bash
duraflow retry <run_id> --step <step_id>
duraflow cancel <run_id>
duraflow resume <run_id>
```

## Exit Criteria

A failed workflow can trigger compensation and show complete compensation history.

---

# Phase 8 — API Server

## Goal

Allow external systems to trigger and inspect workflows.

## Features

- REST API,
- run workflow,
- inspect run,
- inspect events,
- inspect logs,
- cancel/retry endpoints.

## Exit Criteria

A separate program can call DuraFlow to run a workflow and poll status.

---

# Phase 9 — Pluggable Executors

## Goal

Make step execution replaceable.

## Executors

```text
host subprocess executor
Mini Docker executor
Docker executor
```

## Features

- executor interface,
- executor selection per step,
- resource/time limits,
- executor-specific logs.

## Exit Criteria

A workflow can run one step on host and another inside an isolated executor.

---

# Phase 10 — Pluggable Storage Backends

## Goal

Allow alternative state/event stores.

## Backends

```text
SQLite
Postgres
Mini Redis-Cassandra later
```

## Features

- storage interface,
- event append,
- event query,
- state snapshot,
- migration strategy.

## Exit Criteria

SQLite remains default, but architecture allows backend replacement.

---

# Phase 11 — Integration with Stateful-first Server/PaaS

## Goal

Use DuraFlow as the internal operations engine for the Server/PaaS.

## Workflows

- deploy service,
- backup volume/database,
- restore backup,
- rollback deploy,
- run migration,
- rotate secrets,
- scheduled maintenance.

## Exit Criteria

Server/PaaS calls DuraFlow to execute at least one real deploy or backup workflow.

---

# Phase 12 — Distributed/Advanced DuraFlow

## Goal

Make DuraFlow multi-worker and more robust.

## Features

- coordination service integration,
- distributed leases,
- worker pools,
- remote workers,
- advanced failure detection,
- richer scheduler,
- future dashboard.

## Exit Criteria

Multiple workers coordinate tasks reliably with failure handling.

---

## 32. MVP Definition

The smallest version worth calling DuraFlow:

```text
YAML workflow
SQLite event history
CLI run/status/events/logs
step retries
timeouts
crash recovery
basic worker loop
failure demo
```

If these are missing, it is not DuraFlow yet.

---

## 33. Strong V1 Definition

A strong V1 should include:

```text
all MVP features
worker heartbeats
leases
timers
cron
compensation
manual retry/cancel
failure injection tests
good README
demo video
architecture docs
```

---

## 34. What to Cut Ruthlessly

Do not build these early:

- dashboard,
- plugin marketplace,
- cloud deployment,
- Kubernetes support,
- AI agent integration,
- complex language SDK,
- distributed storage backend,
- OAuth/user accounts,
- multi-tenant SaaS,
- fancy visualization,
- full Temporal-like determinism,
- web IDE.

These can destroy scope.

---

## 35. Technology Choices

### 35.1 Recommended Language

Best choices:

```text
Go
Rust
Python
```

Recommendation:

```text
Go if you want fast, practical infrastructure development.
Rust if you want stronger systems signal but slower development.
Python if you want speed and easier integration with your current projects.
```

For DuraFlow specifically:

- **Go** is best balanced for CLI, concurrency, process management, and infrastructure tooling.
- **Rust** is strongest for systems prestige.
- **Python** is easiest but may look less serious if performance/concurrency gets messy.

Recommended:

```text
Go for implementation.
SQLite for storage.
YAML for workflow definitions.
```

---

### 35.2 Libraries

Possible Go libraries:

```text
cobra        → CLI
sqlite driver → storage
yaml parser  → workflow config
cron parser  → schedules
```

Keep dependencies minimal.

---

## 36. Repo Structure

Example:

```text
duraflow/
  cmd/
    duraflow/
      main.go
  internal/
    engine/
    scheduler/
    worker/
    store/
    executor/
    retry/
    timer/
    compensation/
    workflow/
    logs/
  examples/
    hello/
    backup-db/
    deploy-service/
    cron-cleanup/
  docs/
    architecture.md
    event-model.md
    state-machines.md
    failure-model.md
    workflow-spec.md
  tests/
    integration/
    failure/
  README.md
  LICENSE
```

---

## 37. README Requirements

The README must explain:

- what DuraFlow is,
- why it exists,
- what problem it solves,
- quickstart,
- example workflow,
- crash recovery demo,
- retry demo,
- compensation demo,
- architecture diagram,
- event model,
- limitations,
- roadmap.

A weak README will make the project look like a toy.

---

## 38. Documentation Requirements

Docs:

```text
docs/architecture.md
docs/workflow-spec.md
docs/event-model.md
docs/failure-model.md
docs/retry-semantics.md
docs/worker-leases.md
docs/compensation.md
docs/storage.md
docs/examples.md
```

---

## 39. Benchmarking

DuraFlow does not need extreme performance in V1.

But it should measure:

- workflow startup latency,
- steps scheduled per second,
- event append latency,
- recovery time after crash,
- max concurrent local steps,
- SQLite database growth.

---

## 40. Limitations to Be Honest About

V1 limitations:

- local-first only,
- at-least-once execution,
- no exactly-once guarantee,
- no full distributed cluster,
- no strong sandboxing unless executor supports it,
- no enterprise UI,
- no multi-tenant auth,
- no arbitrary language SDK,
- no magic rollback for external side effects.

Being honest about limitations makes the project stronger.

---

## 41. Relationship to Future Projects

### 41.1 Relationship to Stateful-first Server/PaaS

DuraFlow can power:

- deploys,
- backups,
- restores,
- rollbacks,
- migrations,
- cron jobs,
- service maintenance.

The Server/PaaS becomes the product surface.  
DuraFlow becomes the internal reliable execution engine.

---

### 41.2 Relationship to FailForge

FailForge can test DuraFlow later.

Examples:

- kill worker mid-step,
- delay worker heartbeat,
- crash event store process,
- reorder simulated messages in distributed version,
- check workflow invariants.

FailForge is the failure testing lab.  
DuraFlow is a system worth testing.

---

### 41.3 Relationship to Mini Docker

Mini Docker can become an executor backend.

Example:

```yaml
steps:
  - id: build
    run: "npm run build"
    executor: "mini-docker"
```

---

### 41.4 Relationship to Coordination Service

Coordination service can become the distributed worker coordination layer.

Use cases:

- leader election,
- worker leases,
- heartbeats,
- failover.

---

### 41.5 Relationship to Mini Redis-Cassandra

Mini Redis-Cassandra can become an experimental distributed event/state backend later.

Do not use it in V1 unless it is reliable enough.

---

## 42. Interview Talking Points

If asked about DuraFlow, explain:

### 42.1 What Problem It Solves

> Scripts and queues fail badly under crashes. DuraFlow persists workflow history and makes multi-step backend operations recoverable.

### 42.2 Hardest Technical Problem

> The hardest problem is failure boundaries, especially when a step succeeds externally but the engine crashes before recording success.

### 42.3 Execution Guarantee

> DuraFlow provides at-least-once execution and requires idempotency for side-effecting steps.

### 42.4 Why Event History

> Event history allows crash recovery, auditability, debugging, and state reconstruction.

### 42.5 Why Worker Leases

> Leases prevent abandoned tasks from being stuck forever after worker death.

### 42.6 Why Compensation

> Not every operation can be rolled back transactionally, so the workflow must define explicit compensating actions.

---

## 43. Resume Bullet Options

### Short Bullet

> Built DuraFlow, a local-first durable workflow engine with persistent event history, step-level retries, worker heartbeats, crash recovery, timers, idempotency keys, compensation handlers, and CLI-based workflow inspection.

### Stronger Bullet

> Built DuraFlow, a local-first durable execution engine for backend operations, implementing SQLite-backed event history, step-level retry/backoff, worker leases/heartbeats, crash recovery, durable timers, compensation workflows, and CLI inspection for deploy/backup/restore pipelines.

### Advanced Bullet

> Designed DuraFlow, a durable workflow engine for self-hosted backend platforms, with at-least-once execution semantics, idempotency support, event-sourced recovery, worker lease failover, cron/timer scheduling, compensation handlers, and pluggable executors for isolated step execution.

---

## 44. Success Criteria

DuraFlow is successful if:

- it can run real backup/deploy workflows,
- it survives process crash,
- it can recover unfinished workflows,
- retry behavior is explicit and testable,
- worker death does not permanently lose tasks,
- event history explains what happened,
- compensation is visible and reliable,
- documentation is clear enough for senior engineers,
- demos show real failure behavior.

---

## 45. What Would Make It Lame

DuraFlow becomes lame if it is only:

```text
queue + retry + cron
```

It becomes serious only if it has:

```text
persistent event history
crash recovery
worker leases
idempotency model
timers
compensation
inspection
failure tests
```

---

## 46. Final Product Statement

**DuraFlow is a local-first durable workflow engine for backend operations. It lets small teams and self-hosted builders run deploys, backups, restores, migrations, cron jobs, and maintenance workflows with persistent history, retries, crash recovery, worker failover, timers, compensation, and CLI-based inspection.**

It exists because scripts and queues are not enough when operations are important.

It is the reliability layer under a future self-hosted backend platform.

---

## 47. Final North-Star Placement

```text
Stateful-first Server/PaaS → product surface
DuraFlow                  → durable operations engine
FailForge                 → failure/correctness testing lab
```

DuraFlow is the balanced systems project:

- more practical than FailForge,
- deeper than a basic queue,
- less crowded than generic PaaS,
- directly useful for real backend operations,
- strong enough to stand alone,
- powerful enough to become the engine under the server platform.

---

## 48. Absolute Build Rule

Do not build DuraFlow as a fancy name around a simple task queue.

Build it as a **durable execution engine**.

If a workflow crashes halfway and DuraFlow cannot explain, recover, retry, or compensate, then the project has failed.

If DuraFlow can show exactly what happened, resume safely, and make failure inspectable, then it becomes a serious systems flagship.
