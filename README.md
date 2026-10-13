# DuraFlow — Local-First Durable Workflow Engine

DuraFlow is a local-first durable workflow engine that runs multi-step backend operations reliably across crashes, retries, worker failures, timers, idempotency issues, and rollback/compensation needs.

## Verification

Run `go test ./...` for the unprivileged suite. Real Mini-Docker execution uses dedicated BusyBox rootfs fixtures and requires root, or an explicitly capable rootless host with `DURAFLOW_TEST_MINIDOCKER=1`. A prerequisite skip is not runtime proof. PostgreSQL tests require `DURAFLOW_TEST_POSTGRES_DSN`; the 2026-10-08 Cairn acceptance gate ran them against a private real PostgreSQL container, including concurrent initial lease acquisition, and separately passed the three privileged Mini-Docker executor tests.

The October 14 verification reproduced and repaired stale-scan replay of succeeded steps and duplicate in-flight execution by the same worker after a stale heartbeat. SQLite and PostgreSQL lease claims now preserve terminal steps except for deliberate compensation; workers reserve execution before acquiring a lease. All five real PostgreSQL tests passed locally. CI additionally requires the worker/store race detector. This does not turn external side effects into globally exactly-once operations: commands still need replay-safe design across process loss.

## One-Line Definition

**DuraFlow is a local-first durable workflow engine that runs multi-step backend operations reliably across crashes, retries, worker failures, timers, idempotency issues, and rollback/compensation needs.**

## Architecture

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

## Quick Start (Phase 1)

### 1. Build the Binary
```bash
go build -o duraflow ./cmd/duraflow/
```

### 2. Run an Example Workflow
```bash
./duraflow run examples/hello.yaml
```

### 3. List Past Runs
```bash
./duraflow list
```

### 4. Inspect Run Details
```bash
./duraflow status <run_id>
./duraflow events <run_id>
./duraflow logs <run_id>
```

## Example Workflow Definition (`examples/hello.yaml`)

```yaml
name: hello
version: 1
steps:
  - id: greet
    run: "echo 'Hello from DuraFlow!'"
  - id: timestamp
    run: "date -u '+%Y-%m-%dT%H:%M:%SZ'"
    depends_on: ["greet"]
```
