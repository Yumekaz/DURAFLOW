package workflow

import (
	"testing"
)

func TestParseAndValidate_Valid(t *testing.T) {
	yamlContent := []byte(`
name: backup-db
version: 1
env:
  GLOBAL_VAR: "value"
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
`)

	def, hash, orderedSteps, err := ParseAndValidate(yamlContent)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if def.Name != "backup-db" {
		t.Errorf("expected name 'backup-db', got %q", def.Name)
	}

	if len(hash) != 64 {
		t.Errorf("expected 64-char hex hash, got %d chars", len(hash))
	}

	if len(orderedSteps) != 3 {
		t.Fatalf("expected 3 ordered steps, got %d", len(orderedSteps))
	}

	if orderedSteps[0].ID != "dump" || orderedSteps[1].ID != "compress" || orderedSteps[2].ID != "verify" {
		t.Errorf("unexpected topological order: %v, %v, %v", orderedSteps[0].ID, orderedSteps[1].ID, orderedSteps[2].ID)
	}

	if orderedSteps[0].Retry.MaxAttempts != 3 || orderedSteps[0].Retry.Backoff != "exponential" {
		t.Errorf("retry policy not parsed correctly: %+v", orderedSteps[0].Retry)
	}

	if orderedSteps[1].Retry.MaxAttempts != 1 {
		t.Errorf("expected default max attempts 1, got %d", orderedSteps[1].Retry.MaxAttempts)
	}
}

func TestParseAndValidate_Cycle(t *testing.T) {
	yamlContent := []byte(`
name: cycle-test
steps:
  - id: A
    run: "echo A"
    depends_on: ["C"]
  - id: B
    run: "echo B"
    depends_on: ["A"]
  - id: C
    run: "echo C"
    depends_on: ["B"]
`)

	_, _, _, err := ParseAndValidate(yamlContent)
	if err == nil {
		t.Fatalf("expected error due to cycle, but got nil")
	}
}

func TestParseAndValidate_MissingDependency(t *testing.T) {
	yamlContent := []byte(`
name: missing-dep
steps:
  - id: A
    run: "echo A"
    depends_on: ["B"]
`)

	_, _, _, err := ParseAndValidate(yamlContent)
	if err == nil {
		t.Fatalf("expected error due to missing dependency, but got nil")
	}
}

func TestParseAndValidate_DuplicateIDs(t *testing.T) {
	yamlContent := []byte(`
name: duplicate-ids
steps:
  - id: A
    run: "echo A1"
  - id: A
    run: "echo A2"
`)

	_, _, _, err := ParseAndValidate(yamlContent)
	if err == nil {
		t.Fatalf("expected error due to duplicate IDs, but got nil")
	}
}

func TestParseAndValidate_EmptyName(t *testing.T) {
	yamlContent := []byte(`
steps:
  - id: A
    run: "echo A"
`)

	_, _, _, err := ParseAndValidate(yamlContent)
	if err == nil {
		t.Fatalf("expected error due to empty name, but got nil")
	}
}
