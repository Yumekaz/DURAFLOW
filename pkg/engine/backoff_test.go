package engine

import (
	"testing"
	"time"

	"github.com/yumekaz/duraflow/pkg/workflow"
)

func TestCalculateBackoff_Fixed(t *testing.T) {
	policy := &workflow.RetryPolicy{
		Backoff: "fixed",
		DelayMs: 500,
	}

	for attempt := 1; attempt <= 5; attempt++ {
		delay := CalculateBackoff(policy, attempt)
		expected := 500 * time.Millisecond
		if delay != expected {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected, delay)
		}
	}
}

func TestCalculateBackoff_FixedFallback(t *testing.T) {
	policy := &workflow.RetryPolicy{
		Backoff:        "fixed",
		InitialDelayMs: 300, // delay_ms is not set, fallback to InitialDelayMs
	}

	delay := CalculateBackoff(policy, 1)
	expected := 300 * time.Millisecond
	if delay != expected {
		t.Errorf("expected fallback to initial delay %v, got %v", expected, delay)
	}
}

func TestCalculateBackoff_Exponential(t *testing.T) {
	policy := &workflow.RetryPolicy{
		Backoff:        "exponential",
		InitialDelayMs: 100,
		MaxDelayMs:     1000,
	}

	expectedDelays := []time.Duration{
		100 * time.Millisecond,  // attempt 1: 100 * 2^0
		200 * time.Millisecond,  // attempt 2: 100 * 2^1
		400 * time.Millisecond,  // attempt 3: 100 * 2^2
		800 * time.Millisecond,  // attempt 4: 100 * 2^3
		1000 * time.Millisecond, // attempt 5: capped at 1000 (instead of 1600)
		1000 * time.Millisecond, // attempt 6: capped at 1000
	}

	for i, expected := range expectedDelays {
		attempt := i + 1
		delay := CalculateBackoff(policy, attempt)
		if delay != expected {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected, delay)
		}
	}
}

func TestCalculateBackoff_ExponentialDefaultCap(t *testing.T) {
	policy := &workflow.RetryPolicy{
		Backoff:        "exponential",
		InitialDelayMs: 10000, // 10s
		MaxDelayMs:     0,     // should default to 60s
	}

	expectedDelays := []time.Duration{
		10 * time.Second, // attempt 1
		20 * time.Second, // attempt 2
		40 * time.Second, // attempt 3
		60 * time.Second, // attempt 4: capped at 60s
	}

	for i, expected := range expectedDelays {
		attempt := i + 1
		delay := CalculateBackoff(policy, attempt)
		if delay != expected {
			t.Errorf("attempt %d: expected delay %v, got %v", attempt, expected, delay)
		}
	}
}

func TestCalculateBackoff_ZeroOrNil(t *testing.T) {
	if CalculateBackoff(nil, 1) != 0 {
		t.Error("expected 0 delay for nil policy")
	}

	policy := &workflow.RetryPolicy{}
	if CalculateBackoff(policy, 1) != 0 {
		t.Error("expected 0 delay for empty policy")
	}
}
