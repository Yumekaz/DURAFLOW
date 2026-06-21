package engine

import (
	"time"

	"github.com/yumekaz/duraflow/internal/workflow"
)

// CalculateBackoff computes the delay duration before the next retry attempt.
func CalculateBackoff(policy *workflow.RetryPolicy, attempt int) time.Duration {
	if policy == nil {
		return 0
	}

	if policy.Backoff == "exponential" {
		initial := policy.InitialDelayMs
		if initial <= 0 {
			return 0
		}

		maxDelayMs := policy.MaxDelayMs
		if maxDelayMs <= 0 {
			maxDelayMs = 60000 // default cap of 60 seconds
		}

		shift := attempt - 1
		if shift < 0 {
			shift = 0
		}
		if shift > 30 {
			shift = 30 // Cap shift to prevent int64 overflow when computing 1 << shift
		}

		factor := int64(1) << shift
		delayMs := initial * factor

		// Check for potential integer overflow
		if delayMs < 0 || delayMs/factor != initial {
			delayMs = maxDelayMs
		}

		if delayMs > maxDelayMs {
			delayMs = maxDelayMs
		}

		return time.Duration(delayMs) * time.Millisecond
	}

	// Default: "fixed" backoff
	delayMs := policy.DelayMs
	if delayMs <= 0 {
		delayMs = policy.InitialDelayMs
	}
	if delayMs <= 0 {
		return 0
	}
	return time.Duration(delayMs) * time.Millisecond
}
