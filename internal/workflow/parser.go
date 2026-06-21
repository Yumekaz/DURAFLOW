package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"gopkg.in/yaml.v3"
)

// ParseAndValidate parses a workflow YAML byte array, validates it, and returns the definition, its SHA256 hash, and a topologically sorted list of steps.
func ParseAndValidate(data []byte) (*WorkflowDef, string, []StepDef, error) {
	var def WorkflowDef
	if err := yaml.Unmarshal(data, &def); err != nil {
		return nil, "", nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	// 1. Calculate hash
	hasher := sha256.New()
	hasher.Write(data)
	hash := hex.EncodeToString(hasher.Sum(nil))

	// 2. Validate basic structure
	if def.Name == "" {
		return nil, "", nil, fmt.Errorf("workflow name is required")
	}
	if len(def.Steps) == 0 {
		return nil, "", nil, fmt.Errorf("workflow must have at least one step")
	}

	// 3. Validate step uniqueness and non-emptiness
	stepMap := make(map[string]StepDef)
	for i, step := range def.Steps {
		if step.ID == "" {
			return nil, "", nil, fmt.Errorf("step ID cannot be empty")
		}
		if _, exists := stepMap[step.ID]; exists {
			return nil, "", nil, fmt.Errorf("duplicate step ID: %s", step.ID)
		}
		// Normalize retry policies
		if step.Retry == nil {
			step.Retry = &RetryPolicy{
				MaxAttempts: 1,
			}
		} else {
			if step.Retry.MaxAttempts <= 0 {
				step.Retry.MaxAttempts = 1
			}
			if step.Retry.Backoff == "" {
				step.Retry.Backoff = "fixed"
			}
		}
		def.Steps[i] = step
		stepMap[step.ID] = step
	}

	// 4. Validate dependency existence
	for _, step := range def.Steps {
		for _, dep := range step.DependsOn {
			if _, exists := stepMap[dep]; !exists {
				return nil, "", nil, fmt.Errorf("step %q depends on non-existent step %q", step.ID, dep)
			}
		}
	}

	// 5. Topological Sort (Kahn's Algorithm) to validate cycle-free and determine order
	inDegree := make(map[string]int)
	adjList := make(map[string][]string)

	for _, step := range def.Steps {
		inDegree[step.ID] = len(step.DependsOn)
		for _, dep := range step.DependsOn {
			adjList[dep] = append(adjList[dep], step.ID)
		}
	}

	var queue []string
	// Collect initial in-degree 0 steps in definition order to maintain stability
	for _, step := range def.Steps {
		if inDegree[step.ID] == 0 {
			queue = append(queue, step.ID)
		}
	}

	var sortedIDs []string
	for len(queue) > 0 {
		// pop
		u := queue[0]
		queue = queue[1:]
		sortedIDs = append(sortedIDs, u)

		for _, v := range adjList[u] {
			inDegree[v]--
			if inDegree[v] == 0 {
				queue = append(queue, v)
			}
		}
	}

	if len(sortedIDs) < len(def.Steps) {
		return nil, "", nil, fmt.Errorf("dependency cycle detected in workflow steps")
	}

	// Construct ordered list of steps
	orderedSteps := make([]StepDef, len(sortedIDs))
	for i, id := range sortedIDs {
		orderedSteps[i] = stepMap[id]
	}

	return &def, hash, orderedSteps, nil
}
