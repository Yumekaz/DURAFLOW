package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/yumekaz/duraflow/internal/engine"
	"github.com/yumekaz/duraflow/internal/executor"
	"github.com/yumekaz/duraflow/internal/store"
	"github.com/yumekaz/duraflow/internal/workflow"
)

var (
	dbPathFlag string
	version    = "0.1.0"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "duraflow",
		Short: "DuraFlow — Local-First Durable Workflow Engine",
	}

	rootCmd.PersistentFlags().StringVar(&dbPathFlag, "db", "~/.duraflow/duraflow.db", "SQLite database file path")

	rootCmd.AddCommand(runCmd())
	rootCmd.AddCommand(listCmd())
	rootCmd.AddCommand(statusCmd())
	rootCmd.AddCommand(eventsCmd())
	rootCmd.AddCommand(logsCmd())
	rootCmd.AddCommand(resumeCmd())
	rootCmd.AddCommand(versionCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getStore() (store.EventStore, error) {
	resolvedPath := resolveDBPath(dbPathFlag)
	s := store.NewSQLiteStore(resolvedPath)
	if err := s.Init(); err != nil {
		return nil, err
	}
	return s, nil
}

func resolveDBPath(path string) string {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err == nil {
			// Handle both "~" and "~/..."
			if path == "~" {
				return home
			}
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

func runCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run <workflow.yaml>",
		Short: "Parse and execute a workflow YAML file locally",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath := args[0]
			data, err := os.ReadFile(filePath)
			if err != nil {
				return fmt.Errorf("failed to read workflow file: %w", err)
			}

			def, hash, orderedSteps, err := workflow.ParseAndValidate(data)
			if err != nil {
				return fmt.Errorf("invalid workflow definition: %w", err)
			}

			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			exec := executor.NewHostExecutor()
			eng := engine.NewWorkflowEngine(s, exec)

			var attemptStart time.Time

			eng.OnEvent = func(ev *store.Event) {
				switch ev.EventType {
				case engine.EventStepStarted:
					maxAttempts := 1
					for _, st := range orderedSteps {
						if st.ID == ev.StepID {
							if st.Retry != nil && st.Retry.MaxAttempts > 0 {
								maxAttempts = st.Retry.MaxAttempts
							}
							break
						}
					}
					attemptStart = time.Now()
					fmt.Printf("  [%s]    attempt %d/%d ... ", ev.StepID, ev.Attempt, maxAttempts)
				
				case engine.EventStepSucceeded:
					dur := time.Since(attemptStart).Round(time.Millisecond)
					fmt.Printf("SUCCEEDED (%v)\n", dur)
				
				case engine.EventStepRetryScheduled:
					delayMs := int64(0)
					if ev.PayloadJSON != "" {
						var p struct {
							DelayMs int64 `json:"delay_ms"`
						}
						_ = json.Unmarshal([]byte(ev.PayloadJSON), &p)
						delayMs = p.DelayMs
					}
					delayStr := fmt.Sprintf("%v", time.Duration(delayMs)*time.Millisecond)
					fmt.Printf("FAILED [retry in %s]\n", delayStr)
				
				case engine.EventStepFailedFinal:
					errStr := "failed"
					if ev.PayloadJSON != "" {
						var p struct {
							Error string `json:"error"`
						}
						_ = json.Unmarshal([]byte(ev.PayloadJSON), &p)
						if p.Error != "" {
							errStr = p.Error
						}
					}
					fmt.Printf("FAILED (%s)\n", errStr)
				}
			}

			fmt.Printf("Starting workflow %q...\n", def.Name)
			runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, string(data))
			if err != nil {
				return fmt.Errorf("workflow execution failed: %w", err)
			}

			run, err := s.GetRun(runID)
			if err != nil {
				return fmt.Errorf("failed to fetch run details: %w", err)
			}

			fmt.Println()
			fmt.Printf("Run ID:  %s\n", run.RunID)
			fmt.Printf("Status:  %s\n", run.Status)
			
			states, err := s.GetStepStates(runID)
			if err == nil {
				fmt.Println("\nSteps:")
				formatStepStates(states, orderedSteps)
			}

			if run.Status == engine.StatusFailed {
				os.Exit(1)
			}
			return nil
		},
	}
}

func listCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List recent workflow runs",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			runs, err := s.ListRuns(50)
			if err != nil {
				return err
			}

			if len(runs) == 0 {
				fmt.Println("No workflow runs found.")
				return nil
			}

			fmt.Printf("%-38s %-20s %-7s %-12s %-25s\n", "RUN ID", "WORKFLOW", "VERSION", "STATUS", "CREATED AT")
			fmt.Println(strings.Repeat("-", 107))
			for _, r := range runs {
				createdAtStr := r.CreatedAt
				if t, err := time.Parse(time.RFC3339Nano, r.CreatedAt); err == nil {
					createdAtStr = t.Local().Format("2006-01-02 15:04:05")
				}
				fmt.Printf("%-38s %-20s %-7d %-12s %-25s\n", r.RunID, r.WorkflowName, r.WorkflowVersion, r.Status, createdAtStr)
			}
			return nil
		},
	}
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <run_id>",
		Short: "Show detailed status of a workflow run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runID := args[0]
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			run, err := s.GetRun(runID)
			if err != nil {
				return err
			}

			fmt.Printf("Run ID:           %s\n", run.RunID)
			fmt.Printf("Workflow:         %s (v%d)\n", run.WorkflowName, run.WorkflowVersion)
			fmt.Printf("Status:           %s\n", run.Status)
			fmt.Printf("Created At:       %s\n", formatTime(run.CreatedAt))
			if run.StartedAt != "" {
				fmt.Printf("Started At:       %s\n", formatTime(run.StartedAt))
			}
			if run.CompletedAt != "" {
				fmt.Printf("Completed At:     %s\n", formatTime(run.CompletedAt))
			}
			if run.FailedAt != "" {
				fmt.Printf("Failed At:        %s\n", formatTime(run.FailedAt))
			}

			// Get the workflow definition to find original step order
			var orderedSteps []workflow.StepDef
			if def, err := s.GetWorkflowDef(run.WorkflowName, run.WorkflowVersion); err == nil {
				orderedSteps = def.Steps
			}

			states, err := s.GetStepStates(runID)
			if err != nil {
				return err
			}

			fmt.Println("\nSteps:")
			formatStepStates(states, orderedSteps)

			return nil
		},
	}
}

func eventsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "events <run_id>",
		Short: "Show the event timeline for a run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runID := args[0]
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			events, err := s.GetEvents(runID)
			if err != nil {
				return err
			}

			if len(events) == 0 {
				fmt.Println("No events found for this run.")
				return nil
			}

			for _, ev := range events {
				timestampStr := ev.CreatedAt
				if t, err := time.Parse(time.RFC3339Nano, ev.CreatedAt); err == nil {
					timestampStr = t.Local().Format("15:04:05.000")
				}
				stepDetails := ""
				if ev.StepID != "" {
					stepDetails = fmt.Sprintf(" step=%s", ev.StepID)
					if ev.Attempt > 0 {
						stepDetails += fmt.Sprintf(" attempt=%d", ev.Attempt)
					}
				}
				payloadDetails := ""
				if ev.PayloadJSON != "" && ev.PayloadJSON != "{}" {
					payloadDetails = " " + ev.PayloadJSON
				}
				fmt.Printf("[%s] %s%s%s\n", timestampStr, ev.EventType, stepDetails, payloadDetails)
			}
			return nil
		},
	}
}

func logsCmd() *cobra.Command {
	var stepIDFlag string
	c := &cobra.Command{
		Use:   "logs <run_id>",
		Short: "Show captured logs for a run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runID := args[0]
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			states, err := s.GetStepStates(runID)
			if err != nil {
				return err
			}

			foundLogs := false
			for _, state := range states {
				if stepIDFlag != "" && state.StepID != stepIDFlag {
					continue
				}

				entries, err := s.GetLogs(runID, state.StepID)
				if err != nil || len(entries) == 0 {
					continue
				}

				foundLogs = true
				fmt.Printf("--- LOGS FOR STEP %q ---\n", state.StepID)

				// Group logs by attempt
				byAttempt := make(map[int][]*store.LogEntry)
				var attempts []int
				for _, entry := range entries {
					if _, ok := byAttempt[entry.Attempt]; !ok {
						attempts = append(attempts, entry.Attempt)
					}
					byAttempt[entry.Attempt] = append(byAttempt[entry.Attempt], entry)
				}

				// Sort attempts in ascending order
				for i := 0; i < len(attempts); i++ {
					for j := i + 1; j < len(attempts); j++ {
						if attempts[i] > attempts[j] {
							attempts[i], attempts[j] = attempts[j], attempts[i]
						}
					}
				}

				for _, attempt := range attempts {
					fmt.Printf("  Attempt %d:\n", attempt)
					for _, entry := range byAttempt[attempt] {
						prefix := fmt.Sprintf("[%s]", entry.Stream)
						lines := strings.Split(strings.TrimSuffix(entry.Content, "\n"), "\n")
						for _, line := range lines {
							fmt.Printf("    %-8s %s\n", prefix, line)
						}
					}
				}
				fmt.Println()
			}

			if !foundLogs {
				fmt.Println("No logs found.")
			}
			return nil
		},
	}
	c.Flags().StringVar(&stepIDFlag, "step", "", "Filter logs by step ID")
	return c
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print DuraFlow version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("DuraFlow version %s\n", version)
		},
	}
}

func formatTime(rfc3339 string) string {
	if rfc3339 == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339Nano, rfc3339)
	if err != nil {
		return rfc3339
	}
	return t.Local().Format("2006-01-02 15:04:05.000 MST")
}

func formatStepStates(states []*store.StepState, orderedSteps []workflow.StepDef) {
	// Create mapping for sorting
	stepOrderMap := make(map[string]int)
	if len(orderedSteps) > 0 {
		for i, s := range orderedSteps {
			stepOrderMap[s.ID] = i
		}
	} else {
		// fallback to started_at / completed_at sort or index
		for i, s := range states {
			stepOrderMap[s.StepID] = i
		}
	}

	// Sort states by topological order
	for i := 0; i < len(states); i++ {
		for j := i + 1; j < len(states); j++ {
			orderI := stepOrderMap[states[i].StepID]
			orderJ := stepOrderMap[states[j].StepID]
			if orderI > orderJ {
				states[i], states[j] = states[j], states[i]
			}
		}
	}

	fmt.Printf("  %-20s %-18s %-10s %-25s %s\n", "STEP ID", "STATUS", "ATTEMPT", "COMPLETED AT", "ERROR")
	fmt.Println("  " + strings.Repeat("-", 95))
	for _, s := range states {
		completedAtStr := "-"
		if s.CompletedAt != "" {
			if t, err := time.Parse(time.RFC3339Nano, s.CompletedAt); err == nil {
				completedAtStr = t.Local().Format("15:04:05")
			}
		}
		errStr := "-"
		if s.LastError != "" {
			errStr = s.LastError
		}
		attemptStr := fmt.Sprintf("%d/%d", s.Attempt, s.MaxAttempts)
		fmt.Printf("  %-20s %-18s %-10s %-25s %s\n", s.StepID, s.Status, attemptStr, completedAtStr, errStr)
	}
}

func resumeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resume [run_id]",
		Short: "Resume crashed/interrupted workflow runs",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			var runsToResume []string
			if len(args) == 1 {
				runsToResume = append(runsToResume, args[0])
			} else {
				incomplete, err := s.GetIncompleteRuns()
				if err != nil {
					return fmt.Errorf("failed to fetch incomplete runs: %w", err)
				}
				if len(incomplete) == 0 {
					fmt.Println("No incomplete workflow runs found.")
					return nil
				}
				fmt.Printf("Found %d incomplete run(s) to recover.\n", len(incomplete))
				for _, r := range incomplete {
					runsToResume = append(runsToResume, r.RunID)
				}
			}

			exec := executor.NewHostExecutor()
			eng := engine.NewWorkflowEngine(s, exec)

			for idx, runID := range runsToResume {
				run, err := s.GetRun(runID)
				if err != nil {
					return fmt.Errorf("failed to fetch run details for %s: %w", runID, err)
				}
				if run == nil {
					return fmt.Errorf("workflow run not found: %s", runID)
				}

				yamlContent, err := s.GetWorkflowYAML(run.WorkflowName, run.WorkflowVersion)
				if err != nil {
					return fmt.Errorf("failed to fetch definition for %s: %w", runID, err)
				}

				def, _, orderedSteps, err := workflow.ParseAndValidate([]byte(yamlContent))
				if err != nil {
					return fmt.Errorf("failed to parse definition for %s: %w", runID, err)
				}

				fmt.Printf("\n[%d/%d] Resuming workflow run %s (%s)...\n", idx+1, len(runsToResume), runID, def.Name)

				// Print already completed steps
				states, err := s.GetStepStates(runID)
				if err == nil {
					for _, step := range orderedSteps {
						for _, st := range states {
							if st.StepID == step.ID && st.Status == engine.StepSucceeded {
								fmt.Printf("  [%s]    already SUCCEEDED (skipped)\n", step.ID)
							}
						}
					}
				}

				resumedSteps := make(map[string]bool)
				var attemptStart time.Time

				eng.OnEvent = func(ev *store.Event) {
					if ev.RunID != runID {
						return
					}
					switch ev.EventType {
					case engine.EventStepResumed:
						resumedSteps[ev.StepID] = true
						fmt.Printf("  [%s]    resuming ", ev.StepID)
					
					case engine.EventStepStarted:
						maxAttempts := 1
						for _, st := range orderedSteps {
							if st.ID == ev.StepID {
								if st.Retry != nil && st.Retry.MaxAttempts > 0 {
									maxAttempts = st.Retry.MaxAttempts
								}
								break
							}
						}
						attemptStart = time.Now()
						if resumedSteps[ev.StepID] {
							fmt.Printf("attempt %d/%d ... ", ev.Attempt, maxAttempts)
							resumedSteps[ev.StepID] = false
						} else {
							fmt.Printf("  [%s]    attempt %d/%d ... ", ev.StepID, ev.Attempt, maxAttempts)
						}

					case engine.EventStepSucceeded:
						dur := time.Since(attemptStart).Round(time.Millisecond)
						fmt.Printf("SUCCEEDED (%v)\n", dur)

					case engine.EventStepRetryScheduled:
						delayMs := int64(0)
						if ev.PayloadJSON != "" {
							var p struct {
								DelayMs int64 `json:"delay_ms"`
							}
							_ = json.Unmarshal([]byte(ev.PayloadJSON), &p)
							delayMs = p.DelayMs
						}
						delayStr := fmt.Sprintf("%v", time.Duration(delayMs)*time.Millisecond)
						fmt.Printf("FAILED [retry in %s]\n", delayStr)

					case engine.EventStepFailedFinal:
						errStr := "failed"
						if ev.PayloadJSON != "" {
							var p struct {
								Error string `json:"error"`
							}
							_ = json.Unmarshal([]byte(ev.PayloadJSON), &p)
							if p.Error != "" {
								errStr = p.Error
							}
						}
						fmt.Printf("FAILED (%s)\n", errStr)
					}
				}

				err = eng.ResumeWorkflow(context.Background(), runID)
				if err != nil {
					fmt.Printf("Error resuming run %s: %v\n", runID, err)
					continue
				}

				// Fetch updated details
				updatedRun, err := s.GetRun(runID)
				if err == nil {
					fmt.Printf("\nStatus:  %s\n", updatedRun.Status)
					updatedStates, err := s.GetStepStates(runID)
					if err == nil {
						fmt.Println("\nSteps:")
						formatStepStates(updatedStates, orderedSteps)
					}
				}
			}

			return nil
		},
	}
}

