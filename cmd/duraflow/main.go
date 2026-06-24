package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"github.com/yumekaz/duraflow/internal/api"
	"github.com/yumekaz/duraflow/internal/engine"
	"github.com/yumekaz/duraflow/internal/executor"
	"github.com/yumekaz/duraflow/internal/store"
	"github.com/yumekaz/duraflow/internal/worker"
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
	rootCmd.AddCommand(workerCmd())
	rootCmd.AddCommand(cronCmd())
	rootCmd.AddCommand(cancelCmd())
	rootCmd.AddCommand(retryCmd())
	rootCmd.AddCommand(serverCmd())
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

func getExecutor() executor.Executor {
	reg := executor.NewRegistry()
	reg.Register("host", executor.NewHostExecutor())
	reg.Register("docker", executor.NewDockerExecutor())
	reg.Register("mini-docker", executor.NewMiniDockerExecutor())
	return reg
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

			exec := getExecutor()
			eng := engine.NewWorkflowEngine(s, exec)

			if def.Schedule != nil {
				if err := s.RegisterWorkflow(def, hash, string(data)); err != nil {
					return fmt.Errorf("failed to register workflow definition: %w", err)
				}

				parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
				sched, err := parser.Parse(def.Schedule.Cron)
				if err != nil {
					return fmt.Errorf("invalid cron expression %q: %w", def.Schedule.Cron, err)
				}
				nextRunTime := sched.Next(time.Now().UTC()).Format(time.RFC3339Nano)

				err = s.UpsertCronSchedule(&store.CronSchedule{
					WorkflowName:   def.Name,
					CronExpression: def.Schedule.Cron,
					OverlapPolicy:  def.Schedule.Overlap,
					NextRunTime:    nextRunTime,
					DefinitionYAML: string(data),
					Status:         "ACTIVE",
				})
				if err != nil {
					return fmt.Errorf("failed to register cron schedule: %w", err)
				}

				_ = s.AppendEvent(&store.Event{
					RunID:        "",
					WorkflowName: def.Name,
					EventType:    "CronScheduleRegistered",
					PayloadJSON:  fmt.Sprintf(`{"cron_expression":%q,"overlap_policy":%q}`, def.Schedule.Cron, def.Schedule.Overlap),
				})

				fmt.Printf("Cron schedule registered for workflow %q.\n", def.Name)
				fmt.Printf("  Expression:  %s\n", def.Schedule.Cron)
				fmt.Printf("  Overlap:     %s\n", def.Schedule.Overlap)
				fmt.Printf("  Next Run:    %s\n", formatTime(nextRunTime))
				return nil
			}

			fmt.Printf("Starting workflow %q...\n", def.Name)
			runID, err := eng.RunWorkflow(context.Background(), def, orderedSteps, hash, string(data))
			if err != nil {
				return fmt.Errorf("workflow execution failed: %w", err)
			}

			lastID := int64(0)
			completed := false
			failed := false
			stepStartTimes := make(map[string]time.Time)

			for {
				events, err := s.GetEvents(runID)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
					continue
				}

				for _, ev := range events {
					if ev.ID <= lastID {
						continue
					}
					lastID = ev.ID

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
						tStr := ev.CreatedAt
						tVal, err := time.Parse(time.RFC3339Nano, tStr)
						if err == nil {
							stepStartTimes[ev.StepID] = tVal
						} else {
							stepStartTimes[ev.StepID] = time.Now()
						}
						fmt.Printf("  [%s]    attempt %d/%d ... ", ev.StepID, ev.Attempt, maxAttempts)

					case engine.EventStepSucceeded:
						tStr := ev.CreatedAt
						tVal, err := time.Parse(time.RFC3339Nano, tStr)
						dur := time.Duration(0)
						if err == nil && !stepStartTimes[ev.StepID].IsZero() {
							dur = tVal.Sub(stepStartTimes[ev.StepID]).Round(time.Millisecond)
						}
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

					case engine.EventWorkflowCompleted:
						completed = true

					case engine.EventWorkflowFailed:
						failed = true

					case engine.EventWorkflowCancelled:
						fmt.Printf("Workflow cancelled by operator.\n")
						failed = true

					case engine.EventWorkflowCompensationStarted:
						fmt.Printf("Workflow failed; triggering compensation rollback...\n")

					case engine.EventWorkflowCompensationCompleted:
						fmt.Printf("Workflow compensated successfully.\n")
						completed = true

					case engine.EventWorkflowCompensationFailed:
						fmt.Printf("Workflow compensation failed!\n")
						failed = true

					case engine.EventStepCompensating:
						tStr := ev.CreatedAt
						tVal, err := time.Parse(time.RFC3339Nano, tStr)
						if err == nil {
							stepStartTimes[ev.StepID] = tVal
						} else {
							stepStartTimes[ev.StepID] = time.Now()
						}
						fmt.Printf("  [%s]    compensating ... ", ev.StepID)

					case engine.EventStepCompensated:
						tStr := ev.CreatedAt
						tVal, err := time.Parse(time.RFC3339Nano, tStr)
						dur := time.Duration(0)
						if err == nil && !stepStartTimes[ev.StepID].IsZero() {
							dur = tVal.Sub(stepStartTimes[ev.StepID]).Round(time.Millisecond)
						}
						fmt.Printf("COMPENSATED (%v)\n", dur)

					case engine.EventStepCompensationFailed:
						tStr := ev.CreatedAt
						tVal, err := time.Parse(time.RFC3339Nano, tStr)
						dur := time.Duration(0)
						if err == nil && !stepStartTimes[ev.StepID].IsZero() {
							dur = tVal.Sub(stepStartTimes[ev.StepID]).Round(time.Millisecond)
						}
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
						fmt.Printf("COMPENSATION_FAILED (%s) (%v)\n", errStr, dur)
					}
				}

				if completed || failed {
					break
				}

				time.Sleep(100 * time.Millisecond)
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

			exec := getExecutor()
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

				// Find max event ID to start tailing from
				existingEvents, err := s.GetEvents(runID)
				lastID := int64(0)
				if err == nil && len(existingEvents) > 0 {
					lastID = existingEvents[len(existingEvents)-1].ID
				}

				err = eng.ResumeWorkflow(context.Background(), runID)
				if err != nil {
					fmt.Printf("Error resuming run %s: %v\n", runID, err)
					continue
				}

				resumedSteps := make(map[string]bool)
				stepStartTimes := make(map[string]time.Time)
				completed := false
				failed := false

				for {
					events, err := s.GetEvents(runID)
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					for _, ev := range events {
						if ev.ID <= lastID {
							continue
						}
						lastID = ev.ID

						if ev.RunID != runID {
							continue
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
							tStr := ev.CreatedAt
							tVal, err := time.Parse(time.RFC3339Nano, tStr)
							if err == nil {
								stepStartTimes[ev.StepID] = tVal
							} else {
								stepStartTimes[ev.StepID] = time.Now()
							}
							if resumedSteps[ev.StepID] {
								fmt.Printf("attempt %d/%d ... ", ev.Attempt, maxAttempts)
								resumedSteps[ev.StepID] = false
							} else {
								fmt.Printf("  [%s]    attempt %d/%d ... ", ev.StepID, ev.Attempt, maxAttempts)
							}

						case engine.EventStepSucceeded:
							tStr := ev.CreatedAt
							tVal, err := time.Parse(time.RFC3339Nano, tStr)
							dur := time.Duration(0)
							if err == nil && !stepStartTimes[ev.StepID].IsZero() {
								dur = tVal.Sub(stepStartTimes[ev.StepID]).Round(time.Millisecond)
							}
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

						case engine.EventWorkflowCompleted:
							completed = true

						case engine.EventWorkflowFailed:
							failed = true

						case engine.EventWorkflowCancelled:
							fmt.Printf("Workflow cancelled by operator.\n")
							failed = true

						case engine.EventWorkflowCompensationStarted:
							fmt.Printf("Workflow failed; triggering compensation rollback...\n")

						case engine.EventWorkflowCompensationCompleted:
							fmt.Printf("Workflow compensated successfully.\n")
							completed = true

						case engine.EventWorkflowCompensationFailed:
							fmt.Printf("Workflow compensation failed!\n")
							failed = true

						case engine.EventStepCompensating:
							tStr := ev.CreatedAt
							tVal, err := time.Parse(time.RFC3339Nano, tStr)
							if err == nil {
								stepStartTimes[ev.StepID] = tVal
							} else {
								stepStartTimes[ev.StepID] = time.Now()
							}
							fmt.Printf("  [%s]    compensating ... ", ev.StepID)

						case engine.EventStepCompensated:
							tStr := ev.CreatedAt
							tVal, err := time.Parse(time.RFC3339Nano, tStr)
							dur := time.Duration(0)
							if err == nil && !stepStartTimes[ev.StepID].IsZero() {
								dur = tVal.Sub(stepStartTimes[ev.StepID]).Round(time.Millisecond)
							}
							fmt.Printf("COMPENSATED (%v)\n", dur)

						case engine.EventStepCompensationFailed:
							tStr := ev.CreatedAt
							tVal, err := time.Parse(time.RFC3339Nano, tStr)
							dur := time.Duration(0)
							if err == nil && !stepStartTimes[ev.StepID].IsZero() {
								dur = tVal.Sub(stepStartTimes[ev.StepID]).Round(time.Millisecond)
							}
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
							fmt.Printf("COMPENSATION_FAILED (%s) (%v)\n", errStr, dur)
						}
					}

					if completed || failed {
						break
					}

					time.Sleep(100 * time.Millisecond)
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

func workerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker",
		Short: "Manage background execution workers",
	}
	cmd.AddCommand(workerStartCmd())
	return cmd
}

func workerStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start a background execution worker daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			exec := getExecutor()
			eng := engine.NewWorkflowEngine(s, exec)

			w := worker.NewWorkerDaemon(s, eng)
			if err := w.Start(); err != nil {
				return fmt.Errorf("failed to start worker: %w", err)
			}

			hostname, _ := os.Hostname()
			fmt.Printf("Worker daemon started successfully.\n")
			fmt.Printf("  Worker ID: %s\n", w.WorkerID())
			fmt.Printf("  Hostname:  %s\n", hostname)
			fmt.Printf("  PID:       %d\n", os.Getpid())
			fmt.Println("Press Ctrl+C to stop.")

			// Wait for interrupt signal to gracefully exit
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			<-sigChan

			fmt.Println("\nShutting down worker daemon gracefully...")
			w.Stop()
			fmt.Println("Worker daemon stopped.")
			return nil
		},
	}
}

func cronCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cron",
		Short: "Manage registered cron workflow schedules",
	}
	cmd.AddCommand(cronListCmd())
	return cmd
}

func cronListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all active cron schedules",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			schedules, err := s.ListCronSchedules()
			if err != nil {
				return err
			}

			if len(schedules) == 0 {
				fmt.Println("No active cron schedules found.")
				return nil
			}

			fmt.Printf("%-20s %-15s %-10s %-38s %-25s %-25s\n", "WORKFLOW", "CRON", "OVERLAP", "LAST RUN ID", "LAST RUN TIME", "NEXT RUN TIME")
			fmt.Println(strings.Repeat("-", 138))
			for _, cs := range schedules {
				lastRunID := cs.LastRunID
				if lastRunID == "" {
					lastRunID = "-"
				}
				lastRunTime := "-"
				if cs.LastRunTime != "" {
					lastRunTime = formatTime(cs.LastRunTime)
				}
				nextRunTime := formatTime(cs.NextRunTime)

				fmt.Printf("%-20s %-15s %-10s %-38s %-25s %-25s\n", cs.WorkflowName, cs.CronExpression, cs.OverlapPolicy, lastRunID, lastRunTime, nextRunTime)
			}
			return nil
		},
	}
}

func cancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel [run_id]",
		Short: "Cancel an active workflow run",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			runID := args[0]
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			if err := s.CancelWorkflowRun(runID); err != nil {
				return err
			}

			fmt.Printf("Workflow run %s successfully cancelled.\n", runID)
			return nil
		},
	}
}

func retryCmd() *cobra.Command {
	var stepIDFlag string
	cmd := &cobra.Command{
		Use:   "retry [run_id]",
		Short: "Retry a failed step in a failed workflow run",
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
				return fmt.Errorf("failed to fetch run details: %w", err)
			}
			if run == nil {
				return fmt.Errorf("workflow run not found: %s", runID)
			}

			if run.Status != engine.StatusFailed && run.Status != engine.StatusCompensationFailed && run.Status != engine.StatusCancelled {
				return fmt.Errorf("workflow run %s is in status %s; can only retry runs in status FAILED, COMPENSATION_FAILED, or CANCELLED", runID, run.Status)
			}

			stepID := stepIDFlag
			if stepID == "" {
				// Search for a failed or cancelled step in step states
				states, err := s.GetStepStates(runID)
				if err != nil {
					return fmt.Errorf("failed to fetch step states: %w", err)
				}
				var failedSteps []string
				for _, st := range states {
					if st.Status == engine.StepFailedFinal || st.Status == engine.StepCompensationFailed || (run.Status == engine.StatusCancelled && st.Status == engine.StepRunning) {
						failedSteps = append(failedSteps, st.StepID)
					}
				}
				if len(failedSteps) == 0 {
					return fmt.Errorf("could not find any failed or cancelled step to retry in run %s", runID)
				}
				if len(failedSteps) > 1 {
					return fmt.Errorf("multiple failed/cancelled steps found (%s); please specify which step to retry using --step flag", strings.Join(failedSteps, ", "))
				}
				stepID = failedSteps[0]
			}

			// Determine if it was a compensation step failure or regular step failure
			states, err := s.GetStepStates(runID)
			if err != nil {
				return fmt.Errorf("failed to fetch step states: %w", err)
			}
			var targetState *store.StepState
			for _, st := range states {
				if st.StepID == stepID {
					targetState = st
					break
				}
			}
			if targetState == nil {
				return fmt.Errorf("step %s not found in workflow run %s", stepID, runID)
			}

			if targetState.Status == engine.StepCompensationFailed {
				// Retry compensation: reset to SUCCEEDED and set run back to COMPENSATING
				if err := s.ResetStepState(runID, stepID); err != nil {
					return err
				}
				// Wait! ResetStepState sets status to PENDING. We need status to be SUCCEEDED so it's a compensation candidate!
				// Let's manually transition it to SUCCEEDED!
				targetState.Status = engine.StepSucceeded
				targetState.LastError = ""
				targetState.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
				if err := s.UpsertStepState(targetState); err != nil {
					return fmt.Errorf("failed to restore step status to SUCCEEDED: %w", err)
				}

				if err := s.ResetWorkflowRunForRetry(runID, engine.StatusCompensating); err != nil {
					return err
				}

				_ = s.AppendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: run.WorkflowName,
					EventType:    engine.EventWorkflowResumed,
					StepID:       stepID,
					PayloadJSON:  fmt.Sprintf(`{"message":%q}`, "Manual retry of step compensation triggered"),
				})

				fmt.Printf("Triggered manual retry for compensation of step %s in run %s. Run status reset to COMPENSATING.\n", stepID, runID)
			} else {
				// Regular step retry: reset status to PENDING
				if err := s.ResetStepState(runID, stepID); err != nil {
					return err
				}

				if err := s.ResetWorkflowRunForRetry(runID, engine.StatusRunning); err != nil {
					return err
				}

				_ = s.AppendEvent(&store.Event{
					RunID:        runID,
					WorkflowName: run.WorkflowName,
					EventType:    engine.EventWorkflowResumed,
					StepID:       stepID,
					PayloadJSON:  fmt.Sprintf(`{"message":%q}`, "Manual retry of step triggered"),
				})

				fmt.Printf("Triggered manual retry for step %s in run %s. Run status reset to RUNNING.\n", stepID, runID)
			}

			return nil
		},
	}
	cmd.Flags().StringVar(&stepIDFlag, "step", "", "The step ID to retry (optional if there is only one failed step)")
	return cmd
}

func serverCmd() *cobra.Command {
	var port int
	var addr string
	var runWorker bool

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the DuraFlow REST API HTTP server",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := getStore()
			if err != nil {
				return err
			}
			defer s.Close()

			exec := getExecutor()
			eng := engine.NewWorkflowEngine(s, exec)

			bindAddr := addr
			if bindAddr == "" {
				bindAddr = fmt.Sprintf("127.0.0.1:%d", port)
			} else if !strings.Contains(bindAddr, ":") {
				bindAddr = fmt.Sprintf("%s:%d", bindAddr, port)
			}

			srv := api.NewServer(s, eng, bindAddr, runWorker)
			if err := srv.Start(); err != nil {
				return err
			}

			fmt.Printf("DuraFlow API Server started successfully.\n")
			fmt.Printf("  Listen Address: http://%s\n", bindAddr)
			if runWorker {
				fmt.Printf("  Worker daemon running in-process.\n")
			} else {
				fmt.Printf("  Worker daemon disabled. Run 'duraflow worker start' separately to process workflows.\n")
			}
			fmt.Printf("  Database:       %s\n", dbPathFlag)
			fmt.Println("Press Ctrl+C to stop.")

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			<-sigChan

			fmt.Println("\nShutting down API Server gracefully...")
			srv.Stop()
			fmt.Println("API Server stopped.")
			return nil
		},
	}

	cmd.Flags().IntVar(&port, "port", 8080, "Port to listen on")
	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1", "IP address or network interface to bind to")
	cmd.Flags().BoolVar(&runWorker, "worker", false, "Run a background execution worker daemon in-process")

	return cmd
}

