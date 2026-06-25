package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/yumekaz/duraflow/pkg/engine"
	"github.com/yumekaz/duraflow/pkg/store"
	"github.com/yumekaz/duraflow/pkg/worker"
	"github.com/yumekaz/duraflow/pkg/workflow"
)

type Server struct {
	store      store.EventStore
	engine     *engine.WorkflowEngine
	addr       string
	runWorker  bool
	worker     *worker.WorkerDaemon
	httpServer *http.Server
}

func NewServer(s store.EventStore, eng *engine.WorkflowEngine, addr string, runWorker bool) *Server {
	return &Server{
		store:     s,
		engine:    eng,
		addr:      addr,
		runWorker: runWorker,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/workflows/run", s.handleRunWorkflow)
	mux.HandleFunc("GET /api/v1/runs", s.handleListRuns)
	mux.HandleFunc("GET /api/v1/runs/{run_id}", s.handleGetRun)
	mux.HandleFunc("GET /api/v1/runs/{run_id}/events", s.handleGetEvents)
	mux.HandleFunc("GET /api/v1/runs/{run_id}/logs", s.handleGetLogs)
	mux.HandleFunc("POST /api/v1/runs/{run_id}/cancel", s.handleCancelRun)
	mux.HandleFunc("POST /api/v1/runs/{run_id}/retry", s.handleRetryRun)
	mux.HandleFunc("POST /api/v1/runs/{run_id}/resume", s.handleResumeRun)
	return mux
}

func (s *Server) Start() error {
	if s.runWorker {
		s.worker = worker.NewWorkerDaemon(s.store, s.engine)
		if err := s.worker.Start(); err != nil {
			return fmt.Errorf("failed to start worker daemon: %w", err)
		}
	}

	s.httpServer = &http.Server{
		Addr:    s.addr,
		Handler: s.Handler(),
	}

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(ctx)
	}

	if s.worker != nil {
		s.worker.Stop()
	}
}

func (s *Server) handleRunWorkflow(w http.ResponseWriter, r *http.Request) {
	var yamlBytes []byte
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var req struct {
			YAML string `json:"yaml"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}
		yamlBytes = []byte(req.YAML)
	} else {
		var err error
		yamlBytes, err = io.ReadAll(r.Body)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "Failed to read request body")
			return
		}
	}

	if len(yamlBytes) == 0 {
		s.writeError(w, http.StatusBadRequest, "Workflow definition is empty")
		return
	}

	def, hash, orderedSteps, err := workflow.ParseAndValidate(yamlBytes)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid workflow definition: %v", err))
		return
	}

	if def.Schedule != nil {
		if err := s.store.RegisterWorkflow(def, hash, string(yamlBytes)); err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to register workflow: %v", err))
			return
		}

		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		sched, err := parser.Parse(def.Schedule.Cron)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid cron expression %q: %v", def.Schedule.Cron, err))
			return
		}
		nextRunTime := sched.Next(time.Now().UTC()).Format(time.RFC3339Nano)

		err = s.store.UpsertCronSchedule(&store.CronSchedule{
			WorkflowName:   def.Name,
			CronExpression: def.Schedule.Cron,
			OverlapPolicy:  def.Schedule.Overlap,
			NextRunTime:    nextRunTime,
			DefinitionYAML: string(yamlBytes),
			Status:         "ACTIVE",
		})
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to register cron schedule: %v", err))
			return
		}

		_ = s.store.AppendEvent(&store.Event{
			RunID:        "",
			WorkflowName: def.Name,
			EventType:    engine.EventCronScheduleReg,
			PayloadJSON:  fmt.Sprintf(`{"cron_expression":%q,"overlap_policy":%q}`, def.Schedule.Cron, def.Schedule.Overlap),
		})

		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"workflow_name":  def.Name,
			"cron":           def.Schedule.Cron,
			"overlap_policy": def.Schedule.Overlap,
			"next_run_time":  nextRunTime,
			"status":         "CRON_REGISTERED",
		})
		return
	}

	runID, err := s.engine.RunWorkflow(r.Context(), def, orderedSteps, hash, string(yamlBytes))
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to run workflow: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"run_id":        runID,
		"workflow_name": def.Name,
		"status":        engine.StatusRunning,
	})
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil {
			if parsedLimit > 0 && parsedLimit <= 100 {
				limit = parsedLimit
			}
		}
	}

	runs, err := s.store.ListRuns(limit)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to list runs: %v", err))
		return
	}

	if runs == nil {
		runs = []*store.WorkflowRun{}
	}
	s.writeJSON(w, http.StatusOK, runs)
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	run, err := s.store.GetRun(runID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch run: %v", err))
		return
	}
	if run == nil {
		s.writeError(w, http.StatusNotFound, "Workflow run not found")
		return
	}

	states, err := s.store.GetStepStates(runID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch step states: %v", err))
		return
	}

	if def, err := s.store.GetWorkflowDef(run.WorkflowName, run.WorkflowVersion); err == nil {
		stepOrderMap := make(map[string]int)
		for i, step := range def.Steps {
			stepOrderMap[step.ID] = i
		}
		sort.Slice(states, func(i, j int) bool {
			orderI, okI := stepOrderMap[states[i].StepID]
			orderJ, okJ := stepOrderMap[states[j].StepID]
			if okI && okJ {
				return orderI < orderJ
			}
			return false
		})
	}

	if states == nil {
		states = []*store.StepState{}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"run":   run,
		"steps": states,
	})
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	events, err := s.store.GetEvents(runID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch events: %v", err))
		return
	}

	if events == nil {
		events = []*store.Event{}
	}
	s.writeJSON(w, http.StatusOK, events)
}

func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	stepID := r.URL.Query().Get("step_id")

	var logs []*store.LogEntry
	if stepID != "" {
		entries, err := s.store.GetLogs(runID, stepID)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch logs: %v", err))
			return
		}
		logs = entries
	} else {
		states, err := s.store.GetStepStates(runID)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch step states: %v", err))
			return
		}
		for _, state := range states {
			entries, err := s.store.GetLogs(runID, state.StepID)
			if err == nil {
				logs = append(logs, entries...)
			}
		}
	}

	if logs == nil {
		logs = []*store.LogEntry{}
	}
	s.writeJSON(w, http.StatusOK, logs)
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	if err := s.store.CancelWorkflowRun(runID); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to cancel run: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"message": "Workflow run cancelled successfully",
		"run_id":  runID,
	})
}

func (s *Server) handleRetryRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")

	var body struct {
		StepID string `json:"step_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	run, err := s.store.GetRun(runID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch run: %v", err))
		return
	}
	if run == nil {
		s.writeError(w, http.StatusNotFound, "Workflow run not found")
		return
	}

	if run.Status != engine.StatusFailed && run.Status != engine.StatusCompensationFailed && run.Status != engine.StatusCancelled {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Workflow run %s is in status %s; can only retry runs in status FAILED, COMPENSATION_FAILED, or CANCELLED", runID, run.Status))
		return
	}

	stepID := body.StepID
	if stepID == "" {
		states, err := s.store.GetStepStates(runID)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch step states: %v", err))
			return
		}
		var failedSteps []string
		for _, st := range states {
			if st.Status == engine.StepFailedFinal || st.Status == engine.StepCompensationFailed || (run.Status == engine.StatusCancelled && st.Status == engine.StepRunning) {
				failedSteps = append(failedSteps, st.StepID)
			}
		}
		if len(failedSteps) == 0 {
			s.writeError(w, http.StatusBadRequest, "Could not find any failed or cancelled step to retry")
			return
		}
		if len(failedSteps) > 1 {
			s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Multiple failed/cancelled steps found (%s); please specify which step to retry using step_id in body", strings.Join(failedSteps, ", ")))
			return
		}
		stepID = failedSteps[0]
	}

	states, err := s.store.GetStepStates(runID)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to fetch step states: %v", err))
		return
	}
	var targetState *store.StepState
	for _, st := range states {
		if st.StepID == stepID {
			targetState = st
			break
		}
	}
	if targetState == nil {
		s.writeError(w, http.StatusNotFound, fmt.Sprintf("Step %s not found in workflow run", stepID))
		return
	}

	if targetState.Status == engine.StepCompensationFailed {
		if err := s.store.ResetStepState(runID, stepID); err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to reset step: %v", err))
			return
		}
		targetState.Status = engine.StepSucceeded
		targetState.LastError = ""
		targetState.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := s.store.UpsertStepState(targetState); err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to update step state: %v", err))
			return
		}
		if err := s.store.ResetWorkflowRunForRetry(runID, engine.StatusCompensating); err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to reset run status: %v", err))
			return
		}
		_ = s.store.AppendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: run.WorkflowName,
			EventType:    engine.EventWorkflowResumed,
			StepID:       stepID,
			PayloadJSON:  `{"message":"Manual retry of step compensation triggered via API"}`,
		})
		s.writeJSON(w, http.StatusOK, map[string]string{
			"message": "Retry of step compensation triggered",
			"run_id":  runID,
			"step_id": stepID,
		})
	} else {
		if err := s.store.ResetStepState(runID, stepID); err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to reset step: %v", err))
			return
		}
		if err := s.store.ResetWorkflowRunForRetry(runID, engine.StatusRunning); err != nil {
			s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to reset run status: %v", err))
			return
		}
		_ = s.store.AppendEvent(&store.Event{
			RunID:        runID,
			WorkflowName: run.WorkflowName,
			EventType:    engine.EventWorkflowResumed,
			StepID:       stepID,
			PayloadJSON:  `{"message":"Manual retry of step triggered via API"}`,
		})
		s.writeJSON(w, http.StatusOK, map[string]string{
			"message": "Retry of step triggered",
			"run_id":  runID,
			"step_id": stepID,
		})
	}
}

func (s *Server) handleResumeRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("run_id")
	if err := s.engine.ResumeWorkflow(r.Context(), runID); err != nil {
		s.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to resume run: %v", err))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"message": "Workflow run resumed successfully",
		"run_id":  runID,
	})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}
