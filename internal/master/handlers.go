package master

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
)

type ErrorResponse struct {
	Error string `json:"error"`
}

func sendJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}

func sendError(w http.ResponseWriter, status int, msg string) {
	sendJSON(w, status, ErrorResponse{Error: msg})
}

func sendWorkerUpdateError(w http.ResponseWriter, err error) {
	if errors.Is(err, db.ErrInvalidVideoState) {
		sendError(w, http.StatusBadRequest, "invalid job state")
		return
	}
	if errors.Is(err, db.ErrLeaseNotOwned) {
		sendError(w, http.StatusConflict, "job lease is no longer owned by this worker")
		return
	}
	sendError(w, http.StatusInternalServerError, "failed to update job")
}

type CreateSourceReq struct {
	URL  string `json:"url"`
	Kind string `json:"kind"`
}

type CreateSourceResp struct {
	ID int64 `json:"id"`
}

func (s *Server) handleCreateSource(w http.ResponseWriter, r *http.Request) {
	var req CreateSourceReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	id, err := s.db.CreateSource(r.Context(), req.URL, req.Kind)
	if err != nil {
		s.logger.Error("failed to create source", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to create source")
		return
	}
	sendJSON(w, http.StatusCreated, CreateSourceResp{ID: id})
}

func (s *Server) handleListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := s.db.ListSources(r.Context())
	if err != nil {
		s.logger.Error("failed to list sources", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to list sources")
		return
	}
	if sources == nil {
		sources = make([]db.Source, 0)
	}
	sendJSON(w, http.StatusOK, sources)
}

func (s *Server) handleGetSource(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		sendError(w, http.StatusBadRequest, "invalid id")
		return
	}
	source, err := s.db.GetSource(r.Context(), id)
	if err != nil {
		sendError(w, http.StatusNotFound, "source not found")
		return
	}
	sendJSON(w, http.StatusOK, source)
}

func (s *Server) handleListVideos(w http.ResponseWriter, r *http.Request) {
	var state *string
	if st := r.URL.Query().Get("state"); st != "" {
		state = &st
	}
	videos, err := s.db.ListVideos(r.Context(), state)
	if err != nil {
		s.logger.Error("failed to list videos", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to list videos")
		return
	}
	sendJSON(w, http.StatusOK, videos)
}

func (s *Server) handleGetVideo(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		sendError(w, http.StatusBadRequest, "invalid id")
		return
	}
	video, err := s.db.GetVideo(r.Context(), id)
	if err != nil {
		sendError(w, http.StatusNotFound, "video not found")
		return
	}
	sendJSON(w, http.StatusOK, video)
}

type RegisterWorkerReq struct {
	ID string `json:"id"`
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req RegisterWorkerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.db.RegisterWorker(r.Context(), req.ID); err != nil {
		s.logger.Error("failed to register worker", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to register worker")
		return
	}
	sendJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

type HeartbeatReq struct {
	ID string `json:"id"`
}

func (s *Server) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.db.Heartbeat(r.Context(), req.ID); err != nil {
		s.logger.Error("failed to record heartbeat", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to record heartbeat")
		return
	}
	sendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.db.ListWorkers(r.Context())
	if err != nil {
		s.logger.Error("failed to list workers", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to list workers")
		return
	}
	sendJSON(w, http.StatusOK, workers)
}

type ClaimJobReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration_sec"`
}

func (s *Server) handleClaimJob(w http.ResponseWriter, r *http.Request) {
	var req ClaimJobReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dur := time.Duration(req.LeaseDuration) * time.Second
	if dur == 0 {
		dur = 5 * time.Minute
	}
	// A claim is also an opportunity to recover work if the periodic recovery
	// loop has not run yet (for example, immediately after a master restart).
	// Failure is surfaced instead of treating a database outage as an empty
	// queue.
	if _, err := s.db.RecoverExpiredLeases(r.Context()); err != nil {
		s.logger.Error("failed to recover expired job leases before claim", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to recover expired job leases")
		return
	}
	video, err := s.db.ClaimJob(r.Context(), req.WorkerID, dur)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			sendError(w, http.StatusNotFound, "no pending jobs")
			return
		}
		s.logger.Error("failed to claim job", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to claim job")
		return
	}
	sendJSON(w, http.StatusOK, video)
}

type UpdateJobStateReq struct {
	WorkerID string  `json:"worker_id"`
	State    string  `json:"state"`
	Error    *string `json:"error,omitempty"`
}

func (s *Server) handleUpdateJobState(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		sendError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req UpdateJobStateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.db.UpdateVideoState(r.Context(), id, req.WorkerID, req.State, req.Error); err != nil {
		s.logger.Error("failed to update job state", "error", err)
		sendWorkerUpdateError(w, err)
		return
	}
	sendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type CompleteJobReq struct {
	WorkerID    string `json:"worker_id"`
	StoragePath string `json:"storage_path"`
}

func (s *Server) handleCompleteJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		sendError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req CompleteJobReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.db.CompleteVideo(r.Context(), id, req.WorkerID, req.StoragePath); err != nil {
		s.logger.Error("failed to complete job", "error", err)
		sendWorkerUpdateError(w, err)
		return
	}
	sendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type RenewJobReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration_sec"`
}

func (s *Server) handleRenewJob(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		sendError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req RenewJobReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dur := time.Duration(req.LeaseDuration) * time.Second
	if dur == 0 {
		dur = 5 * time.Minute
	}
	if err := s.db.RenewLease(r.Context(), id, req.WorkerID, dur); err != nil {
		s.logger.Error("failed to renew job", "error", err)
		sendWorkerUpdateError(w, err)
		return
	}
	sendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.db.CountByState(r.Context())
	if err != nil {
		s.logger.Error("failed to get stats", "error", err)
		sendError(w, http.StatusInternalServerError, "failed to get stats")
		return
	}
	sendJSON(w, http.StatusOK, counts)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
