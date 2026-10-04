package master

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
)

func retryAt(attempt int) time.Time { // bounded exponential backoff with deterministic jitter
	if attempt < 1 {
		attempt = 1
	}
	d := time.Minute * time.Duration(1<<minInt(attempt-1, 6))
	jitter := time.Duration((attempt*7919)%30000) * time.Millisecond
	return time.Now().UTC().Add(d + jitter)
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func sendJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data != nil {
		_ = json.NewEncoder(w).Encode(data)
	}
}
func sendError(w http.ResponseWriter, status int, msg string) {
	sendJSON(w, status, ErrorResponse{msg})
}
func workerError(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, control.ErrLeaseNotOwned):
		sendError(w, http.StatusConflict, "job lease is no longer owned by this worker")
	case errors.Is(e, control.ErrInvalidState), errors.Is(e, control.ErrCapacity):
		sendError(w, http.StatusBadRequest, e.Error())
	default:
		sendError(w, http.StatusInternalServerError, "failed to update job")
	}
}

type CreateSourceReq struct {
	URL                       string `json:"url"`
	Kind                      string `json:"kind"`
	AuthorizationAcknowledged bool   `json:"authorization_acknowledged"`
}

func (s *Server) handleCreateSource(w http.ResponseWriter, r *http.Request) {
	var req CreateSourceReq
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		sendError(w, 400, "invalid request body")
		return
	}
	if !req.AuthorizationAcknowledged {
		sendError(w, 400, "authorization_acknowledged must be true")
		return
	}
	if req.URL == "" {
		sendError(w, 400, "url is required")
		return
	}
	if req.Kind != "channel" && req.Kind != "playlist" && req.Kind != "video" {
		sendError(w, 400, "unsupported source kind")
		return
	}
	src, e := s.store.CreateSource(r.Context(), req.URL, req.Kind, true)
	if e != nil {
		sendError(w, 500, "failed to create source")
		return
	}
	// Reconciliation starts workers. Submission stays durable if the Cloud Run
	// control API is unavailable at this instant.
	sendJSON(w, http.StatusAccepted, src)
}
func (s *Server) handleListSources(w http.ResponseWriter, r *http.Request) {
	xs, e := s.store.ListSources(r.Context())
	if e != nil {
		sendError(w, 500, "failed to list sources")
		return
	}
	if xs == nil {
		xs = []control.Source{}
	}
	sendJSON(w, 200, xs)
}
func (s *Server) handleGetSource(w http.ResponseWriter, r *http.Request) {
	x, e := s.store.GetSource(r.Context(), r.PathValue("id"))
	if errors.Is(e, control.ErrNotFound) {
		sendError(w, 404, "source not found")
		return
	}
	if e != nil {
		sendError(w, 500, "failed to get source")
		return
	}
	sendJSON(w, 200, x)
}
func (s *Server) handleListVideos(w http.ResponseWriter, r *http.Request) {
	xs, e := s.store.ListVideos(r.Context(), r.URL.Query().Get("state"))
	if e != nil {
		sendError(w, 500, "failed to list videos")
		return
	}
	if xs == nil {
		xs = []control.Video{}
	}
	sendJSON(w, 200, xs)
}
func (s *Server) handleGetVideo(w http.ResponseWriter, r *http.Request) {
	x, e := s.store.GetVideo(r.Context(), r.PathValue("id"))
	if errors.Is(e, control.ErrNotFound) {
		sendError(w, 404, "video not found")
		return
	}
	if e != nil {
		sendError(w, 500, "failed to get video")
		return
	}
	sendJSON(w, 200, x)
}

// Cloud Run job tasks are short-lived. Video leases, rather than a worker
// record, are the authoritative ownership mechanism.
type workerReq struct {
	ID string `json:"id"`
}

func (s *Server) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var v workerReq
	if json.NewDecoder(r.Body).Decode(&v) != nil || v.ID == "" {
		sendError(w, 400, "invalid worker id")
		return
	}
	sendJSON(w, 200, map[string]string{"status": "registered"})
}
func (s *Server) handleWorkerHeartbeat(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, 200, map[string]string{"status": "ok"})
}
func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) { sendJSON(w, 200, []any{}) }

type ClaimJobReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration_sec"`
}

func (s *Server) handleClaimJob(w http.ResponseWriter, r *http.Request) {
	var req ClaimJobReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	if _, e := s.store.RecoverExpiredLeases(r.Context()); e != nil {
		sendError(w, 500, "failed to recover expired leases")
		return
	}
	d := time.Duration(req.LeaseDuration) * time.Second
	if d <= 0 {
		d = 5 * time.Minute
	}
	v, e := s.store.ClaimVideo(r.Context(), req.WorkerID, d)
	if errors.Is(e, sql.ErrNoRows) || errors.Is(e, control.ErrNotFound) {
		sendError(w, 404, "no pending jobs")
		return
	}
	if errors.Is(e, control.ErrCapacity) {
		sendError(w, 409, "artifact capacity unavailable")
		return
	}
	if e != nil {
		sendError(w, 500, "failed to claim job")
		return
	}
	sendJSON(w, 200, v)
}

type UpdateJobStateReq struct {
	WorkerID string `json:"worker_id"`
	State    string `json:"state"`
	Error    string `json:"error,omitempty"`
}

func (s *Server) handleUpdateJobState(w http.ResponseWriter, r *http.Request) {
	var req UpdateJobStateReq
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		sendError(w, 400, "invalid request body")
		return
	}
	if e := s.store.SetVideoState(r.Context(), r.PathValue("id"), req.WorkerID, req.State, req.Error); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "ok"})
}

type CompleteJobReq struct {
	WorkerID    string `json:"worker_id"`
	StoragePath string `json:"storage_path"`
	ActualSize  int64  `json:"actual_size_bytes"`
}

func (s *Server) handleCompleteJob(w http.ResponseWriter, r *http.Request) {
	var req CompleteJobReq
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		sendError(w, 400, "invalid request body")
		return
	}
	if e := s.store.CompleteVideo(r.Context(), r.PathValue("id"), req.WorkerID, req.StoragePath, req.ActualSize); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "ok"})
}

type RenewJobReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration_sec"`
}

func (s *Server) handleRenewJob(w http.ResponseWriter, r *http.Request) {
	var req RenewJobReq
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		sendError(w, 400, "invalid request body")
		return
	}
	d := time.Duration(req.LeaseDuration) * time.Second
	if d <= 0 {
		d = 5 * time.Minute
	}
	if e := s.store.RenewLease(r.Context(), r.PathValue("id"), req.WorkerID, d); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "ok"})
}

type QuarantineJobReq struct {
	WorkerID        string `json:"worker_id"`
	Class           string `json:"class"`
	CooldownSeconds int    `json:"cooldown_seconds"`
}

func (s *Server) handleQuarantineJob(w http.ResponseWriter, r *http.Request) {
	var req QuarantineJobReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	if req.Class != "rate_limited" && req.Class != "blocked" {
		sendError(w, 400, "invalid quarantine class")
		return
	}
	if req.CooldownSeconds <= 0 {
		req.CooldownSeconds = 3600
	}
	if e := s.store.QuarantineVideo(r.Context(), r.PathValue("id"), req.WorkerID, req.Class, time.Now().UTC().Add(time.Duration(req.CooldownSeconds)*time.Second)); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "quarantined"})
}
func (s *Server) handleRetryVideo(w http.ResponseWriter, r *http.Request) {
	if e := s.store.RetryVideo(r.Context(), r.PathValue("id")); e != nil {
		if errors.Is(e, control.ErrQuarantined) {
			sendError(w, 409, "video cooldown has not elapsed")
			return
		}
		sendError(w, 400, e.Error())
		return
	}
	sendJSON(w, 202, map[string]string{"status": "queued"})
}
func (s *Server) handleRetrySource(w http.ResponseWriter, r *http.Request) {
	if e := s.store.RetrySource(r.Context(), r.PathValue("id")); e != nil {
		if errors.Is(e, control.ErrQuarantined) {
			sendError(w, 409, "source cooldown has not elapsed")
			return
		}
		sendError(w, 400, e.Error())
		return
	}
	sendJSON(w, 202, map[string]string{"status": "queued"})
}
func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if e := s.Reconcile(r.Context()); e != nil {
		sendError(w, 500, "reconciliation failed")
		return
	}
	sendJSON(w, 200, map[string]string{"status": "ok"})
}
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	x, e := s.store.CountByState(r.Context())
	if e != nil {
		sendError(w, 500, "failed to get stats")
		return
	}
	sendJSON(w, 200, x)
}
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, 200, map[string]string{"status": "ok"})
}

type normalizerClaimReq struct {
	WorkerID      string `json:"worker_id"`
	LeaseDuration int    `json:"lease_duration_sec"`
}

func normalizerLease(n int) time.Duration {
	d := time.Duration(n) * time.Second
	if d <= 0 {
		d = 10 * time.Minute
	}
	return d
}
func (s *Server) handleClaimDiscovery(w http.ResponseWriter, r *http.Request) {
	var req normalizerClaimReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	x, e := s.store.ClaimDiscovery(r.Context(), req.WorkerID, normalizerLease(req.LeaseDuration))
	if errors.Is(e, control.ErrNotFound) {
		sendError(w, 404, "no discoverable source")
		return
	}
	if e != nil {
		sendError(w, 500, "failed to claim source")
		return
	}
	sendJSON(w, 200, x)
}

type checkpointReq struct {
	WorkerID string `json:"worker_id"`
	Index    int    `json:"index"`
	URL      string `json:"url"`
}

func (s *Server) handleDiscoveryCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req checkpointReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" || req.URL == "" || req.Index < 0 {
		sendError(w, 400, "invalid request body")
		return
	}
	if e := s.store.CheckpointDiscovery(r.Context(), r.PathValue("id"), req.WorkerID, req.Index, req.URL); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "checkpointed"})
}

type workerOnlyReq struct {
	WorkerID string `json:"worker_id"`
}

func (s *Server) handleDiscoveryComplete(w http.ResponseWriter, r *http.Request) {
	var req workerOnlyReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	if e := s.store.FinishDiscovery(r.Context(), r.PathValue("id"), req.WorkerID); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "discovered"})
}

type failureReq struct {
	WorkerID  string `json:"worker_id"`
	Retryable bool   `json:"retryable"`
	Class     string `json:"class"`
	Error     string `json:"error"`
}

func (s *Server) handleDiscoveryFailure(w http.ResponseWriter, r *http.Request) {
	var req failureReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	src, e := s.store.GetSource(r.Context(), r.PathValue("id"))
	if e != nil {
		workerError(w, e)
		return
	}
	if e = s.store.FailDiscovery(r.Context(), src.ID, req.WorkerID, req.Retryable, req.Class, req.Error, retryAt(src.AttemptCount)); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "released"})
}
func (s *Server) handleClaimRawURL(w http.ResponseWriter, r *http.Request) {
	var req normalizerClaimReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	x, e := s.store.ClaimRawURL(r.Context(), req.WorkerID, normalizerLease(req.LeaseDuration))
	if errors.Is(e, control.ErrNotFound) {
		sendError(w, 404, "no eligible raw URL")
		return
	}
	if e != nil {
		sendError(w, 500, "failed to claim raw URL")
		return
	}
	sendJSON(w, 200, x)
}

type rawCompleteReq struct {
	WorkerID      string `json:"worker_id"`
	YoutubeID     string `json:"youtube_id"`
	URL           string `json:"url"`
	Title         string `json:"title"`
	EstimatedSize int64  `json:"estimated_size_bytes"`
}

func (s *Server) handleRawURLComplete(w http.ResponseWriter, r *http.Request) {
	var req rawCompleteReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" || req.YoutubeID == "" || req.URL == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	if e := s.store.CompleteRawURL(r.Context(), r.PathValue("id"), req.WorkerID, control.Metadata{YoutubeID: req.YoutubeID, URL: req.URL, Title: req.Title, EstimatedSize: req.EstimatedSize}); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "normalized"})
}
func (s *Server) handleRawURLFailure(w http.ResponseWriter, r *http.Request) {
	var req failureReq
	if json.NewDecoder(r.Body).Decode(&req) != nil || req.WorkerID == "" {
		sendError(w, 400, "invalid request body")
		return
	}
	raw, e := s.store.GetRawURL(r.Context(), r.PathValue("id"))
	if e != nil {
		workerError(w, e)
		return
	}
	if e = s.store.ReleaseRawURL(r.Context(), raw.ID, req.WorkerID, req.Retryable, req.Class, req.Error, retryAt(raw.AttemptCount)); e != nil {
		workerError(w, e)
		return
	}
	sendJSON(w, 200, map[string]string{"status": "released"})
}
