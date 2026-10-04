package downloader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/idtoken"
)

// ErrNoPendingJobs is returned when the master has no job to lease.
var ErrNoPendingJobs = errors.New("no pending jobs")

// ErrLeaseConflict means the master rejected a worker operation because the
// job's lease is no longer owned by this worker.
var ErrLeaseConflict = errors.New("job lease conflict")

// HTTPStatusError preserves the master response when an HTTP request fails.
// It deliberately includes the response body because master errors are useful
// operational context for short-lived Cloud Run Job tasks.
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *HTTPStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("master request failed: %s", e.Status)
	}
	return fmt.Sprintf("master request failed: %s: %s", e.Status, e.Body)
}

func (e *HTTPStatusError) Is(target error) bool {
	return target == ErrLeaseConflict && e.StatusCode == http.StatusConflict
}

// Video mirrors the master's video response.
type Video struct {
	ID           int64   `json:"id"` // Legacy local SQLite id; Firestore uses YoutubeID as the route key.
	YoutubeID    string  `json:"youtube_id"`
	Title        *string `json:"title,omitempty"`
	State        string  `json:"state"`
	ReservedSize int64   `json:"reserved_size_bytes,omitempty"`
}

// UnmarshalJSON accepts legacy numeric IDs and Firestore's string YouTube-ID
// document IDs. Worker requests always prefer a YouTube ID when no numeric ID
// is present.
func (v *Video) UnmarshalJSON(data []byte) error {
	type wire struct {
		ID           json.RawMessage `json:"id"`
		YoutubeID    string          `json:"youtube_id"`
		Title        *string         `json:"title"`
		State        string          `json:"state"`
		ReservedSize int64           `json:"reserved_size_bytes"`
	}
	var x wire
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	v.YoutubeID = x.YoutubeID
	v.Title = x.Title
	v.State = x.State
	v.ReservedSize = x.ReservedSize
	if len(x.ID) > 0 {
		var n int64
		if json.Unmarshal(x.ID, &n) == nil {
			v.ID = n
		} else {
			var id string
			_ = json.Unmarshal(x.ID, &id)
			if v.YoutubeID == "" {
				v.YoutubeID = id
			}
		}
	}
	return nil
}

// Config holds the downloader configuration.
type Config struct {
	MasterURL       string        // Base URL of the master server (e.g. http://localhost:8080)
	WorkerID        string        // Unique identifier for this worker
	LeaseDuration   time.Duration // How long the lease lasts (default 5m)
	HeartbeatFreq   time.Duration // How often to send heartbeats (default 30s)
	OutputDir       string        // Base directory for downloaded files
	Uploader        Uploader      // Upload strategy (GCS, local filesystem, or noop)
	CleanupTempDir  bool          // Whether to delete temp dir after successful upload
	ExitWhenEmpty   bool          // Exit successfully instead of polling when the queue is empty
	IDTokenAudience string        // Cloud Run master URL; empty keeps local HTTP unauthenticated
}

// Worker is a single-video downloader that pulls jobs from the master.
type Worker struct {
	cfg     Config
	client  *http.Client
	logger  *slog.Logger
	initErr error
}

// New creates a new downloader worker.
func New(cfg Config, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 5 * time.Minute
	}
	if cfg.HeartbeatFreq == 0 {
		cfg.HeartbeatFreq = 30 * time.Second
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "/tmp/scrape-tupe/downloads"
	}
	if cfg.Uploader == nil {
		cfg.Uploader = NewNoopUploader(logger)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	var initErr error
	if cfg.IDTokenAudience != "" {
		idClient, err := idtoken.NewClient(context.Background(), cfg.IDTokenAudience)
		if err == nil {
			client = idClient
		} else {
			initErr = fmt.Errorf("configure Cloud Run ID token client: %w", err)
		}
	}
	return &Worker{
		cfg:     cfg,
		client:  client,
		logger:  logger,
		initErr: initErr,
	}
}

// Run starts the worker loop. It blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	if w.initErr != nil {
		return w.initErr
	}
	w.logger.Info("worker starting", "worker_id", w.cfg.WorkerID, "master", w.cfg.MasterURL)

	if err := w.register(ctx); err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}
	w.logger.Info("worker registered")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker shutting down (context cancelled)")
			return nil
		default:
		}

		video, err := w.claimJob(ctx)
		if err != nil {
			if errors.Is(err, ErrNoPendingJobs) && w.cfg.ExitWhenEmpty {
				w.logger.Info("no pending jobs; bounded worker exiting")
				return nil
			}
			w.logger.Info("no pending jobs, waiting 5s before retrying", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}

		w.logger.Info("claimed job",
			"job_id", video.jobKey(),
			"youtube_id", video.YoutubeID,
			"title", video.Title,
		)

		if err := w.processJob(ctx, video); err != nil {
			if errors.Is(err, ErrLeaseConflict) {
				w.logger.Warn("job lease lost; leaving it for recovery or its new owner",
					"job_id", video.jobKey(),
					"youtube_id", video.YoutubeID,
				)
				continue
			}
			w.logger.Error("job failed",
				"job_id", video.jobKey(),
				"youtube_id", video.YoutubeID,
				"error", err,
			)
			errMsg := err.Error()
			_ = w.updateJobState(ctx, video.jobKey(), "error", &errMsg)
		}
	}
}

// register tells the master about this worker.
func (w *Worker) register(ctx context.Context) error {
	body := map[string]string{"id": w.cfg.WorkerID}
	_, err := w.postJSON(ctx, "/workers/register", body)
	return err
}

// claimJob asks the master for the next pending video.
func (w *Worker) claimJob(ctx context.Context) (*Video, error) {
	body := map[string]any{
		"worker_id":          w.cfg.WorkerID,
		"lease_duration_sec": int(w.cfg.LeaseDuration.Seconds()),
	}
	resp, err := w.postJSON(ctx, "/jobs/claim", body)
	if err != nil {
		var statusErr *HTTPStatusError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
			return nil, ErrNoPendingJobs
		}
		return nil, err
	}
	defer resp.Body.Close()

	var video Video
	if err := json.NewDecoder(resp.Body).Decode(&video); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}
	return &video, nil
}

// processJob downloads a single video using yt-dlp and uploads it.
func (w *Worker) processJob(ctx context.Context, video *Video) error {
	// Update state to downloading
	jobID := video.jobKey()
	if err := w.updateJobState(ctx, jobID, "downloading", nil); err != nil {
		return fmt.Errorf("failed to update state to downloading: %w", err)
	}

	// Create per-job temp directory
	jobDir := filepath.Join(w.cfg.OutputDir, "job-"+safeName(jobID))
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return fmt.Errorf("failed to create job dir: %w", err)
	}
	if w.cfg.CleanupTempDir {
		defer func() {
			if err := os.RemoveAll(jobDir); err != nil {
				w.logger.Warn("failed to clean up job dir", "dir", jobDir, "error", err)
			}
		}()
	}

	// Start heartbeat goroutine
	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()
	leaseLost := make(chan struct{})
	var markLeaseLost sync.Once
	go w.heartbeatLoopWithLeaseLoss(jobCtx, jobID, func() {
		markLeaseLost.Do(func() {
			close(leaseLost)
			jobCancel()
		})
	})
	hasLostLease := func() bool {
		select {
		case <-leaseLost:
			return true
		default:
			return false
		}
	}

	// Run yt-dlp
	outputPath, err := w.runYTDLP(jobCtx, video.YoutubeID, jobDir, video.ReservedSize)
	if err != nil {
		if hasLostLease() {
			return ErrLeaseConflict
		}
		if class := classifyYTDLPFailure(err); class != "" {
			_ = w.quarantineJob(ctx, jobID, class)
			return nil
		}
		return fmt.Errorf("yt-dlp failed: %w", err)
	}
	if hasLostLease() {
		return ErrLeaseConflict
	}

	w.logger.Info("download complete",
		"job_id", jobID,
		"youtube_id", video.YoutubeID,
		"output_path", outputPath,
	)

	// Upload to storage
	info, err := os.Stat(outputPath)
	if err != nil {
		return fmt.Errorf("stat downloaded file: %w", err)
	}
	if video.ReservedSize > 0 && info.Size() > video.ReservedSize {
		return fmt.Errorf("downloaded file exceeds reservation")
	}
	if err := w.updateJobState(jobCtx, jobID, "uploading", nil); err != nil {
		return fmt.Errorf("failed to update state to uploading: %w", err)
	}
	if hasLostLease() {
		return ErrLeaseConflict
	}

	remotePath := BuildRemotePath(video.YoutubeID, outputPath)
	storagePath, err := w.cfg.Uploader.Upload(jobCtx, outputPath, remotePath)
	if err != nil {
		if hasLostLease() {
			return ErrLeaseConflict
		}
		return fmt.Errorf("upload failed: %w", err)
	}
	if hasLostLease() {
		return ErrLeaseConflict
	}

	// Mark job as completed
	if err := w.completeJob(jobCtx, jobID, storagePath, info.Size()); err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}

	w.logger.Info("job completed",
		"job_id", jobID,
		"youtube_id", video.YoutubeID,
		"storage_path", storagePath,
	)

	return nil
}

// runYTDLP executes yt-dlp to download a video. Returns the output file path.
func (w *Worker) runYTDLP(ctx context.Context, youtubeID string, outputDir string, maximumSize int64) (string, error) {
	url := fmt.Sprintf("https://www.youtube.com/watch?v=%s", youtubeID)

	// Output template: put the file in outputDir with youtube_id as name
	outputTemplate := filepath.Join(outputDir, "%(id)s.%(ext)s")

	args := []string{
		"--js-runtimes", "deno",
		"--no-playlist",
		"--restrict-filenames",
		"--print", "after_move:filepath",
		"-o", outputTemplate,
		url,
	}
	if maximumSize > 0 {
		args = append([]string{"--max-filesize", strconv.FormatInt(maximumSize, 10)}, args...)
	}

	w.logger.Info("running yt-dlp", "youtube_id", youtubeID, "args", args)

	cmd := exec.CommandContext(ctx, "yt-dlp", args...)
	cmd.Stderr = os.Stderr

	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("yt-dlp exited with error: %w", err)
	}

	// yt-dlp --print after_move:filepath outputs the final file path
	outputPath := trimNewlines(string(output))
	if outputPath == "" {
		// Fallback: look for a file in the output directory
		entries, _ := os.ReadDir(outputDir)
		for _, e := range entries {
			if !e.IsDir() {
				outputPath = filepath.Join(outputDir, e.Name())
				break
			}
		}
	}

	if outputPath == "" {
		return "", fmt.Errorf("yt-dlp produced no output file")
	}

	return outputPath, nil
}

// heartbeatLoop periodically sends heartbeats and lease renewals.
func (w *Worker) heartbeatLoop(ctx context.Context, jobID any) {
	w.heartbeatLoopWithLeaseLoss(ctx, jobID, nil)
}

func (w *Worker) heartbeatLoopWithLeaseLoss(ctx context.Context, jobID any, onLeaseLost func()) {
	ticker := time.NewTicker(w.cfg.HeartbeatFreq)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Send worker heartbeat
			if err := w.sendHeartbeat(ctx); err != nil {
				w.logger.Warn("heartbeat failed", "error", err)
			}
			// Renew the lease
			if err := w.renewLease(ctx, jobID); err != nil {
				w.logger.Warn("lease renewal failed", "error", err)
				if errors.Is(err, ErrLeaseConflict) && onLeaseLost != nil {
					onLeaseLost()
					return
				}
			}
		}
	}
}

// sendHeartbeat tells the master the worker is still alive.
func (w *Worker) sendHeartbeat(ctx context.Context) error {
	body := map[string]string{"id": w.cfg.WorkerID}
	resp, err := w.postJSON(ctx, "/workers/heartbeat", body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// renewLease extends the lease on the current job.
func (w *Worker) renewLease(ctx context.Context, jobID any) error {
	body := map[string]any{
		"worker_id":          w.cfg.WorkerID,
		"lease_duration_sec": int(w.cfg.LeaseDuration.Seconds()),
	}
	resp, err := w.postJSON(ctx, "/jobs/"+jobIDString(jobID)+"/renew", body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// updateJobState sets the job state on the master.
func (w *Worker) updateJobState(ctx context.Context, jobID any, state string, errMsg *string) error {
	body := map[string]any{
		"worker_id": w.cfg.WorkerID,
		"state":     state,
	}
	if errMsg != nil {
		body["error"] = *errMsg
	}
	resp, err := w.postJSON(ctx, "/jobs/"+jobIDString(jobID)+"/state", body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// completeJob marks a job as completed with a storage path.
func (w *Worker) completeJob(ctx context.Context, jobID any, storagePath string, actual ...int64) error {
	body := map[string]any{
		"worker_id":    w.cfg.WorkerID,
		"storage_path": storagePath,
	}
	if len(actual) > 0 {
		body["actual_size_bytes"] = actual[0]
	}
	resp, err := w.postJSON(ctx, "/jobs/"+jobIDString(jobID)+"/complete", body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (v *Video) jobKey() string {
	if v.ID != 0 {
		return strconv.FormatInt(v.ID, 10)
	}
	return v.YoutubeID
}
func jobIDString(id any) string {
	switch v := id.(type) {
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}
func (w *Worker) quarantineJob(ctx context.Context, jobID, class string) error {
	resp, e := w.postJSON(ctx, "/jobs/"+jobID+"/quarantine", map[string]any{"worker_id": w.cfg.WorkerID, "class": class, "cooldown_seconds": 3600})
	if e != nil {
		return e
	}
	resp.Body.Close()
	return nil
}
func classifyYTDLPFailure(err error) string {
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "http error 429") || strings.Contains(s, "too many requests") || strings.Contains(s, "rate limit") {
		return "rate_limited"
	}
	if strings.Contains(s, "sign in to confirm") || strings.Contains(s, "not available in your country") || strings.Contains(s, "access denied") || strings.Contains(s, "blocked") {
		return "blocked"
	}
	return ""
}

// postJSON is a helper that POSTs JSON to a master endpoint.
func (w *Worker) postJSON(ctx context.Context, path string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.MasterURL+path, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		responseBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, &HTTPStatusError{StatusCode: resp.StatusCode, Status: resp.Status, Body: string(responseBody)}
	}
	return resp, nil
}

// trimNewlines removes leading/trailing whitespace and newlines.
func trimNewlines(s string) string {
	// Trim all trailing newlines/spaces, then take the last line
	// (yt-dlp might print progress before the final path)
	lines := splitNonEmpty(s)
	if len(lines) == 0 {
		return ""
	}
	return lines[len(lines)-1]
}

// splitNonEmpty splits a string by newlines and returns non-empty lines.
func splitNonEmpty(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' || s[i] == '\r' {
			line := s[start:i]
			if len(line) > 0 {
				result = append(result, line)
			}
			start = i + 1
		}
	}
	if start < len(s) {
		line := s[start:]
		if len(line) > 0 {
			result = append(result, line)
		}
	}
	return result
}
