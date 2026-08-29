package downloader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Video mirrors the master's video response.
type Video struct {
	ID        int64  `json:"id"`
	YoutubeID string `json:"youtube_id"`
	Title     *string `json:"title,omitempty"`
	State     string `json:"state"`
}

// Config holds the downloader configuration.
type Config struct {
	MasterURL     string        // Base URL of the master server (e.g. http://localhost:8080)
	WorkerID      string        // Unique identifier for this worker
	LeaseDuration time.Duration // How long the lease lasts (default 5m)
	HeartbeatFreq time.Duration // How often to send heartbeats (default 30s)
	OutputDir     string        // Base directory for downloaded files
}

// Worker is a single-video downloader that pulls jobs from the master.
type Worker struct {
	cfg    Config
	client *http.Client
	logger *slog.Logger
}

// New creates a new downloader worker.
func New(cfg Config, logger *slog.Logger) *Worker {
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 5 * time.Minute
	}
	if cfg.HeartbeatFreq == 0 {
		cfg.HeartbeatFreq = 30 * time.Second
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "/tmp/scrape-tupe/downloads"
	}
	return &Worker{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
		logger: logger,
	}
}

// Run starts the worker loop. It blocks until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
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
			w.logger.Info("no pending jobs, waiting 5s before retrying", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
			continue
		}

		w.logger.Info("claimed job",
			"job_id", video.ID,
			"youtube_id", video.YoutubeID,
			"title", video.Title,
		)

		if err := w.processJob(ctx, video); err != nil {
			w.logger.Error("job failed",
				"job_id", video.ID,
				"youtube_id", video.YoutubeID,
				"error", err,
			)
			errMsg := err.Error()
			_ = w.updateJobState(ctx, video.ID, "error", &errMsg)
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
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claim failed: %s %s", resp.Status, string(b))
	}

	var video Video
	if err := json.NewDecoder(resp.Body).Decode(&video); err != nil {
		return nil, fmt.Errorf("decode claim response: %w", err)
	}
	return &video, nil
}

// processJob downloads a single video using yt-dlp.
func (w *Worker) processJob(ctx context.Context, video *Video) error {
	// Update state to downloading
	if err := w.updateJobState(ctx, video.ID, "downloading", nil); err != nil {
		return fmt.Errorf("failed to update state to downloading: %w", err)
	}

	// Create per-job temp directory
	jobDir := filepath.Join(w.cfg.OutputDir, fmt.Sprintf("job-%d", video.ID))
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return fmt.Errorf("failed to create job dir: %w", err)
	}

	// Start heartbeat goroutine
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()
	go w.heartbeatLoop(heartbeatCtx, video.ID)

	// Run yt-dlp
	outputPath, err := w.runYTDLP(ctx, video.YoutubeID, jobDir)
	if err != nil {
		return fmt.Errorf("yt-dlp failed: %w", err)
	}

	w.logger.Info("download complete",
		"job_id", video.ID,
		"youtube_id", video.YoutubeID,
		"output_path", outputPath,
	)

	// Phase 3 will add Bunny Storage upload here.
	// For now, mark as completed with local path as storage_path.
	if err := w.completeJob(ctx, video.ID, outputPath); err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}

	w.logger.Info("job completed",
		"job_id", video.ID,
		"youtube_id", video.YoutubeID,
		"storage_path", outputPath,
	)
	return nil
}

// runYTDLP executes yt-dlp to download a video. Returns the output file path.
func (w *Worker) runYTDLP(ctx context.Context, youtubeID string, outputDir string) (string, error) {
	url := fmt.Sprintf("https://www.youtube.com/watch?v=%s", youtubeID)

	// Output template: put the file in outputDir with youtube_id as name
	outputTemplate := filepath.Join(outputDir, "%(id)s.%(ext)s")

	args := []string{
		"--no-playlist",
		"--restrict-filenames",
		"--print", "after_move:filepath",
		"-o", outputTemplate,
		url,
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
func (w *Worker) heartbeatLoop(ctx context.Context, jobID int64) {
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
func (w *Worker) renewLease(ctx context.Context, jobID int64) error {
	body := map[string]any{
		"worker_id":          w.cfg.WorkerID,
		"lease_duration_sec": int(w.cfg.LeaseDuration.Seconds()),
	}
	resp, err := w.postJSON(ctx, fmt.Sprintf("/jobs/%d/renew", jobID), body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// updateJobState sets the job state on the master.
func (w *Worker) updateJobState(ctx context.Context, jobID int64, state string, errMsg *string) error {
	body := map[string]any{
		"worker_id": w.cfg.WorkerID,
		"state":     state,
	}
	if errMsg != nil {
		body["error"] = *errMsg
	}
	resp, err := w.postJSON(ctx, fmt.Sprintf("/jobs/%d/state", jobID), body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// completeJob marks a job as completed with a storage path.
func (w *Worker) completeJob(ctx context.Context, jobID int64, storagePath string) error {
	body := map[string]string{
		"worker_id":    w.cfg.WorkerID,
		"storage_path": storagePath,
	}
	resp, err := w.postJSON(ctx, fmt.Sprintf("/jobs/%d/complete", jobID), body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
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
	return w.client.Do(req)
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
