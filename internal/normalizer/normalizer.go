// Package normalizer expands an authorized source into bounded, deduplicated
// YouTube videos. It intentionally only reads metadata; downloader tasks own
// all media transfers.
package normalizer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/ytdlp"
	"google.golang.org/api/idtoken"
)

// HTTPWorker is the production normalizer. It only calls the master; the
// Firestore service account belongs exclusively to the master control plane.
type HTTPWorker struct {
	MasterURL, WorkerID, Audience string
	Client                        *http.Client
	Binary                        string
	LeaseDuration                 time.Duration
	RenewalInterval               time.Duration
	clientOnce                    sync.Once
	httpClient                    *http.Client
	clientErr                     error
}

func (w *HTTPWorker) client() (*http.Client, error) {
	w.clientOnce.Do(func() {
		if w.Client != nil {
			w.httpClient = w.Client
			return
		}
		if w.Audience == "" {
			w.httpClient = &http.Client{Timeout: 30 * time.Second}
			return
		}
		w.httpClient, w.clientErr = idtoken.NewClient(context.Background(), w.Audience)
		if w.httpClient != nil {
			w.httpClient.Timeout = 30 * time.Second
		}
	})
	return w.httpClient, w.clientErr
}
func (w *HTTPWorker) leaseDuration() time.Duration {
	if w.LeaseDuration > 0 {
		return w.LeaseDuration
	}
	return 10 * time.Minute
}
func (w *HTTPWorker) post(ctx context.Context, path string, in, out any) (int, error) {
	c, e := w.client()
	if e != nil {
		return 0, e
	}
	b, e := json.Marshal(in)
	if e != nil {
		return 0, e
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(w.MasterURL, "/")+path, bytes.NewReader(b))
	if e != nil {
		return 0, e
	}
	req.Header.Set("Content-Type", "application/json")
	res, e := c.Do(req)
	if e != nil {
		return 0, e
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		if res.StatusCode == http.StatusConflict {
			return res.StatusCode, control.ErrLeaseNotOwned
		}
		return res.StatusCode, fmt.Errorf("master %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return res.StatusCode, json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode, nil
}
func (w *HTTPWorker) claim(ctx context.Context, path string, out any) (bool, error) {
	status, e := w.post(ctx, path, map[string]any{"worker_id": w.WorkerID, "lease_duration_sec": int(w.leaseDuration().Seconds())}, out)
	if status == http.StatusNotFound {
		return false, nil
	}
	return status != 0, e
}
func (w *HTTPWorker) RunDiscovery(ctx context.Context) error {
	var src control.Source
	ok, e := w.claim(ctx, "/normalizer/discovery/claim", &src)
	if e != nil || !ok {
		return e
	}
	path := "/normalizer/discovery/" + src.ID
	e = w.withLease(ctx, path, src.LeaseUntil, func(task context.Context) error {
		index := 0
		if err := w.streamURLs(task, src.URL, func(u string) error {
			_, err := w.post(task, path+"/checkpoint", map[string]any{"worker_id": w.WorkerID, "index": index, "url": u}, nil)
			if err == nil {
				index++
			}
			return err
		}); err != nil {
			return err
		}
		_, err := w.post(task, path+"/complete", map[string]string{"worker_id": w.WorkerID}, nil)
		return err
	})
	return w.reportFailure(path, e)
}
func (w *HTTPWorker) RunURL(ctx context.Context) error {
	var raw control.RawURL
	ok, e := w.claim(ctx, "/normalizer/urls/claim", &raw)
	if e != nil || !ok {
		return e
	}
	path := "/normalizer/urls/" + raw.ID
	e = w.withLease(ctx, path, raw.LeaseUntil, func(task context.Context) error {
		m, err := w.metadata(task, raw.RawURL)
		if err != nil {
			return err
		}
		_, err = w.post(task, path+"/complete", map[string]any{"worker_id": w.WorkerID, "youtube_id": m.YoutubeID, "url": m.URL, "title": m.Title, "estimated_size_bytes": m.EstimatedSize}, nil)
		return err
	})
	return w.reportFailure(path, e)
}
func (w *HTTPWorker) reportFailure(path string, err error) error {
	if err == nil || errors.Is(err, control.ErrLeaseNotOwned) {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, releaseErr := w.post(ctx, path+"/failure", failure(w.WorkerID, err), nil)
	if releaseErr != nil {
		return errors.Join(err, fmt.Errorf("release failed: %w", releaseErr))
	}
	return err
}

// Renewal failures cannot silently allow processing beyond known ownership.
func (w *HTTPWorker) withLease(ctx context.Context, path string, until *time.Time, run func(context.Context) error) error {
	task, cancel := context.WithCancelCause(ctx)
	duration := w.leaseDuration()
	deadline := time.Now().Add(duration)
	if until != nil {
		deadline = *until
	}
	interval := w.RenewalInterval
	if interval <= 0 {
		interval = duration / 3
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		expires := time.NewTimer(time.Until(deadline))
		defer expires.Stop()
		for {
			select {
			case <-task.Done():
				return
			case <-expires.C:
				cancel(control.ErrLeaseNotOwned)
				return
			case <-ticker.C:
				started := time.Now()
				request, c := context.WithDeadline(task, minTime(deadline, started.Add(30*time.Second)))
				_, e := w.post(request, path+"/renew", map[string]any{"worker_id": w.WorkerID, "lease_duration_sec": int(duration.Seconds())}, nil)
				c()
				if errors.Is(e, control.ErrLeaseNotOwned) {
					cancel(e)
					return
				}
				if e == nil {
					deadline = started.Add(duration)
					if !expires.Stop() {
						select {
						case <-expires.C:
						default:
						}
					}
					expires.Reset(time.Until(deadline))
				}
			}
		}
	}()
	err := run(task)
	cause := context.Cause(task)
	cancel(context.Canceled)
	<-done
	if cause != nil {
		return cause
	}
	return err
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func failure(worker string, e error) map[string]any {
	class := classifyFailure(e)
	return map[string]any{"worker_id": worker, "retryable": class == "", "class": class, "error": e.Error()}
}
func (w *HTTPWorker) streamURLs(ctx context.Context, url string, each func(string) error) error {
	b := w.Binary
	if b == "" {
		b = "yt-dlp"
	}
	cmd := exec.CommandContext(ctx, b, "--js-runtimes", "deno", "--flat-playlist", "--skip-download", "--no-warnings", "--print", "%(webpage_url)s", "--", url)
	ytdlp.Configure(cmd)
	var diagnostic ytdlp.Diagnostics
	cmd.Stderr = &diagnostic
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	scan := bufio.NewScanner(stdout)
	scan.Buffer(make([]byte, 1024), 1024*1024)
	for scan.Scan() {
		u := strings.TrimSpace(scan.Text())
		if u != "" {
			if e := each(u); e != nil {
				_ = ytdlp.Stop(cmd)
				_ = cmd.Wait()
				return e
			}
		}
	}
	if e = scan.Err(); e != nil {
		_ = ytdlp.Stop(cmd)
		_ = cmd.Wait()
		return e
	}
	return diagnostic.Wrap(cmd.Wait())
}
func (w *HTTPWorker) metadata(ctx context.Context, url string) (control.Metadata, error) {
	b := w.Binary
	if b == "" {
		b = "yt-dlp"
	}
	out, e := ytdlp.Output(exec.CommandContext(ctx, b, "--js-runtimes", "deno", "--dump-single-json", "--skip-download", "--no-warnings", "--", url))
	if e != nil {
		return control.Metadata{}, fmt.Errorf("yt-dlp metadata: %w", e)
	}
	var x metadataDoc
	if e = json.Unmarshal(out, &x); e != nil {
		return control.Metadata{}, e
	}
	id := strings.TrimSpace(x.ID)
	if id == "" {
		return control.Metadata{}, errors.New("yt-dlp returned no video id")
	}
	size := formatSize(x.Filesize, x.FilesizeApprox)
	if size <= 0 && len(x.RequestedFormats) > 0 {
		size = sumFormats(x.RequestedFormats)
	}
	if size <= 0 && len(x.RequestedDownloads) > 0 {
		size = sumFormats(x.RequestedDownloads)
	}

	return control.Metadata{YoutubeID: id, Title: x.Title, URL: "https://www.youtube.com/watch?v=" + id, EstimatedSize: size}, nil
}

func classifyFailure(err error) string {
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "429") || strings.Contains(s, "rate limit") || strings.Contains(s, "too many requests") {
		return "rate_limited"
	}
	if strings.Contains(s, "blocked") || strings.Contains(s, "sign in to confirm") || strings.Contains(s, "access denied") {
		return "blocked"
	}
	return ""
}

type metadataDoc struct {
	ID                 string        `json:"id"`
	Title              string        `json:"title"`
	Filesize           int64         `json:"filesize"`
	FilesizeApprox     int64         `json:"filesize_approx"`
	RequestedFormats   []metadataDoc `json:"requested_formats"`
	RequestedDownloads []metadataDoc `json:"requested_downloads"`
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func formatSize(exact, approx int64) int64 {
	if exact > 0 {
		return exact
	}
	return approx
}
func sumFormats(fs []metadataDoc) int64 {
	var total int64
	for _, f := range fs {
		n := formatSize(f.Filesize, f.FilesizeApprox)
		if n <= 0 {
			return 0
		}
		if n > control.ArtifactBudgetBytes-total {
			return control.ArtifactBudgetBytes + 1
		}
		total += n
	}
	return total
}
