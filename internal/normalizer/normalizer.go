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
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
	"google.golang.org/api/idtoken"
)

// HTTPWorker is the production normalizer. It only calls the master; the
// Firestore service account belongs exclusively to the master control plane.
type HTTPWorker struct {
	MasterURL, WorkerID, Audience string
	Client                        *http.Client
	Binary                        string
}

func (w *HTTPWorker) client() (*http.Client, error) {
	if w.Client != nil {
		return w.Client, nil
	}
	if w.Audience == "" {
		return &http.Client{Timeout: 30 * time.Second}, nil
	}
	return idtoken.NewClient(context.Background(), w.Audience)
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
		return res.StatusCode, fmt.Errorf("master %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return res.StatusCode, json.NewDecoder(res.Body).Decode(out)
	}
	return res.StatusCode, nil
}
func (w *HTTPWorker) claim(ctx context.Context, path string, out any) (bool, error) {
	status, e := w.post(ctx, path, map[string]any{"worker_id": w.WorkerID, "lease_duration_sec": 600}, out)
	if status == http.StatusNotFound {
		return false, nil
	}
	return status != 0, e
}
func (w *HTTPWorker) RunDiscovery(ctx context.Context) error {
	var src control.Source
	ok, e := w.claim(ctx, "/normalizer/discovery/claim", &src)
	if e != nil {
		return e
	}
	if !ok {
		return nil
	}
	index := src.DiscoveryCursor
	err := w.streamURLs(ctx, src.URL, func(u string) error {
		_, e := w.post(ctx, "/normalizer/discovery/"+src.ID+"/checkpoint", map[string]any{"worker_id": w.WorkerID, "index": index, "url": u}, nil)
		if e == nil {
			index++
		}
		return e
	})
	if err != nil {
		_, _ = w.post(context.Background(), "/normalizer/discovery/"+src.ID+"/failure", failure(w.WorkerID, err), nil)
		return err
	}
	_, e = w.post(ctx, "/normalizer/discovery/"+src.ID+"/complete", map[string]string{"worker_id": w.WorkerID}, nil)
	return e
}
func (w *HTTPWorker) RunURL(ctx context.Context) error {
	var raw control.RawURL
	ok, e := w.claim(ctx, "/normalizer/urls/claim", &raw)
	if e != nil {
		return e
	}
	if !ok {
		return nil
	}
	m, e := w.metadata(ctx, raw.RawURL)
	if e != nil {
		_, _ = w.post(context.Background(), "/normalizer/urls/"+raw.ID+"/failure", failure(w.WorkerID, e), nil)
		return e
	}
	_, e = w.post(ctx, "/normalizer/urls/"+raw.ID+"/complete", map[string]any{"worker_id": w.WorkerID, "youtube_id": m.YoutubeID, "url": m.URL, "title": m.Title, "estimated_size_bytes": m.EstimatedSize}, nil)
	return e
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
	cmd := exec.CommandContext(ctx, b, "--js-runtimes", "deno", "--flat-playlist", "--skip-download", "--no-warnings", "--print", "%(webpage_url)s", url)
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
				_ = cmd.Process.Kill()
				return e
			}
		}
	}
	if e = scan.Err(); e != nil {
		return e
	}
	return cmd.Wait()
}
func (w *HTTPWorker) metadata(ctx context.Context, url string) (control.Metadata, error) {
	b := w.Binary
	if b == "" {
		b = "yt-dlp"
	}
	out, e := exec.CommandContext(ctx, b, "--js-runtimes", "deno", "--dump-single-json", "--skip-download", "--no-warnings", url).Output()
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
	size := x.Filesize
	if size <= 0 {
		size = x.FilesizeApprox
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
	ID             string `json:"id"`
	Title          string `json:"title"`
	Filesize       int64  `json:"filesize"`
	FilesizeApprox int64  `json:"filesize_approx"`
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
