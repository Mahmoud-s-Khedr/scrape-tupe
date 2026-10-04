package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/downloader"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/envfile"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/gcp"
)

func main() {
	// Load .env file (if present) before parsing flags
	if err := envfile.Load(".env"); err != nil {
		slog.Error("failed to load .env", "error", err)
		os.Exit(1)
	}

	masterURL := flag.String("master", envOr("MASTER_URL", "http://localhost:8080"), "Master server URL")
	masterAudience := flag.String("master-audience", envOr("MASTER_AUDIENCE", ""), "Cloud Run ID-token audience; empty for local master")
	workerID := flag.String("id", envOr("WORKER_ID", ""), "Worker ID (defaults to hostname)")
	outputDir := flag.String("output", envOr("OUTPUT_DIR", "/tmp/scrape-tupe/downloads"), "Download output directory")
	storageBackend := flag.String("storage-backend", envOr("STORAGE_BACKEND", "noop"), "Storage backend: noop, local, or gcs")
	gcsBucket := flag.String("gcs-bucket", envOr("GCS_BUCKET", ""), "Private Cloud Storage bucket for gcs backend")
	localStorageDir := flag.String("local-storage-dir", envOr("LOCAL_STORAGE_DIR", ""), "Local artifact directory for local backend")
	cleanupTemp := flag.Bool("cleanup", envOr("CLEANUP_TEMP", "true") == "true", "Clean up each temporary job directory")
	exitWhenEmpty := flag.Bool("exit-when-empty", envOr("EXIT_WHEN_EMPTY", "false") == "true", "Exit successfully after the queue is drained")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	wid := *workerID
	if wid == "" {
		wid = defaultWorkerID()
		logger.Info("using generated worker ID", "worker_id", wid)
	}

	// Choose upload strategy. GCS uses Application Default Credentials; no key
	// material belongs in environment files or container images.
	var uploader downloader.Uploader
	switch *storageBackend {
	case "noop":
		uploader = downloader.NewNoopUploader(logger)
	case "local":
		local, err := downloader.NewLocalUploader(*localStorageDir)
		if err != nil {
			logger.Error("invalid local storage configuration", "error", err)
			os.Exit(1)
		}
		uploader = local
	case "gcs":
		cloudStorage, err := gcp.NewGCSUploader(context.Background(), *gcsBucket, logger)
		if err != nil {
			logger.Error("failed to configure Cloud Storage uploader", "error", err)
			os.Exit(1)
		}
		defer cloudStorage.Close()
		uploader = cloudStorage
	default:
		logger.Error("unknown storage backend", "storage_backend", *storageBackend)
		os.Exit(1)
	}

	cfg := downloader.Config{
		MasterURL:       *masterURL,
		WorkerID:        wid,
		OutputDir:       *outputDir,
		Uploader:        uploader,
		CleanupTempDir:  *cleanupTemp,
		ExitWhenEmpty:   *exitWhenEmpty,
		IDTokenAudience: *masterAudience,
	}

	w := downloader.New(cfg, logger)

	// Context that cancels on SIGINT/SIGTERM for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		logger.Info("received signal, shutting down gracefully...", "signal", sig)
		cancel()
	}()

	if err := w.Run(ctx); err != nil {
		logger.Error("worker exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("worker exited cleanly")
}

func defaultWorkerID() string {
	if execution, index := os.Getenv("CLOUD_RUN_EXECUTION"), os.Getenv("CLOUD_RUN_TASK_INDEX"); execution != "" && index != "" {
		return execution + "-task-" + index + "-attempt-" + envOr("CLOUD_RUN_TASK_ATTEMPT", "0")
	}
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return "local-worker"
}

// envOr returns the value of the environment variable named by key, or fallback
// only when the variable is not set. An explicit empty value is preserved.
func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
