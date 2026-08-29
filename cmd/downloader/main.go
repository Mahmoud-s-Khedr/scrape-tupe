package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/downloader"
)

func main() {
	masterURL := flag.String("master", "http://localhost:8080", "Master server URL")
	workerID := flag.String("id", "", "Worker ID (defaults to hostname)")
	outputDir := flag.String("output", "/tmp/scrape-tupe/downloads", "Download output directory")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	wid := *workerID
	if wid == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "local-worker"
		}
		wid = hostname
		logger.Info("using hostname as worker ID", "worker_id", wid)
	}

	cfg := downloader.Config{
		MasterURL: *masterURL,
		WorkerID:  wid,
		OutputDir: *outputDir,
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
