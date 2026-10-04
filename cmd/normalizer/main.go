package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/normalizer"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	masterURL := os.Getenv("MASTER_URL")
	if masterURL == "" {
		logger.Error("MASTER_URL is required")
		os.Exit(2)
	}
	worker := os.Getenv("WORKER_ID")
	if worker == "" {
		worker = fmt.Sprintf("%s-%s", os.Getenv("CLOUD_RUN_EXECUTION"), os.Getenv("CLOUD_RUN_TASK_INDEX"))
	}
	w := &normalizer.HTTPWorker{MasterURL: masterURL, Audience: os.Getenv("MASTER_AUDIENCE"), WorkerID: worker}
	mode := os.Getenv("NORMALIZER_MODE")
	var err error
	if mode == "discovery" {
		err = w.RunDiscovery(context.Background())
	} else {
		err = w.RunURL(context.Background())
	}
	if err != nil {
		logger.Error("normalizer failed", "mode", mode, "error", err)
		os.Exit(1)
	}
}
