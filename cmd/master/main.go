package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/envfile"
	controlfs "github.com/Mahmoud-s-Khedr/scrape-tupe/internal/firestore"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/gcp"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/master"
)

func main() {
	// Load .env file (if present) before parsing flags
	if err := envfile.Load(".env"); err != nil {
		slog.Error("failed to load .env", "error", err)
		os.Exit(1)
	}

	addr := flag.String("addr", envOr("MASTER_ADDR", ":8080"), "HTTP server address")
	dbPath := flag.String("db", envOr("MASTER_DB", "scrape-tupe.db"), "SQLite database file path (local only)")
	storeKind := flag.String("store", envOr("MASTER_STORE", "sqlite"), "Control store: sqlite (local) or firestore")
	projectID := flag.String("gcp-project", envOr("GOOGLE_CLOUD_PROJECT", ""), "GCP project for Firestore")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	var backend any
	var closer interface{ Close() error }
	switch *storeKind {
	case "sqlite":
		database, err := db.Open(*dbPath)
		if err != nil {
			logger.Error("failed to open local database", "error", err)
			os.Exit(1)
		}
		backend, closer = database, database
	case "firestore":
		store, err := controlfs.New(context.Background(), *projectID)
		if err != nil {
			logger.Error("failed to open Firestore", "error", err)
			os.Exit(1)
		}
		backend, closer = store, store
	default:
		logger.Error("unknown control store", "store", *storeKind)
		os.Exit(1)
	}
	defer closer.Close()

	srv := master.New(backend, logger)
	srv.SetAuth(envOr("OPERATOR_API_KEY", ""), envOr("MASTER_AUDIENCE", ""))
	srv.SetWorkerServiceAccounts(strings.Split(envOr("WORKER_SERVICE_ACCOUNTS", ""), ",")...)
	if err := srv.ValidateAuthConfig(); err != nil {
		logger.Error("invalid worker authentication configuration", "error", err)
		os.Exit(1)
	}
	if *storeKind == "firestore" {
		jobs, err := gcp.NewCloudRunJobs(context.Background(), *projectID, envOr("GCP_REGION", "us-central1"), envOr("DISCOVERY_JOB", "scrape-tupe-discovery"), envOr("NORMALIZER_JOB", "scrape-tupe-normalizer"), envOr("DOWNLOADER_JOB", "scrape-tupe-downloader"), 45*time.Minute)
		if err != nil {
			logger.Error("failed to configure Cloud Run Jobs client", "error", err)
			os.Exit(1)
		}
		defer jobs.Close()
		srv.SetJobRunner(jobs)
	}
	leaseRecoveryCtx, cancelLeaseRecovery := context.WithCancel(context.Background())
	defer cancelLeaseRecovery()
	go srv.RunLeaseRecovery(leaseRecoveryCtx, time.Minute)

	httpServer := &http.Server{
		Addr:    *addr,
		Handler: srv.Handler(),
	}

	go func() {
		logger.Info("starting server", "addr", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
	}
	logger.Info("server exited")
}

// envOr returns the value of the environment variable named by key, or fallback
// only when the variable is not set. An explicit empty value is preserved.
func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
