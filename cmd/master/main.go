package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/db"
	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/master"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP server address")
	dbPath := flag.String("db", "scrape-tupe.db", "Database file path")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	database, err := db.Open(*dbPath)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	srv := master.New(database, logger)
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
