package master

import (
	"context"
	"errors"
	"log/slog"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
)

type Scaler struct {
	logger *slog.Logger
}

// Reconcile launches at most one bounded downloader execution. The execution
// intent is committed before RunJob. If the API outcome is ambiguous, it stays
// "launching" and subsequent ticks fail closed instead of overlapping batches.
func (s *Server) Reconcile(ctx context.Context) error {
	if s.jobs == nil {
		return nil
	}
	if _, err := s.store.RecoverExpiredLeases(ctx); err != nil {
		return err
	}
	work, err := s.store.WorkCounts(ctx)
	if err != nil {
		return err
	}
	if work["discoverable_sources"] > 0 {
		if err := s.reconcileAux(ctx, "discovery", 1, func(ctx context.Context, _ int) (string, error) { return s.jobs.RunDiscovery(ctx) }); err != nil {
			return err
		}
	}
	if work["eligible_raw_urls"] > 0 {
		count := work["eligible_raw_urls"]
		if count > 25 {
			count = 25
		}
		if err := s.reconcileAux(ctx, "url-normalizer", count, s.jobs.RunURLNormalizer); err != nil {
			return err
		}
	}
	active, err := s.store.GetActiveDownloaderExecution(ctx)
	if err != nil {
		return err
	}
	if active != nil {
		// A launch with no execution name is intentionally ambiguous and remains
		// guarded. Once RunJob returned a name, query its terminal status rather
		// than relying on a task's eventual HTTP callback.
		if inspector, ok := s.jobs.(ExecutionInspector); ok && active.CloudRunName != "" {
			done, successful, err := inspector.ExecutionDone(ctx, active.CloudRunName)
			if err != nil {
				return err
			}
			if !done {
				return nil
			}
			state := "completed"
			if !successful {
				state = "failed"
			}
			if err := s.store.UpdateExecution(ctx, active.ID, state, active.CloudRunName); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
	counts, err := s.store.CountByState(ctx)
	if err != nil {
		return err
	}
	pending := counts["pending"]
	if pending == 0 {
		return nil
	}
	if pending > 10 {
		pending = 10
	}
	exec, err := s.store.CreateExecution(ctx, control.Execution{Kind: "downloader", TaskCount: pending})
	if errors.Is(err, control.ErrExecutionActive) {
		return nil
	}
	if err != nil {
		return err
	}
	name, err := s.jobs.RunDownloader(ctx, pending)
	if err != nil {
		s.logger.Error("downloader launch outcome is unknown; execution remains guarded", "execution_id", exec.ID, "error", err)
		return err
	}
	return s.store.UpdateExecution(ctx, exec.ID, "running", name)
}

func (s *Server) reconcileAux(ctx context.Context, kind string, count int, run func(context.Context, int) (string, error)) error {
	active, err := s.store.GetActiveExecution(ctx, kind)
	if err != nil {
		return err
	}
	if active != nil {
		if inspector, ok := s.jobs.(ExecutionInspector); ok && active.CloudRunName != "" {
			done, success, e := inspector.ExecutionDone(ctx, active.CloudRunName)
			if e != nil {
				return e
			}
			if !done {
				return nil
			}
			state := "completed"
			if !success {
				state = "failed"
			}
			return s.store.UpdateExecution(ctx, active.ID, state, active.CloudRunName)
		}
		return nil
	}
	exec, err := s.store.CreateExecution(ctx, control.Execution{Kind: kind, TaskCount: count})
	if errors.Is(err, control.ErrExecutionActive) {
		return nil
	}
	if err != nil {
		return err
	}
	name, err := run(ctx, count)
	if err != nil {
		return err
	}
	return s.store.UpdateExecution(ctx, exec.ID, "running", name)
}

func NewScaler(logger *slog.Logger) *Scaler {
	return &Scaler{logger: logger}
}

func (s *Scaler) Run(ctx context.Context) {
	s.logger.Info("scaler not yet implemented")
}
