package master

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Mahmoud-s-Khedr/scrape-tupe/internal/control"
)

type scalerStore struct {
	control.Store
	work    map[string]int
	counts  map[string]int
	active  *control.Execution
	updates []string
}

func (s *scalerStore) RecoverExpiredLeases(context.Context) (int, error)    { return 0, nil }
func (s *scalerStore) WorkCounts(context.Context) (map[string]int, error)   { return s.work, nil }
func (s *scalerStore) CountByState(context.Context) (map[string]int, error) { return s.counts, nil }
func (s *scalerStore) GetActiveDownloaderExecution(context.Context) (*control.Execution, error) {
	return s.active, nil
}
func (s *scalerStore) GetActiveExecution(context.Context, string) (*control.Execution, error) {
	return nil, nil
}
func (s *scalerStore) CreateExecution(_ context.Context, e control.Execution) (*control.Execution, error) {
	if s.active != nil {
		return nil, control.ErrExecutionActive
	}
	e.ID, e.State = "execution-1", "launching"
	s.active = &e
	return &e, nil
}
func (s *scalerStore) UpdateExecution(_ context.Context, id, state, name string) error {
	s.updates = append(s.updates, state+":"+name)
	if s.active != nil && s.active.ID == id {
		s.active.State, s.active.CloudRunName = state, name
		if state == "completed" || state == "failed" {
			s.active = nil
		}
	}
	return nil
}

type scalerJobs struct {
	launches      int
	launchErr     error
	name          string
	done, success bool
}

func (j *scalerJobs) RunDiscovery(context.Context) (string, error)          { return "", nil }
func (j *scalerJobs) RunURLNormalizer(context.Context, int) (string, error) { return "", nil }
func (j *scalerJobs) RunDownloader(context.Context, int) (string, error) {
	j.launches++
	return j.name, j.launchErr
}
func (j *scalerJobs) ExecutionDone(context.Context, string) (bool, bool, error) {
	return j.done, j.success, nil
}

func newScalerServer(store control.Store, jobs JobRunner) *Server {
	s := New(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.SetJobRunner(jobs)
	return s
}
func TestReconcileStoresExecutionResourceNameAndTerminalFailure(t *testing.T) {
	store := &scalerStore{work: map[string]int{}, counts: map[string]int{"pending": 1}}
	jobs := &scalerJobs{name: "projects/p/locations/r/jobs/downloader/executions/e"}
	s := newScalerServer(store, jobs)
	if err := s.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if jobs.launches != 1 || len(store.updates) != 1 || store.updates[0] != "running:"+jobs.name {
		t.Fatalf("launches=%d updates=%v", jobs.launches, store.updates)
	}
	jobs.done, jobs.success = true, false
	store.counts["pending"] = 0
	if err := s.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.updates) != 2 || store.updates[1] != "failed:"+jobs.name {
		t.Fatalf("updates=%v", store.updates)
	}
}
func TestReconcileKeepsAmbiguousLaunchGuard(t *testing.T) {
	store := &scalerStore{work: map[string]int{}, counts: map[string]int{"pending": 1}}
	jobs := &scalerJobs{launchErr: errors.New("timeout")}
	s := newScalerServer(store, jobs)
	if err := s.Reconcile(context.Background()); err == nil {
		t.Fatal("expected ambiguous launch error")
	}
	if err := s.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if jobs.launches != 1 {
		t.Fatalf("overlapping launch count=%d", jobs.launches)
	}
}
