package gcp

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/run/apiv2/runpb"
)

type fakeOperation struct {
	execution *runpb.Execution
	err       error
}

func (o fakeOperation) Metadata() (*runpb.Execution, error) { return o.execution, o.err }

type fakeJobs struct {
	request   *runpb.RunJobRequest
	operation runJobOperation
	err       error
}

func (f *fakeJobs) RunJob(_ context.Context, req *runpb.RunJobRequest) (runJobOperation, error) {
	f.request = req
	return f.operation, f.err
}
func (f *fakeJobs) Close() error { return nil }

type fakeExecutions struct {
	execution *runpb.Execution
	err       error
}

func (f *fakeExecutions) GetExecution(context.Context, *runpb.GetExecutionRequest) (*runpb.Execution, error) {
	return f.execution, f.err
}
func (f *fakeExecutions) Close() error { return nil }

func TestRunJobStoresExecutionResourceName(t *testing.T) {
	fake := &fakeJobs{operation: fakeOperation{execution: &runpb.Execution{Name: "projects/p/locations/r/jobs/downloader/executions/run-123"}}}
	j := newCloudRunJobs(fake, &fakeExecutions{}, "p", "r", "discovery", "normalizer", "downloader", 0)
	name, err := j.RunDownloader(context.Background(), 99)
	if err != nil {
		t.Fatal(err)
	}
	if name != "projects/p/locations/r/jobs/downloader/executions/run-123" {
		t.Fatalf("name=%q", name)
	}
	if fake.request.GetOverrides().GetTaskCount() != 10 {
		t.Fatalf("task count=%d", fake.request.GetOverrides().GetTaskCount())
	}
}

func TestRunJobRejectsMissingOrUnreadableMetadata(t *testing.T) {
	for _, operation := range []runJobOperation{fakeOperation{}, fakeOperation{err: errors.New("metadata unavailable")}} {
		j := newCloudRunJobs(&fakeJobs{operation: operation}, &fakeExecutions{}, "p", "r", "d", "n", "x", 0)
		if _, err := j.RunDiscovery(context.Background()); err == nil {
			t.Fatal("expected metadata failure")
		}
	}
}
