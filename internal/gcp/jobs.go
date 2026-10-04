package gcp

import (
	"context"
	"fmt"
	"time"

	run "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// JobRunner is the narrow seam used by the master. Tests can provide a fake;
// only this implementation talks to the Cloud Run Jobs API.
type JobRunner interface {
	RunDiscovery(context.Context) (string, error)
	RunURLNormalizer(context.Context, int) (string, error)
	RunDownloader(context.Context, int) (string, error)
	Close() error
}

// jobsAPI and executionsAPI keep generated clients at the edge of this
// package. They let unit tests exercise launch/reconciliation behavior without
// credentials or a Cloud Run project.
type runJobOperation interface {
	Metadata() (*runpb.Execution, error)
}
type jobsAPI interface {
	RunJob(context.Context, *runpb.RunJobRequest) (runJobOperation, error)
	Close() error
}
type executionsAPI interface {
	GetExecution(context.Context, *runpb.GetExecutionRequest) (*runpb.Execution, error)
	Close() error
}
type jobsClient struct{ *run.JobsClient }

func (c jobsClient) RunJob(ctx context.Context, req *runpb.RunJobRequest) (runJobOperation, error) {
	return c.JobsClient.RunJob(ctx, req)
}

type executionsClient struct{ *run.ExecutionsClient }

func (c executionsClient) GetExecution(ctx context.Context, req *runpb.GetExecutionRequest) (*runpb.Execution, error) {
	return c.ExecutionsClient.GetExecution(ctx, req)
}

type CloudRunJobs struct {
	client                            jobsAPI
	executions                        executionsAPI
	discovery, normalizer, downloader string
	timeout                           time.Duration
}

func NewCloudRunJobs(ctx context.Context, project, region, discoveryJob, normalizerJob, downloaderJob string, timeout time.Duration) (*CloudRunJobs, error) {
	c, e := run.NewJobsClient(ctx)
	if e != nil {
		return nil, e
	}
	executions, e := run.NewExecutionsClient(ctx)
	if e != nil {
		_ = c.Close()
		return nil, e
	}
	return newCloudRunJobs(jobsClient{c}, executionsClient{executions}, project, region, discoveryJob, normalizerJob, downloaderJob, timeout), nil
}
func newCloudRunJobs(client jobsAPI, executions executionsAPI, project, region, discoveryJob, normalizerJob, downloaderJob string, timeout time.Duration) *CloudRunJobs {
	prefix := fmt.Sprintf("projects/%s/locations/%s/jobs/", project, region)
	if timeout <= 0 {
		timeout = 45 * time.Minute
	}
	return &CloudRunJobs{client: client, executions: executions, discovery: prefix + discoveryJob, normalizer: prefix + normalizerJob, downloader: prefix + downloaderJob, timeout: timeout}
}
func (j *CloudRunJobs) Close() error {
	err := j.client.Close()
	second := j.executions.Close()
	if err != nil {
		return err
	}
	return second
}
func (j *CloudRunJobs) run(ctx context.Context, name string, count int, env []*runpb.EnvVar) (string, error) {
	op, e := j.client.RunJob(ctx, &runpb.RunJobRequest{Name: name, Overrides: &runpb.RunJobRequest_Overrides{TaskCount: int32(count), Timeout: durationpb.New(j.timeout), ContainerOverrides: []*runpb.RunJobRequest_Overrides_ContainerOverride{{Env: env}}}})
	if e != nil {
		return "", e
	}
	metadata, e := op.Metadata()
	if e != nil {
		return "", fmt.Errorf("read RunJob execution metadata: %w", e)
	}
	if metadata == nil || metadata.Name == "" {
		return "", fmt.Errorf("RunJob returned no execution resource name")
	}
	return metadata.Name, nil
}
func (j *CloudRunJobs) RunDiscovery(ctx context.Context) (string, error) {
	return j.run(ctx, j.discovery, 1, nil)
}
func (j *CloudRunJobs) RunURLNormalizer(ctx context.Context, count int) (string, error) {
	if count < 1 {
		count = 1
	}
	if count > 25 {
		count = 25
	}
	return j.run(ctx, j.normalizer, count, nil)
}
func (j *CloudRunJobs) RunDownloader(ctx context.Context, count int) (string, error) {
	if count < 1 {
		count = 1
	}
	if count > 10 {
		count = 10
	}
	return j.run(ctx, j.downloader, count, nil)
}

// ExecutionDone provides reconciliation without guessing from an operation
// response. A completion timestamp is terminal; failed tasks make it failed.
func (j *CloudRunJobs) ExecutionDone(ctx context.Context, name string) (done, successful bool, err error) {
	exec, err := j.executions.GetExecution(ctx, &runpb.GetExecutionRequest{Name: name})
	if err != nil {
		return false, false, err
	}
	if exec.CompletionTime == nil {
		return false, false, nil
	}
	return true, exec.FailedCount == 0 && exec.CancelledCount == 0, nil
}
