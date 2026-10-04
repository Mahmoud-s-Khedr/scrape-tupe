# Scrape-Tupe — GCP Migration Roadmap

Scrape-Tupe is moving to a bounded, pull-based GCP pipeline. The intended production shape is a Cloud Run **service** for the master, short-lived Cloud Run **Jobs** for downloaders, private GCS for artifacts, and Firestore for the control plane.

> Historical note: the Bunny-based architecture is retired. Its clients, flags, and deployment assumptions are not part of this roadmap.

## Target architecture

```text
source submission → normalizer → Firestore videos
                                  │
                         Cloud Run master service
                         leases / execution guard
                                  │
                  bounded Cloud Run downloader Job tasks
                                  │
                              yt-dlp / ffmpeg
                                  │
                           private GCS artifacts
```

The master owns the HTTP API, job state machine, renewable leases, and guarded execution decisions. Each downloader claims one job at a time, transitions it through `leased → downloading → uploading → completed`, and exits once the queue is empty. A valid lease is required for every worker-owned transition; expired work is recovered for another task.

Artifacts are immutable GCS objects at `videos/{youtube_id}/{filename}`. The uploader uses GCS create-only writes and CRC32C, and accepts an existing object only after its size and CRC32C match the local artifact.

## Migration phases

| Phase | Deliverable | Status |
| --- | --- | --- |
| A | Local master/downloader foundation: SQLite adapter, leases, Docker images, noop/local/GCS upload configuration, bounded empty-queue exit | In progress and locally verified |
| B | Storage abstraction and Firestore control plane | Next |
| C | Normalizer and guarded Cloud Run Job execution control | Deferred |
| D | Terraform, production IAM, and bounded end-to-end validation | Deferred |

### Phase A — current foundation

- The local master remains SQLite-backed for development only.
- Downloader HTTP operations reject every non-2xx response. A `409 Conflict` is a lease loss: the task does not upload, complete, or overwrite the job with an error.
- Lease renewal, worker state transitions, and completion are enforced in the SQLite update predicates, including lease expiry and state ordering.
- Docker images exist for the master and downloader. The downloader supports `noop`, `local`, and `gcs` storage backends; Cloud Run Job tasks use `EXIT_WHEN_EMPTY=true`.

This is not a production Cloud Run deployment: SQLite is stored on an ephemeral filesystem and is unsuitable as a Cloud Run service control plane.

### Phase B — Firestore control plane

Define a storage interface under the master and retain SQLite only as its local adapter. Implement Firestore documents for sources, videos (keyed by YouTube ID), workers, and job executions. Claims, lease renewal, completion, and execution guards must be transactional; lease recovery must query expired leases explicitly rather than relying on TTL.

### Phase C — normalizer and execution control

Build the normalizer to expand permitted channels, playlists, and direct video URLs into deduplicated video records. Then add guarded Cloud Run Job launching: the master sets `task_count` and `parallelism` to `min(pending_jobs, MAX_PARALLELISM)` only when no suitable execution is active. Tasks drain available work and stop; the master does not maintain an idle downloader fleet.

### Phase D — infrastructure and validation

Provision Firestore Native mode, Artifact Registry, a private regional GCS bucket, service accounts, authenticated Cloud Run resources, and any required reconciliation scheduler with Terraform. CI supplies immutable image digests; Terraform never builds images and credentials never live in source, images, or `.env` files.

Validate one authorized bounded batch from source submission through normalization, guarded task launch, GCS upload, and Firestore completion. Also force a task stop to verify lease recovery and verify tasks exit after drain.

## GCS IAM requirement

The downloader service account needs `storage.objects.create` for create-only uploads **and** `storage.objects.get` for idempotent duplicate verification: when a create precondition fails, the uploader calls `Attrs` to compare size and CRC32C. Bind both permissions with a bucket-level IAM condition limiting access to artifact names, for example:

```text
resource.name.startsWith(
  "projects/_/buckets/BUCKET_NAME/objects/videos/"
)
```

Use a separate conditional binding (or custom role) if needed to grant exactly those two permissions; `storage.objects.create` alone is insufficient.
