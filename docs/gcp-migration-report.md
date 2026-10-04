# Scrape-Tupe: GCP Migration Research Report

**Prepared:** 2026-10-04
**Decision:** replace Bunny Magic Containers and Bunny Storage with **Cloud Run Jobs** and **Cloud Storage**. For an Always Free-oriented MVP, run the master on **Cloud Run** and move its state to **Firestore (Native mode)**. Keep **Cloud SQL for PostgreSQL** as the paid scale-up path, not the free-first default.

## Executive recommendation

Use this GCP architecture for the first production version:

```text
Client
  │ authenticated HTTPS
  ▼
Cloud Run service: master/normalizer API
  ├── Firestore (Native mode): sources, videos, workers, executions
  ├── Cloud Run Jobs API: start one bounded downloader execution
  │       └── N parallel downloader tasks, each claims jobs over private HTTP
  └── Cloud Storage: videos/{youtube_id}/{filename}

Artifact Registry ← Cloud Build/CI builds master and downloader images
Secret Manager  ← third-party credentials/configuration, if any
Cloud Logging + Cloud Monitoring ← logs, alerts, and operational metrics
```

Firestore has a daily free quota of 1 GiB stored data, 50,000 document reads, 20,000 writes, and 20,000 deletes, plus 10 GiB/month outbound transfer. This comfortably covers a small control plane, but not unbounded batch activity. [Firestore free quota](https://cloud.google.com/firestore/pricing)

Cloud Run Jobs are the best match because this product has finite batches: a source is normalized, a known number of pending videos is available, download tasks run, and the execution ends. A job can run up to 10,000 independent tasks in parallel; task timeout is configurable up to seven days; and failed tasks can be retried. The master can override task count and timeout per execution. [Cloud Run jobs](https://cloud.google.com/run/docs/create-jobs), [execution overrides](https://cloud.google.com/run/docs/execute/jobs)

Do **not** use a Cloud Run worker pool for v1. It would preserve the current forever-polling downloader loop, but worker pools do not automatically scale; they require a running instance to start and need explicit/manual scaling or an external-metrics autoscaler. They are better for a permanent queue consumer, not this bursty batch workload. [Cloud Run worker pools](https://cloud.google.com/run/docs/overview/what-is-cloud-run)

## Mapping from the current design

| Current component | GCP replacement | Required change |
| --- | --- | --- |
| Bunny Magic Container downloader replicas | Cloud Run Job tasks | Master starts an execution with `tasks=N` and `parallelism=N` (bounded by a configured cap). |
| Bunny Storage | Cloud Storage bucket | Replace the Bunny `PUT` client with the Cloud Storage Go SDK; preserve `videos/{youtube_id}/{filename}` keys. |
| Bunny scaler/client | Cloud Run Jobs API client | Replace min/max replica updates and undeploy calls with a `RunJob` execution request and execution-status reconciliation. |
| Local SQLite master database | Firestore for the free-first MVP; Cloud SQL for paid production | Cloud Run local files are disposable. Port state and job-claim operations to Firestore transactions, or use PostgreSQL once a paid relational database is justified. |
| `.env` Bunny credentials | Service accounts + Secret Manager | Use Application Default Credentials for GCP APIs; reserve Secret Manager for genuine secrets. |
| Long-polling downloader | One bounded Cloud Run Job task | Claim/process until the queue is empty, then exit successfully. |

Cloud Run's writable filesystem is in-memory and disappears when an instance stops. It is appropriate only for each task's temporary yt-dlp directory, not SQLite or completed artifacts. [Cloud Run filesystem lifecycle](https://cloud.google.com/run/docs/overview/what-is-cloud-run)

## Recommended runtime behaviour

1. A caller submits a source to the private master API.
2. The normalizer resolves the source and inserts deduplicated `videos` rows.
3. In the same control path—or, preferably, after committing through an outbox/reconciler—the master counts runnable videos.
4. If no active downloader execution owns the current batch, master invokes Cloud Run Jobs with:
   - `taskCount = min(pending jobs, MAX_PARALLELISM)`
   - `parallelism = taskCount`
   - an explicit task timeout sized for the maximum permitted video
   - a small, deliberate Cloud Run retry count (for infrastructure failure only)
5. Each task uses a unique identity built from `CLOUD_RUN_EXECUTION`, `CLOUD_RUN_TASK_INDEX`, and its attempt. It repeatedly claims one video from master, downloads it to `/tmp`, uploads it to Cloud Storage, reports completion, then claims another job.
6. A task exits with status 0 after an empty-queue response. The execution naturally ends—there is no equivalent of Bunny undeploy to manage.
7. The master periodically recovers expired leases, marks stale workers/tasks dead, and launches another execution only if pending work remains and no suitable execution is active.

Keep the current lease design. Cloud Run task retries and master job leases solve different failures: the former restarts a failed container; the latter prevents two workers from believing they own the same video. Add an execution/batch table and a database-level guard so two API requests or reconciliation ticks cannot start duplicate fleets.

## Data and implementation changes

### Database: Firestore first; PostgreSQL when needed

An Always Free deployment cannot depend on the original Cloud SQL recommendation. Cloud SQL offers a time-limited trial option, but it is not an Always Free production database. Use Firestore Native mode for the low-volume MVP, then choose Cloud SQL PostgreSQL when relational queries, higher throughput, or production backup/HA requirements justify paid infrastructure. [Cloud SQL free trial limitations](https://cloud.google.com/sql/docs/mysql/free-trial-instance)

For the Firestore implementation:

- Model `sources`, `videos`, `workers`, and `executions` as collections. Make `youtube_id` the video document ID, which preserves deduplication without a separate unique index.
- Claim a video in a Firestore transaction: query a small ordered window of `pending` videos, transactionally re-read the selected document, and change it to `leased` with worker and lease fields. Handle transaction retries and contention explicitly.
- Maintain small counter documents (such as `stats/pending`) transactionally, rather than repeatedly scanning all video documents to count work.
- Use explicit lease-expiry queries and a scheduled reconciliation request. Avoid Firestore TTL for correctness: TTL deletion is not part of the free quota and is asynchronous.
- Enforce a project-level source/video/task cap so all reads and writes remain below the daily free allowances.

For a future PostgreSQL migration, the present SQLite database uses local-file WAL and SQLite-specific SQL such as `datetime('now')`, `INSERT OR REPLACE`, and `RETURNING` patterns. PostgreSQL supports `RETURNING`, but the SQL must be ported and claim semantics must become explicitly transactional.

PostgreSQL migration changes:

- Replace `modernc.org/sqlite` with `pgx/v5` (or `database/sql` with a PostgreSQL driver).
- Replace `DATETIME` with `TIMESTAMPTZ` and database expressions with `CURRENT_TIMESTAMP`.
- Claim jobs using a transaction plus `FOR UPDATE SKIP LOCKED`, then update the selected row. This is the important concurrency guarantee when several Cloud Run tasks claim work at once.
- Check `RowsAffected` for every worker-owned state transition and return a conflict if it is zero. The current code treats a zero-row update as success.
- Add `executions` (or `batches`) and an outbox/reconciliation lock table; record Cloud Run execution name, desired task count, status, and timestamps.
- Keep the `youtube_id` unique constraint and object path stable to retain deduplication.

Cloud Run can connect to Cloud SQL using a service identity with `roles/cloudsql.client`; Cloud SQL connectors/Auth Proxy support the `/cloudsql/INSTANCE_CONNECTION_NAME` Unix socket path. Use a bounded connection pool because Cloud SQL enforces connection limits. [Cloud Run to Cloud SQL](https://cloud.google.com/sql/docs/postgres/connect-run)

### Storage: replace the Bunny client

Implement a `GCSUploader` behind the existing `downloader.Uploader` interface:

- Bucket: one regional bucket in the same region as Cloud Run and Firestore.
- Object key: `videos/{youtube_id}/{filename}`.
- Content type: derive from the final file extension, rather than always using `application/octet-stream`.
- Use the Google Cloud Storage Go client with checksum validation enabled; Cloud Storage validates supplied checksums and stores checksum metadata. [Cloud Storage validation](https://cloud.google.com/storage/docs/data-validation)
- Use object generation preconditions (`DoesNotExist`) if an object must never be overwritten. Treat a pre-existing object as an idempotent success only after verifying its metadata/checksum.
- Configure lifecycle rules deliberately: e.g. transition older approved archives to a colder class or delete short-lived artifacts. Lifecycle actions are asynchronous, so application logic must not depend on immediate deletion. [Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle)

Avoid a public bucket by default. If consumers need downloads, issue short-lived signed URLs from the master after authorization, or put a controlled delivery layer in front of the bucket.

### Containers and build

- Create separate images for `master`, `normalizer`, and `downloader`; include pinned versions of `yt-dlp` and `ffmpeg` in the downloader image.
- Store images in Artifact Registry, which is Google's recommended registry for Cloud Run deployments. [Artifact Registry overview](https://cloud.google.com/artifact-registry/docs/overview)
- Use Cloud Build or CI to build, test, push, and deploy immutable image digests.
- Give yt-dlp sufficient ephemeral disk/memory for the largest permitted video; clean its per-job directory in all paths, not only successful upload paths.
- Pin Cloud Run task CPU/memory and task timeout after measuring representative downloads. Jobs are billed for the full lifetime of each started instance, with a one-minute minimum, so size concurrency and retry limits carefully. [Cloud Run pricing](https://cloud.google.com/run/pricing)

### Authentication, IAM, and network boundaries

- Deploy master as a Cloud Run **service** with authentication required; use internal ingress if every caller is on GCP, otherwise front it with an appropriate authenticated public edge.
- Deploy downloader as a Cloud Run **job** with a dedicated service account. It needs only authenticated invocation access to master, `storage.objects.create` (and narrowly scoped read if using idempotency checks), and Firestore access only if it truly connects directly—prefer that workers continue using master HTTP only.
- Give master its own service account Firestore access, object-signing capability only if it issues URLs, and permission to execute the downloader job (`run.jobs.run`).
- Use Secret Manager for third-party secrets and grant each service account only `roles/secretmanager.secretAccessor` on the specific secret it needs. Google recommends Secret Manager for Cloud Run secrets. [Cloud Run secrets](https://cloud.google.com/run/docs/configuring/services/secrets)
- Do not ship service-account JSON keys in `.env`, images, or source control. Cloud Run service identities obtain GCP credentials automatically.

## Phased migration plan

### Phase A — make the project cloud-ready locally

1. Implement the normalizer and tests.
2. Define a `StorageUploader` configuration that selects `noop`, local filesystem (tests), or GCS; remove Bunny-specific flags and documentation.
3. Change downloader behaviour from endless idle polling to an `exit-when-empty` mode for Cloud Run Job tasks.
4. Add Dockerfiles, pinned yt-dlp/ffmpeg, health checks for master, and integration-test fakes.

### Phase B — database and GCS

1. Port the DB package to Firestore, including transaction-safe claim, lease, and counter operations.
2. Provision the default Firestore Native-mode database and a regional Cloud Storage bucket in the chosen region.
3. Implement and test `GCSUploader`, including retries, checksum validation, and collision/idempotency behaviour.
4. Move configuration to service-account identities and Secret Manager.

### Phase C — Cloud Run deployment

1. Create Artifact Registry, build images, and deploy master as a private Cloud Run service.
2. Create the downloader Cloud Run Job with a dedicated service account, resource limits, timeout, and retry policy.
3. Add a small Cloud Run Jobs client in `internal/gcp` that starts executions with per-run task overrides and records their execution names.
4. Run an end-to-end test with one owned/authorized test video and one task.

### Phase D — reliable batch scaling and operations

1. Add the execution guard/outbox and reconciliation loop.
2. Scale each execution to pending work, capped at `MAX_PARALLELISM`; never start overlapping unbounded executions.
3. Add alerts for job failures, excessive lease recovery, Firestore quota usage, storage upload failure, and cost/budget thresholds.
4. Load-test using permitted content; tune task count, CPU/memory, task timeout, retry policy, and source submission limits.

## Always Free operating envelope

This section is a **hard operating target**, not a promise that Google will never issue a bill. Enable billing, budgets, and usage caps; Free Tier offers and prices can change, and egress or a missing cap can create charges.

Use `us-central1`, `us-west1`, or `us-east1` for the Cloud Storage bucket. The Cloud Storage Always Free allowance is shared across those regions: 5 GB-months of Standard storage, 5,000 Class A operations, 50,000 Class B operations, and 100 GB/month outbound transfer from North America. Keep Cloud Run, Artifact Registry, and the bucket in the same selected region. [Google Cloud Free Program](https://cloud.google.com/free/docs/free-cloud-features), [Cloud Storage pricing](https://cloud.google.com/storage/pricing)

| Service | Free-first limit | Project control |
| --- | --- | --- |
| Cloud Run Jobs | First 240,000 vCPU-seconds and 450,000 GiB-seconds/month on the current Cloud Run free allowance | 1 vCPU, 512 MiB, strict `MAX_PARALLELISM`, no idle tasks; CPU is the binding limit at roughly 66.7 task-hours/month. |
| Cloud Run master service | Request-based free allowance includes 2 million requests/month, 180,000 vCPU-seconds and 360,000 GiB-seconds | Scale to zero; keep endpoints terse; do not allocate always-on CPU. |
| Firestore default database | 1 GiB stored, 50k reads/day, 20k writes/day, 20k deletes/day | Low-volume control plane only; counters, source caps, and dashboard alerts. |
| Cloud Storage | 5 GB-months in the three eligible US regions | Cap final artifacts at 5 GB; use a lifecycle rule to delete test data; do not enable versioning for this MVP. |
| Artifact Registry | First 0.5 GiB-month/account | Use small multi-stage images and delete unneeded image versions. |
| Cloud Scheduler | First three jobs/account | Use one reconciliation schedule, if needed. |
| Secret Manager | Six active secret versions and 10,000 access operations/month | Use only for non-GCP secrets; do not store service-account keys. |

Cloud Run pricing and quota figures distinguish request-based services from jobs, so treat their compute budgets separately and monitor them independently. [Cloud Run pricing](https://cloud.google.com/run/pricing), [Free-tier details](https://cloud.google.com/free/docs/free-cloud-features)

The constraints mean this is appropriate for tests, demos, and a small owned-content archive, not bulk downloading. The 5 GB object-storage limit is especially restrictive: set a maximum artifact size and reject or delete jobs that would exceed it. A downloaded 500 MB video consumes 10% of the monthly storage allowance.

## Terraform plan

Terraform should provision all GCP resources, but it should **not** build or push images. CI builds versioned images into Artifact Registry; Terraform receives their immutable digests as variables. Terraform can manage Cloud Storage buckets, Cloud Run v2 services and jobs, service accounts/IAM, Firestore database configuration, Scheduler, and the billing budget. [Cloud Storage Terraform guide](https://cloud.google.com/storage/docs/terraform-create-bucket-upload-object), [Cloud Run Jobs Terraform](https://cloud.google.com/run/docs/create-jobs)

Suggested repository layout:

```text
infra/
  versions.tf       # Terraform and Google provider constraints
  providers.tf      # project and us-central1 region
  variables.tf      # project_id, billing_account, image digests, caps
  services.tf       # APIs, service accounts, IAM
  storage.tf        # private GCS bucket and lifecycle rule
  firestore.tf      # default Native-mode database
  run.tf            # private master service and downloader Job
  scheduler.tf      # optional single reconciliation trigger
  budget.tf         # small budget and Pub/Sub/email notifications
  outputs.tf        # master URL, bucket, job name
```

The following is intentionally a starting shape, not a ready-to-apply production module; it shows the free-first resource decisions and keeps the image digests as explicit inputs.

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    google = { source = "hashicorp/google", version = "~> 6.0" }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region # us-central1, us-west1, or us-east1
}

resource "google_storage_bucket" "videos" {
  name                        = "${var.project_id}-scrape-tupe-videos"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    condition { age = var.artifact_ttl_days }
    action    { type = "Delete" }
  }
}

resource "google_firestore_database" "control_plane" {
  name        = "(default)"
  location_id = var.region
  type        = "FIRESTORE_NATIVE"
}

resource "google_service_account" "master" {
  account_id   = "scrape-tupe-master"
  display_name = "Scrape-Tupe master"
}

resource "google_service_account" "downloader" {
  account_id   = "scrape-tupe-downloader"
  display_name = "Scrape-Tupe downloader"
}

resource "google_storage_bucket_iam_member" "downloader_create" {
  bucket = google_storage_bucket.videos.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.downloader.email}"
}

resource "google_cloud_run_v2_service" "master" {
  name     = "scrape-tupe-master"
  location = var.region

  template {
    service_account = google_service_account.master.email
    scaling { min_instance_count = 0, max_instance_count = 1 }
    containers {
      image = var.master_image_digest
      resources { limits = { cpu = "1", memory = "512Mi" } }
      env { name = "FIRESTORE_DATABASE" value = "(default)" }
    }
  }
}

resource "google_cloud_run_v2_job" "downloader" {
  name     = "scrape-tupe-downloader"
  location = var.region

  template {
    template {
      service_account = google_service_account.downloader.email
      timeout         = "1800s"
      max_retries     = 1
      containers {
        image = var.downloader_image_digest
        resources { limits = { cpu = "1", memory = "512Mi" } }
        env { name = "EXIT_WHEN_EMPTY" value = "true" }
      }
    }
  }
}
```

At runtime, master invokes the existing Job with an override for task count and parallelism; Terraform defines its upper resource configuration and IAM, while application code selects the current batch size. Add an IAM binding that permits the master service account to run this specific job, and a separate authenticated invocation binding for downloader-to-master calls. Do not grant project-wide Owner or Editor roles.

Add a billing budget at a deliberately small amount (for example, USD 1) and route notifications to email and/or Pub/Sub. Budgets are alerts, not a guaranteed spend stop: billing data is estimated and can lag. Programmatic notifications can trigger an automated response if a hard stop is required. [Billing budget notifications](https://cloud.google.com/billing/docs/how-to/budgets-programmatic-notifications)

## Important risks and decisions still needed

1. **Content rights and platform terms:** YouTube's terms restrict downloading or otherwise using content unless YouTube authorizes it or you have prior permission from YouTube and, where applicable, rights holders. Restrict this tool to content the operator owns or is explicitly authorized to archive; retain provenance/authorization records. This is a product and legal review requirement, not just a hosting decision. [YouTube Terms of Service](https://yt-terms.static.usercontent.goog/pdf/terms/20231215/en_us_20231215.pdf)
2. **Region:** to preserve Always Free eligibility, use `us-central1`, `us-west1`, or `us-east1` and keep Cloud Run, Firestore, Artifact Registry, and the GCS bucket aligned there. This reduces latency and inter-location transfer cost.
3. **Cost controls:** set a maximum task count, project budget alerts, per-source video/byte/duration limits, and retention lifecycle rules before broad rollout. Network egress and stored video retention are likely to dominate the cost profile more than the control plane.
4. **PostgreSQL migration approach:** decide whether to make a clean cut before production (recommended) or build a temporary SQLite local-development adapter. Do not run production master state on a Cloud Run filesystem.
5. **Ingress model:** decide whether this is an internal operator tool or an externally accessible product. That determines Cloud Run ingress, identity provider, rate limiting, and audit requirements.

## Acceptance criteria for the GCP version

- A normalized, authorized test source produces deduplicated video documents in Firestore.
- Master starts a Cloud Run Job with no more than the configured task cap.
- Parallel tasks never complete the same video twice; a crash or forced task stop requeues work after lease expiry.
- Completed artifacts exist under their stable key in private Cloud Storage with validated integrity metadata.
- Tasks exit after the queue drains; the master does not leave idle compute running.
- No Bunny credentials, clients, or deployment references remain in runtime configuration.
- IAM uses dedicated least-privilege service accounts and no checked-in credential files.
