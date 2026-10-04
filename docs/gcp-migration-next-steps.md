# GCP Migration — Next Steps

## Verified Phase A work

- The GCS uploader is tracked and supports Application Default Credentials, create-only object writes, CRC32C, and duplicate verification.
- Downloader configuration supports `noop`, `local`, and `gcs` storage.
- Bounded workers exit successfully when the queue is empty.
- Master and downloader Docker images are present.
- Master API errors, downloader non-2xx handling, and SQLite lease ownership and transition checks are hardened.

The master is still SQLite-backed. It is for local development only and is **not production-safe on Cloud Run**, whose writable filesystem is ephemeral.

## Immediate next feature: Firestore control plane

First define the master storage interface, preserving the SQLite adapter for local use. Then implement Firestore-backed source, video, worker, lease, and execution-guard operations with transaction tests. Video documents remain deduplicated by `youtube_id`; claims and all lease-owned mutations must be transactional, and expired leases require explicit recovery.

## Follow-on order

1. Build the normalizer using a fakeable yt-dlp metadata extractor.
2. Add guarded Cloud Run Job execution control with bounded task count and parallelism.
3. Add Terraform for Firestore, GCS, Artifact Registry, Cloud Run resources, least-privilege service accounts, and supporting operations resources.
4. Run one authorized bounded end-to-end batch, including lease-recovery and queue-drain exit checks.

## Required GCS IAM condition

The downloader must have both `storage.objects.create` and `storage.objects.get`. The latter is required because duplicate create failures are verified with `Attrs` before an existing object is accepted. Scope both permissions to artifact objects with a conditional bucket IAM binding such as:

```text
resource.name.startsWith(
  "projects/_/buckets/BUCKET_NAME/objects/videos/"
)
```

Object creation permission without object read permission does not support idempotent duplicate verification.
