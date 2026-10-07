# Operations and recovery

## Queue and discovery

Production reserves at most 5 GiB across stored artifacts and in-flight work.
Claims inspect up to 64 pending videos per transaction and save their scan
position. A temporarily unsuitable prefix cannot permanently hide later work.
Reconciliation checks capacity before launching downloaders, and bounded workers
exit when no candidate fits the current scan. Another reconciliation advances
the scan; large queues can require multiple ticks. Permanently oversized videos
become `error` with class `oversized`; correct their metadata before retrying.

Unknown-size videos reserve 512 MiB by default. The master setting
`UNKNOWN_SIZE_RESERVATION_BYTES` overrides this and must be positive and no
larger than the total budget. Workers pass `--max-filesize` to yt-dlp and check
the final artifact against the reservation before uploading.

Discovery replays from the beginning and deduplicates emitted URLs within each
source, including legacy opaque raw-record IDs. The cursor counts unique inserts;
it is progress information, not an extractor offset. Reordering a playlist does
not duplicate earlier URLs. Existing audit records remain intact.
`discovery_complete` distinguishes partial discovery from normalization during
retry and lease recovery.

Normalizers renew their leases independently of checkpoint traffic. Expired or
rejected ownership cancels processing, and HTTP requests have deadlines. Unix
cancellation kills the subprocess group, including ffmpeg and JavaScript children.
Errors retain the last 16 KiB of stderr for rate-limit and blocked-content policy.

## Artifact accounting

The production master requires `GCS_BUCKET` and metadata access to its private
`videos/` prefix; Terraform supplies both. Each GCS downloader registers
`POST /jobs/{id}/upload` before creating an object. The stored upload intent and
destination keep the capacity reservation durable even if completion fails.

Completion verifies GCS metadata and records the object generation. Scheduled
reconciliation scans up to 64 video documents using a durable cursor:

- Confirmed absent completed objects become `artifact_state=expired` and
  decrement used bytes exactly once. Their historical storage path remains
  visible; the expired status indicates it is no longer available.
- Abandoned intents with valid objects become completed artifacts, transferring
  reserved bytes into used bytes once the lease is inactive.
- Confirmed absent abandoned intents release capacity and requeue work after
  a two-minute settlement period. Active leases retain their reservations.
- Permission failures, timeouts, generation mismatches, and invalid sizes retain
  accounting and emit structured error logs for investigation.

Lifecycle deletion is asynchronous; budget recovery also waits for the scan.
The artifact prefix is owned by this pipeline. Manually replacing objects
requires investigation rather than automatic adjustment of accounting.
Legacy completed videos are scanned and acquire generation/artifact metadata.
Legacy sources infer completed discovery from their lifecycle state when the
new field is absent. SQLite adds the discovery-complete column on startup.

## Release sequence

Drain old worker executions before upgrading the master and workers together.
Old workers do not register upload intents. Orphaned objects from earlier
releases require a one-time inventory/accounting review. Run unit, race,
emulator, image, and Terraform checks; build immutable images; then follow the
deployment procedure. The infrastructure update adds master bucket metadata
access and the `GCS_BUCKET` setting.

Fixture tests validate behavior locally. A live GCP rollout must also verify
IAM, production indexes, and media-tool behavior with an authorized bounded source.
