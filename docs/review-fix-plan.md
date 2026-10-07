# Review remediation plan

Scope: the seven findings from the review of commit `3b67caa`. This plan does not authorize a production deployment. Implement and validate the changes locally before release.

## Implementation status

All seven fixes are implemented locally. Regression coverage includes bounded
queue scans beyond blocked prefixes, readiness/claim coordination, concurrent
reservations, replay and legacy-source retries, normalizer and downloader lease
expiry, subprocess diagnostics/cancellation, upload intents, orphan adoption,
and idempotent artifact expiry. A fixture pipeline exercises the actual HTTP
API and Firestore emulator end to end.

Validation passed: `go test ./...`, `go test -race ./...`, `go vet ./...`,
Firestore emulator tests (including the HTTP pipeline), container builds and
non-root yt-dlp/Deno smoke checks, and Terraform formatting/validation.
No live GCP deployment or media download was performed. See
[operations and recovery](operations.md) for required bucket permissions,
settlement behavior, and the coordinated worker/master upgrade.

## 1. Restore worker compatibility with yt-dlp

- Update the pinned yt-dlp version in both worker Dockerfiles to a tested release supporting `--js-runtimes deno`.
- Keep both images on the same version. Verify Deno can run under the configured non-root user.
- Extend image smoke checks to execute the actual option set against a local fixture, rather than relying on `--version`, which exits before detecting unsupported options.
- Add a subprocess seam or fake executable for normalizer and downloader regression tests without live YouTube access.

Acceptance: both images accept the production arguments; discovery, metadata extraction, and downloading exercise their command paths successfully with fixtures.

## 2. Prevent unsuitable videos from blocking claims

- Select an eligible pending video rather than rejecting the entire claim after inspecting the oldest record.
- Paginate candidates so unsuitable records beyond the first query window cannot hide eligible work. Bound each request's reads and preserve scan progress across requests when necessary.
- Define explicit size policy: calculate selected-format size from metadata where possible; otherwise reserve a configurable conservative maximum and enforce it during download. Permanently oversized videos enter a terminal state with an actionable reason; temporarily capacity-constrained videos stay deferred.
- Keep candidate ownership and capacity reservation in one transaction. Never reserve more than the remaining budget.
- Distinguish capacity waiting from an empty queue. Bounded downloader tasks should exit or use a bounded retry interval when no work fits, and reconciliation should avoid repeatedly launching batches that cannot claim work.
- Preserve the local adapter contract and document any deliberate development-only differences.

Acceptance: a zero-size or oversized oldest video cannot block smaller eligible records; candidates beyond the first page remain reachable; concurrent claims cannot exceed the budget; capacity waiting does not consume an entire job timeout.

## 3. Make discovery replay idempotent

- Treat the discovery cursor as progress information, rather than assigning its saved value to the first URL of a fresh extraction.
- Replay extraction from the beginning and deduplicate checkpoints using a deterministic source-and-canonical-URL key. This avoids relying on playlist order remaining unchanged between attempts.
- Increment discovered counts and raw-URL counters only when inserting a genuinely new record. A repeated checkpoint must succeed without another insert.
- Preserve existing raw records. Introduce a checkpoint lookup compatible with their opaque IDs, or backfill deterministic lookup records before switching behavior.
- Ensure completion derives the correct aggregate even for empty sources or an entirely replayed discovery.

Acceptance: crash after A/B, replay A/B/C, and obtain exactly A/B/C once; repeated HTTP checkpoints and concurrent retries do not duplicate records or counters; reordered input does not duplicate existing URLs.

## 4. Restore scheduling when discovery is retried

- In `RetrySource`, atomically increment `discoverable_sources` when a terminal channel or playlist source is moved back to `queued`.
- Clear obsolete retry/cooldown information and verify that direct-video sources instead requeue normalization work.
- Keep repeated retry requests from counting the same transition twice.
- Review the analogous local adapter transitions.

Acceptance: a quarantined discovery source with no quarantined raw records restarts through reconciliation from an otherwise empty queue; repeated or concurrent retries preserve correct counts.

## 5. Renew normalizer leases and stop stale workers

- Add discovery and raw-URL renewal operations to the store contract, both adapters, and worker-authenticated HTTP routes.
- Renew periodically well before expiry, independent of checkpoint traffic.
- Use cancellable task contexts and bounded HTTP request deadlines. Cancel yt-dlp when ownership is rejected or a successful renewal cannot be obtained before the locally tracked lease deadline.
- Stop renewal goroutines on every exit path and reap subprocesses after cancellation or checkpoint failure.
- Validate state, worker identity, and unexpired lease consistently for completion, renewal, and failure mutations.

Acceptance: discovery and metadata extraction can run beyond the original lease while renewing; a stale worker cannot write checkpoints or results; cancellation leaves no subprocess or renewal goroutine running.

## 6. Preserve yt-dlp diagnostics for classification

- Capture bounded stderr for downloader, discovery, and metadata subprocesses, while retaining useful structured logs.
- Return a typed subprocess error containing exit status and captured diagnostic text; classify that diagnostic text explicitly.
- Propagate failed quarantine/release callbacks rather than silently reporting success.
- Keep cancellation, lost leases, rate limiting, blocked content, and generic failures distinct.

Acceptance: fake subprocesses emitting HTTP 429 or blocked-content messages produce the expected quarantine transitions; generic failures retain diagnostics; callback failures remain visible.

## 7. Reconcile artifact retention and storage accounting

- Record object identity, generation, size, and accounted lifecycle state for each completed artifact.
- Add scheduled, bounded reconciliation against GCS object metadata. Only a confirmed not-found response establishes deletion; permission errors, timeouts, and other failures must not release capacity.
- Transactionally mark a previously accounted artifact expired and decrement its size exactly once, checking that object identity/generation still matches the inspected record.
- Expose expired artifact status so completed records do not promise an available storage path after retention deletion.
- Grant the reconciliation identity only the required artifact metadata access. Document the possible delay between bucket lifecycle deletion and budget recovery.
- Define explicit treatment for uploaded-but-uncommitted objects so they remain discoverable and cannot cause physical storage to exceed the intended budget unnoticed.

Acceptance: confirmed deletion restores capacity once; repeated/concurrent reconciliation is idempotent; metadata failures retain the reservation/accounting; replacement objects and uploads in progress are not mistaken for deleted artifacts.

## Delivery order and release checks

1. Worker compatibility and subprocess diagnostics: issues 1 and 6.
2. Queue selection and retry scheduling: issues 2 and 4.
3. Discovery replay and normalizer lease lifecycle: issues 3 and 5.
4. Artifact retention accounting: issue 7.

For each change, add focused regression coverage for the failure and preserve existing tests. Run `go test ./...`; run Firestore emulator tests for transactional changes; run all three image smoke checks for the final worker arguments; run Terraform formatting and validation for infrastructure changes. Update deployment and migration documentation to match implemented behavior.

Finish with a local integration scenario using fixture subprocesses: submit a source, interrupt and resume discovery, normalize URLs, claim around an unsuitable video, renew leases, upload, complete, simulate deletion, and verify recovered capacity. Verify counter consistency and duplicate-free records throughout. Live GCP deployment and real media downloads are separate release actions.
