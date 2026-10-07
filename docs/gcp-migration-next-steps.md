# GCP migration status and next steps

Firestore transactions, discovery/normalization workers, Cloud Run execution
guards, GCS uploads, and Terraform resources are implemented. SQLite remains
the development adapter and is not suitable for production Cloud Run.

The review fixes add compatible worker tools, bounded queue scanning,
idempotent discovery replay, normalizer lease renewal, subprocess diagnostics,
and retention accounting with durable upload intents.
See [operations](operations.md) for behavior and upgrade requirements.

Before production rollout:

1. Run unit/race tests, the Firestore emulator suite, image smoke checks, and
   Terraform formatting/validation.
2. Build and push immutable image digests, then update Terraform inputs.
3. Drain old executions and upgrade the master and workers together, including
   the master's GCS metadata permission.
4. Review legacy orphaned objects and validate one authorized bounded end-to-end
   batch in GCP, including ownership recovery and retention accounting.

GCS uploader IAM needs `storage.objects.create` and `storage.objects.get` for
immutable creation and retry integrity checks. Master accounting needs only
`storage.objects.get`. Both grants are restricted to the artifact `videos/`
prefix; workers continue accessing Firestore exclusively through the master.
