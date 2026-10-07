# GCP release

This module deploys only immutable image references. Build, test, and push the
three images manually, then obtain their Artifact Registry digest references:

```bash
docker build -f Dockerfile.master -t "$REGISTRY/master:$TAG" . && docker push "$REGISTRY/master:$TAG"
docker build -f Dockerfile.normalizer -t "$REGISTRY/normalizer:$TAG" . && docker push "$REGISTRY/normalizer:$TAG"
docker build -f Dockerfile.downloader -t "$REGISTRY/downloader:$TAG" . && docker push "$REGISTRY/downloader:$TAG"
terraform -chdir=infra init
terraform -chdir=infra apply -var master_image="$REGISTRY/master@sha256:..." -var normalizer_image="$REGISTRY/normalizer@sha256:..." -var downloader_image="$REGISTRY/downloader@sha256:..."
```

Run `terraform fmt -check` and `terraform validate` before apply. The USD 1
budget is an alert only; it does not stop resource usage. Downloader jobs have
10 tasks/parallelism and a bounded timeout. Cloud Run ephemeral storage is
configured by the platform default (10 GiB); verify the relevant project quota
before enabling a production batch of ten concurrent tasks.

The master requires the artifact bucket name and metadata-read permission for
retention accounting; this module supplies both. Drain old worker executions
before upgrading: current GCS workers register upload intents through the master
before creating objects. See [operations and recovery](../docs/operations.md)
for the settlement period, expired artifact status, and legacy-object review.

The deployment keeps Cloud Run's default dynamic outbound egress. It does not
promise one public IP per task and deliberately contains no IP rotation logic.
If a legitimate integration later needs static egress, add Direct VPC egress
and Cloud NAT as a separate reviewed change.
