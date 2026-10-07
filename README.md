# Scrape-Tupe

A Go video-processing pipeline with a master HTTP API, metadata normalizers,
and bounded downloader workers. Production uses Firestore, Cloud Run Jobs,
and a private GCS bucket. SQLite is a development adapter.

## Local development

Requires Go 1.26.6 or newer. Workers additionally require yt-dlp 2026.8.19
with its default extras and Deno; downloading merged formats requires ffmpeg.

```sh
go run ./cmd/master -addr :8080 -db scrape-tupe.db
```

In another terminal, submit an authorized source:

```sh
curl -H 'Content-Type: application/json' http://localhost:8080/sources \
  -d '{"url":"https://www.youtube.com/watch?v=VIDEO_ID","kind":"video","authorization_acknowledged":true}'
MASTER_URL=http://localhost:8080 NORMALIZER_MODE=url go run ./cmd/normalizer
go run ./cmd/downloader -master http://localhost:8080 -id local-worker \
  -storage-backend local -local-storage-dir /tmp/scrape-tupe/artifacts -exit-when-empty
```

For channels or playlists, run `NORMALIZER_MODE=discovery` first, then run
URL normalization once per pending raw URL. Cloud Run performs that orchestration
through scheduled reconciliation in production. Local SQLite intentionally
does not enforce the production byte budget or reconcile GCS retention.

## Validation

```sh
make test
go test -race ./...
make test-firestore
make test-images
terraform -chdir=infra init -backend=false
terraform -chdir=infra fmt -check
terraform -chdir=infra validate
```

The integration suite uses isolated Firestore emulator projects and exercises
the real master/worker HTTP pipeline with fixture subprocesses and simulated
GCS metadata. No live media or GCP account is needed. Image smoke tests execute
production yt-dlp options against loopback fixtures and run Deno as the non-root
image user. `BUILD_SECRETS='--secret id=proxy_ca,src=/path/to/combined-ca.pem'`
supports build environments with an HTTPS proxy CA.

See [operations and recovery](docs/operations.md), [deployment](infra/README.md),
and the [review remediation plan](docs/review-fix-plan.md).
