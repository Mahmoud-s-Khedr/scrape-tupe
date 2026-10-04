.PHONY: test test-firestore test-images

test:
	go test ./...

# Runs only local emulator tests. No GCP project, credentials, deployment, or
# media download is involved. The trap cleans up containers and networks even
# when startup, readiness, or the test itself fails.
test-firestore:
	@set -eu; \
		cleanup() { docker compose -f docker-compose.firestore.yml down -v >/dev/null 2>&1 || true; }; \
		trap cleanup EXIT INT TERM; \
		docker compose -f docker-compose.firestore.yml up -d firestore; \
		ready=0; \
		for attempt in $$(seq 1 30); do \
			if curl -fs -o /dev/null http://127.0.0.1:8080; then ready=1; break; fi; \
			sleep 1; \
		done; \
		if [ "$$ready" -ne 1 ]; then docker compose -f docker-compose.firestore.yml logs firestore >&2; exit 1; fi; \
		FIRESTORE_EMULATOR_HOST=127.0.0.1:8080 go test -tags=integration ./internal/firestore

# BUILD_SECRETS may be set to e.g. "--secret id=proxy_ca,src=/path/to/ca.pem"
# in managed environments that require a session proxy CA.
test-images:
	docker build $(BUILD_SECRETS) -f Dockerfile.master -t scrape-tupe-master:smoke .
	docker build $(BUILD_SECRETS) -f Dockerfile.normalizer -t scrape-tupe-normalizer:smoke .
	docker build $(BUILD_SECRETS) -f Dockerfile.downloader -t scrape-tupe-downloader:smoke .
	test "$$(docker image inspect -f '{{.Config.User}}' scrape-tupe-master:smoke)" = "nonroot:nonroot"
	docker run --rm --entrypoint deno scrape-tupe-normalizer:smoke --version
	docker run --rm --entrypoint yt-dlp scrape-tupe-normalizer:smoke --version
	docker run --rm --entrypoint deno scrape-tupe-downloader:smoke --version
	docker run --rm --entrypoint yt-dlp scrape-tupe-downloader:smoke --version
	docker run --rm --entrypoint ffmpeg scrape-tupe-downloader:smoke -version
