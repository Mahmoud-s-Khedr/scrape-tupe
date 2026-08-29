# Scrape-Tupe — Build Plan

> Elastic video-download pipeline backed by **Bunny Magic Containers**, orchestrated by a central **master** service with **SQLite** as the single source of truth.

---

## Architecture Overview

```text
             INPUT
        channel / playlist / video
                 │
                 ▼
        ┌─────────────────┐
        │   Normalizer    │
        │     yt-dlp      │
        └────────┬────────┘
                 │
          discovered videos
                 ▼
        ┌─────────────────┐
        │     MASTER      │
        │                 │
        │ SQLite          │
        │ leases          │
        │ worker registry │
        │ scaler          │
        │ Bunny API       │
        └───────┬─────────┘
                │
        desired capacity N
                │
                ▼
        Bunny Magic Containers
        downloader application
                │
      ┌─────────┼─────────┐
      ▼         ▼         ▼
   Worker 1  Worker 2  Worker N
      │         │         │
      └──── claim jobs ───┘
                │
             yt-dlp
                │
                ▼
          Bunny Storage
                │
                ▼
        MASTER: completed
```

---

## Three Logical Components

### 1. Master

- Owns the **SQLite** database.
- Exposes an internal **HTTP API** for workers and normalizers.
- Tracks every source, video, and job.
- Manages **worker heartbeats** and **leases**.
- Controls Bunny Magic Containers through the **Bunny API** (deploy / undeploy / autoscaling `min`/`max` / inspect desired & active instances / recreate broken pods).

### 2. Normalizer

- Accepts channel, playlist, and direct-video URLs.
- Uses **yt-dlp** metadata / flat extraction to resolve them into unique individual video records.
- Inserts discovered videos into SQLite.
- Initially runs as a **single instance**.

### 3. Downloader

- Takes **exactly one video job** at a time.
- Downloads with **yt-dlp / ffmpeg**.
- Uploads the finished artifact to **Bunny Storage**.
- Reports completion, then immediately requests the next job.
- Replicas are **elastic** — scaled by the master.

---

## Data Model (SQLite)

### Core Tables

| Table     | Purpose                                                      |
| --------- | ------------------------------------------------------------ |
| `sources` | Channel / playlist / direct-video URLs submitted for processing |
| `videos`  | Individual video records, deduplicated by **YouTube ID**     |
| `workers` | Registered downloader pods with heartbeat tracking           |

### Video State Machine

```text
pending → leased → downloading → uploading → completed
                                            ↘ error (retryable)
```

### Key Video Columns

| Column          | Description                                          |
| --------------- | ---------------------------------------------------- |
| `youtube_id`    | **Unique key** — dedup by YouTube ID, not submitted URL |
| `state`         | Current lifecycle state (see above)                  |
| `worker_id`     | Pod currently holding the lease                      |
| `lease_until`   | Timestamp when the lease expires                     |
| `attempt_count` | Number of download attempts                          |
| `storage_path`  | Final path in Bunny Storage                          |
| `error`         | Last error message (if any)                          |

---

## Step-by-Step Plan

### Step 1 — Normalization as the Single Entry Point

All input goes through normalization:

1. Submit the test dataset: **1 channel + 2 playlists + 5 direct videos**.
2. The normalizer expands channels and playlists into individual video records.
3. Videos are inserted with `INSERT ... ON CONFLICT DO NOTHING` (dedup on `youtube_id`).
4. The source is marked `normalized`.
5. Direct video URLs simply become one normalized video each.
6. **Only after normalization** does a URL count toward downloader capacity.

### Step 2 — Pull-Based Job Distribution

Workers pull; the master **never pushes**.

1. Downloader starts and **registers** with the master using its Bunny pod ID.
2. Calls `POST /jobs/claim` — master atomically leases **one** pending video.
3. Worker **heartbeats** while yt-dlp is running.
4. Uploads finished artifact to Bunny Storage.
5. Calls `POST /jobs/{id}/complete`.
6. Immediately claims the next job.

### Step 3 — Lease-Based Fault Tolerance

Leases make worker death harmless:

- Each claimed job gets a **60–120 second renewable lease**.
- If a worker crashes, gets killed by Bunny, or loses network — heartbeats stop.
- Master detects the **expired lease** and resets the video to `pending`.
- A **different worker** retries the job.

> **IMPORTANT:** This is the key mechanism that prevents Bunny choosing the "wrong" pod during scale-down from corrupting the queue.

### Step 4 — Master as the Autoscaler

The master calculates parallelism from the **video table** — not from channels/playlists:

- If there are **N runnable video jobs** and capacity allows N, request **N downloader replicas**.
- Bunny API allows updating autoscaling `min` and `max` (both required, values ≥ 1).
- Master forces a target by setting `min=N, max=N`, subject to account/per-region limits.

### Step 5 — V1 Scaling Strategy: Right-Size to Pending Work

Bunny might terminate a busy pod instead of an idle one during scale-down. This is **safe** because:

1. **SIGTERM handler** (Step 7) releases the lease before exit — the job is immediately re-available.
2. **Lease expiry** (Step 3) catches hard crashes — the master resets the job to `pending` after 60–120 s.
3. The worst case is **one video gets re-downloaded**, which is far cheaper than paying for idle pods.

**Rules for V1:**

| Condition                                            | Action                                                                       |
| ---------------------------------------------------- | ---------------------------------------------------------------------------- |
| `pending_jobs > active_workers`                      | Scale **up** to `min(pending_jobs, max_capacity)`                            |
| `pending_jobs < active_workers`                      | Scale **down** to `max(pending_jobs, 1)` — a killed busy pod's job is auto-requeued |
| All videos in terminal state, nothing pending/running | **Undeploy** the entire downloader application                               |

Bunny's API explicitly defines `undeploy` as stopping all running instances.

### Step 6 — Batch Restart

When normalization later discovers new videos while downloaders are undeployed:

```text
videos pending > 0
  → calculate required capacity
  → deploy downloader app
  → set min=max=N
  → workers register
  → workers claim jobs
```

Bunny has specifically improved Magic Containers lifecycle speed for automated / short-lived workloads.

### Step 7 — Graceful Termination

Every downloader handles `SIGTERM`:

1. **Stop claiming** new jobs.
2. **Release the current lease** back to the master (if possible).
3. Exit cleanly.

> **NOTE:** Lease expiry remains the fallback for hard crashes. This design means aggressive scale-down can be safely added later — correctness is already protected.

### Step 8 — Temporary Download Directories

Downloads use per-job temporary directories:

```
/tmp/jobs/<job-id>/
```

1. yt-dlp downloads there.
2. ffmpeg merges there.
3. Final file uploads to Bunny Storage.
4. Directory is deleted.

> **TIP:** Magic Containers are intended for worker/automation workloads, but the container runtime has finite local resources. Cleaning up promptly avoids disk pressure.

### Step 9 — Master Reconciliation Loop

Every few seconds the master runs:

```text
1. Recover expired leases
2. Count remaining work
3. Inspect registered workers
4. Inspect Bunny application state (activeInstances vs desiredInstances)
5. Scale up if useful
6. Undeploy if absolutely no work remains
```

The Bunny overview API reports both `activeInstances` and `desiredInstances`, so the master does not have to assume that requesting 8 means 8 are already running.

---

## Build Order

| Phase       | Deliverable                                   | Key Milestone                                         |
| ----------- | --------------------------------------------- | ----------------------------------------------------- |
| **Phase 1** | SQLite schema + Master HTTP API               | Master starts, serves endpoints, reads/writes DB      |
| **Phase 2** | Single local downloader with yt-dlp           | One video downloaded end-to-end locally                |
| **Phase 3** | Bunny Storage upload integration              | Downloaded video lands in Bunny Storage                |
| **Phase 4** | Normalizer (channel/playlist/video expansion) | Test dataset fully expanded into `videos` table        |
| **Phase 5** | Leases & heartbeats                           | Simulated crash → lease expires → job re-queued        |
| **Phase 6** | Dockerize the downloader                      | Container image runs and completes one job             |
| **Phase 7** | Deploy one Bunny worker                       | Single Magic Container replica processes jobs          |
| **Phase 8** | Test 2–3 replicas manually                    | Multiple workers drain the queue in parallel           |
| **Phase 9** | Connect Bunny API to master                   | Master reads `activeInstances` / `desiredInstances`    |
| **Phase 10**| Automatic scale-up / undeploy logic           | Full autoscaling loop operational                      |

---

## First Milestone (End-to-End Demo)

> Submit **1 channel + 2 playlists + 5 direct videos**. Watch:
>
> 1. Normalization populate SQLite with all discovered videos.
> 2. Master automatically deploy the appropriate number of Bunny downloader replicas.
> 3. Workers drain the entire queue.
> 4. All artifacts uploaded to Bunny Storage.
> 5. Master undeploy the downloader application.
>
> **This single demo proves essentially the whole architecture.**

---

## References

- [How we sped up deploys and updates in Magic Containers](https://bunny.net/blog/how-we-sped-up-deploys-updates-and-undeploys-in-magic-containers/) — Bunny lifecycle improvements
- [Magic Containers](https://bunny.net/magic-containers/) — Product overview and runtime constraints
