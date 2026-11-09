# Mini Distributed Object Store

A small, S3-style distributed object store written in Go. Objects are split
into fixed-size chunks, each chunk is stored on **two different workers**, and
a single controller keeps the metadata, detects failed workers, and repairs
lost or corrupted data automatically. The whole system runs on one machine
with Docker Compose.

> **Implementation status: Phase 1 of 11 complete.** The storage **worker**
> works as a standalone Docker container (durable, atomic, checksummed chunk
> storage over HTTP). The controller and client API come next. See
> [Current status](#current-status).

## The problem

One machine has limited disk and is a single point of failure. Spreading data
over several machines fixes both, but creates new problems: where each piece
lives, noticing dead machines, restoring lost copies, and detecting silent
corruption. This project implements the standard answers (chunking,
replication, heartbeats, self-healing, checksums) in a system small enough to
understand completely: the same ideas behind Amazon S3, HDFS, and GFS.

## Architecture at a glance

```
 client ──REST/JSON + binary──▶ controller (:8080) ──chunk PUT/GET/DELETE──▶ worker1..worker4
                                  │   ▲                                        │
                                  │   └──────────── register / heartbeat ──────┘
                                  └── SQLite metadata DB (persistent volume)
```

- **Controller** (one): client REST API, metadata (source of truth), worker
  liveness, placement, background repair and cleanup. Clients never see worker
  addresses.
- **Workers** (four): store, serve, and delete chunk files on their own
  persistent volume; register and heartbeat; scrub their own data. They never
  talk to each other.
- **Clients** (one or more): any HTTP client, plus the `objctl` CLI.

Details: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Full original design
with rationale: [`docs/reference/Object_Store_Architecture.md`](docs/reference/Object_Store_Architecture.md).

## Key features (target)

| Feature | Phase |
| --- | --- |
| Durable, atomic, checksummed chunk storage on workers | 1 |
| Worker registration, heartbeats, failure detection | 2 |
| Streaming upload: 1 MiB chunks, RF = 2 on distinct workers, atomic commit | 3 |
| Streaming download with per-chunk verification and replica fallback | 4 |
| Metadata GET/PATCH, list with pagination, delete | 5 |
| Correct operation with a worker down; inventory resync on return | 6 |
| Automatic re-replication after a worker failure | 7 |
| Corruption detection (read path, scrubber) and automatic repair | 8 |
| Crash-consistent cleanup after controller/worker/client crashes | 9 |

## Technology stack

| Concern | Choice |
| --- | --- |
| Language | Go 1.27 |
| HTTP | Go standard library (`net/http`, method-pattern routing) |
| Metadata | SQLite via pure-Go `modernc.org/sqlite` (from Phase 3) |
| Concurrency helpers | `golang.org/x/sync` (from Phase 3) |
| Logging | `log/slog` (structured) |
| Deployment | Docker + Docker Compose (from Phases 1–2) |
| Static analysis | `go vet`, `gofmt`, `staticcheck` (pinned in `go.mod`) |

Reasons for each choice: [`docs/DECISIONS.md`](docs/DECISIONS.md).

## Repository structure

```
cmd/
  controller/        controller binary (stub)
  worker/            worker binary (+ `worker inspect` debug tool)
  objctl/            client CLI (stub)
internal/
  ids/               object/chunk/worker ID generation and validation
  config/            environment-variable configuration
  wire/              shared JSON types, headers, error codes
  httpx/             JSON errors, request logging middleware
  worker/store/      on-disk chunk store (atomic, checksummed)
  worker/server/     worker HTTP API
  version/           build version string
test/
  integration/       multi-process tests (kill worker mid-write)
  e2e/               Docker tests (build tag `e2e`)
Dockerfile           one image for all binaries
docs/
  ARCHITECTURE.md        system as implemented, deviations from the design
  IMPLEMENTATION_PLAN.md phase roadmap and status
  DEVELOPMENT_LOG.md     chronological record of each phase
  DECISIONS.md           implementation decision log (DEC-nnn)
  RUN_AND_TEST.md        how to build, run, and verify everything
  API.md                 every external and internal endpoint
  reference/             original design document and project specification
.env.example         configuration contract (all variables and defaults)
```

## Prerequisites

- **Go 1.27.x** (`go version`). On Windows: `winget install GoLang.Go`.
- **Docker** with **Docker Compose v2** (Docker Desktop on Windows/macOS).
  Required for the race detector on Windows now, and for running the cluster
  from Phase 1.
- **Git.**

## Quick start

Build and test:

```bash
go build ./...
go test ./...
```

Run a standalone worker in Docker (Git Bash / Linux / macOS):

```bash
docker build -t objstore:dev .
docker volume create objstore-w1-data
docker run -d --name objstore-w1 -e WORKER_ID=worker1 -v objstore-w1-data:/data -p 127.0.0.1:19001:9000 objstore:dev
curl -s http://127.0.0.1:19001/healthz
```

Store, fetch, corrupt, and inspect chunks: see
[`docs/RUN_AND_TEST.md`](docs/RUN_AND_TEST.md) §3–5.

Full, verified instructions: [`docs/RUN_AND_TEST.md`](docs/RUN_AND_TEST.md).

## Configuration

All settings are environment variables with defaults; see
[`.env.example`](.env.example) for the full list (chunk size, replication
factor, heartbeat/dead/grace timings, repair concurrency, upload/download
windows, timeouts, fsync). Each variable takes effect once its phase is
implemented.

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [API reference](docs/API.md)
- [Run and test guide](docs/RUN_AND_TEST.md)
- [Implementation plan](docs/IMPLEMENTATION_PLAN.md)
- [Development log](docs/DEVELOPMENT_LOG.md)
- [Decision log](docs/DECISIONS.md)
- [Project specification](docs/reference/Mini_Distributed_Cloud_Object_Store.pdf)
  (text version: [`.txt`](docs/reference/Mini_Distributed_Cloud_Object_Store.txt))

## Current status

| Phase | Theme | Status |
| --- | --- | --- |
| 0 | Setup and design lock-in | COMPLETED |
| 1 | Worker: durable chunk store over HTTP, in Docker | COMPLETED (awaiting review) |
| 2–11 | See [implementation plan](docs/IMPLEMENTATION_PLAN.md) | NOT STARTED |

Open items awaiting owner confirmation: chunk size definition (DEC-005,
provisionally 1 MiB) and string-only custom metadata values (DEC-013).
