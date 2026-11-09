# Mini Distributed Object Store

A small, S3-style distributed object store written in Go. Objects are split
into fixed-size chunks, each chunk is stored on **two different workers**, and
a single controller keeps the metadata, detects failed workers, and repairs
lost or corrupted data automatically. The whole system runs on one machine
with Docker Compose.

> **Implementation status: Phase 4 of 11 complete.** The cluster (1
> controller + 4 workers) runs under Docker Compose. Objects can be
> **uploaded** (1 MB chunks, each on two different workers, atomic commit) and
> **downloaded** (every chunk verified, failing over to the other copy; corrupt
> data is never returned). Metadata, list, and delete come next. See
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
| Streaming upload: 1 MB (1,000,000-byte) chunks, RF = 2 on distinct workers, atomic commit | 3 |
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
| Metadata | SQLite via pure-Go `modernc.org/sqlite` |
| Concurrency helpers | `golang.org/x/sync` (`errgroup`) |
| Logging | `log/slog` (structured) |
| Deployment | Docker + Docker Compose (from Phases 1–2) |
| Static analysis | `go vet`, `gofmt`, `staticcheck` (pinned in `go.mod`) |

Reasons for each choice: [`docs/DECISIONS.md`](docs/DECISIONS.md).

## Repository structure

```
cmd/
  controller/        controller binary
  worker/            worker binary (+ `worker inspect` debug tool)
  objctl/            client CLI (stub)
internal/
  ids/               object/chunk/worker ID generation and validation
  config/            environment-variable configuration
  wire/              shared JSON types, headers, error codes
  httpx/             JSON errors, request logging middleware
  controller/registry/ worker states, epochs, failure detector
  controller/api/    controller HTTP API (membership, upload, /healthz, /cluster)
  controller/meta/   SQLite metadata store (objects, chunks, replicas, pending deletes)
  controller/upload/ upload coordinator (chunking, placement, replication, commit)
  controller/download/ download coordinator (verify, fail over, stream in order)
  controller/placement/ replica placement policy
  controller/workerclient/ the controller's HTTP client for workers
  controller/janitor/ deferred cleanup of aborted uploads
  controller/custommeta/ custom metadata validation
  worker/store/      on-disk chunk store (atomic, checksummed)
  worker/server/     worker HTTP API and write gate
  worker/agent/      worker membership: register, heartbeat, re-register
  version/           build version string
test/
  integration/       in-process cluster tests; kill worker mid-write
  e2e/               Docker and Compose tests (build tag `e2e`)
Dockerfile           one image for all binaries
docker-compose.yml   controller + worker1..worker4, one volume each
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
- **Docker** with **Docker Compose v2** (Docker Desktop on Windows/macOS):
  runs the cluster, the e2e tests, and the race detector on Windows.
- **Git.**

## Quick start

Build and test:

```bash
go build ./...
go test ./...
```

Run the cluster (Git Bash / Linux / macOS):

```bash
docker compose up -d --build
curl -s localhost:8080/cluster        # 4 workers ALIVE
head -c 5500000 /dev/urandom > big.bin
curl -s -F 'metadata={"owner":"alice"}' -F 'file=@big.bin' localhost:8080/api/v1/objects
                                      # 201: 6 chunks, each on 2 workers
curl -s -o got.bin localhost:8080/api/v1/objects/<object_id> && cmp big.bin got.bin
docker stop worker2                   # ~10 s later: worker2 DEAD
docker start worker2                  # ALIVE again (re-registered)
docker compose down                   # stop; data volumes are kept (-v deletes them)
```

> If containers named `controller` or `worker1`…`worker4` already exist on
> your machine (another project), first run
> `export OBJSTORE_CONTAINER_PREFIX=objstore-` and use `objstore-worker2` etc.
> Never remove containers you did not create.

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
| 1 | Worker: durable chunk store over HTTP, in Docker | COMPLETED |
| 2 | Membership, failure detection, `SYNCING`, Compose cluster | COMPLETED |
| 3 | Write path: metadata DB, chunking, replication, atomic commit | COMPLETED |
| 4 | Read path: verified, in-order download with replica fallback | COMPLETED (awaiting review) |
| 5–11 | See [implementation plan](docs/IMPLEMENTATION_PLAN.md) | NOT STARTED |

Open item awaiting owner confirmation: string-only custom metadata values
(DEC-013). The chunk size is confirmed as 1 MB = 1,000,000 bytes (DEC-005).
