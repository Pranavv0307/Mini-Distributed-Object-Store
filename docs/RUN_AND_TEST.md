# Run and Test Guide

Practical, step-by-step instructions to set up, run, and verify the project.

> **Rule for this document:** every command here was run and its result
> observed at the phase noted. Sections for later phases are marked
> *not yet available* until those phases are implemented.

**Current state (end of Phase 2):** the cluster runs under Docker Compose:
one controller and four workers. Workers register (with their inventory),
heartbeat, and are marked `DEAD` when they stop; `/cluster` shows it. There
is no client object API yet (Phase 3).

> ⚠️ **Docker name collisions.** Container names are global on a Docker host.
> The Compose file uses the spec's names `controller` and `worker1`…`worker4`.
> If containers with those names already exist (from another project),
> `docker compose up` fails with a "Conflict" error. **Do not remove containers
> you did not create.** Instead give this project's containers a prefix
> (DEC-032):
>
> ```bash
> export OBJSTORE_CONTAINER_PREFIX=objstore-
> ```
>
> and use `objstore-worker2` etc. in the commands below. All commands in this
> document were verified with that prefix set. Without a prefix, drop
> `objstore-` from container names (e.g. `docker stop worker2`).

---

## 1. Environment setup

### Required software

| Software | Version verified | Needed for |
| --- | --- | --- |
| Go | 1.27.3 (any 1.27.x; `go.mod` requires ≥ 1.27.0) | Building and testing |
| Docker Engine | 29.9.2 (Docker Desktop on Windows) | Image, cluster, e2e tests, race detector on Windows |
| Docker Compose | v5.6.1 (`docker compose`) | Running the cluster |
| Git (and Git Bash on Windows) | 2.55 | Source control; the shell commands below |
| curl, sha256sum, grep | bundled with Git Bash | Manual API checks |

The shell examples are **bash** (Git Bash on Windows, or Linux/macOS). On
Windows, run them in Git Bash, not PowerShell. In Git Bash also run
`export MSYS_NO_PATHCONV=1`, so `docker exec … /data/...` paths are not
rewritten. `jq` is not required; the examples use `grep`.

### Installing Go

- **Windows:** `winget install GoLang.Go` (installs to `C:\Program Files\Go`).
  **Open a new terminal afterwards.** Check with `go version`.
- **macOS:** `brew install go`. **Linux:** see <https://go.dev/doc/install>.

### Docker

Start Docker Desktop and wait until `docker info` prints server information.

### Dependencies

```bash
go mod download
```

---

## 2. Building

```bash
go build ./...                 # compile everything (no output = success)
go build -o bin/ ./cmd/...     # binaries into bin/ (git-ignored)
docker build -t objstore:dev . # the image Compose uses (DEC-023)
```

`bin/controller` and `bin/worker` are functional; `bin/objctl` is still a
stub. `docker compose up --build` (below) also builds the image.

---

## 3. Running the cluster (Docker Compose)

### Configuration

All variables are optional (defaults shown). Set them in the shell or in a
`.env` file next to `docker-compose.yml` (see `.env.example`).

| Variable | Applies to | Default | Meaning |
| --- | --- | --- | --- |
| `OBJSTORE_CONTAINER_PREFIX` | Compose | *(empty)* | Prefix for container names (DEC-032) |
| `CONTROLLER_PORT` | Compose | `8080` | Host port (bound to 127.0.0.1) for the controller |
| `OBJSTORE_IMAGE` | Compose | `objstore:dev` | Image for all services |
| `HEARTBEAT_INTERVAL` | controller | `3s` | Heartbeat period, sent to workers at registration |
| `DEAD_AFTER` | controller | `10s` | Silence after which a worker is `DEAD`; must exceed `HEARTBEAT_INTERVAL` |
| `REPAIR_GRACE` | controller | `30s` | `DEAD` for longer → `repair_eligible` (repair starts in Phase 7) |
| `FAILURE_CHECK_INTERVAL` | controller | `1s` | How often the failure detector runs |
| `CHUNK_SIZE` | workers | `1000000` | Max chunk payload: 1 MB (DEC-005) |
| `FSYNC_ON_WRITE` | workers | `true` | fsync file and directory on write/delete |
| `MAX_CONCURRENT_READS` | workers | `16` | Chunk GETs held in memory at once |
| `LOG_LEVEL` | all | `info` | `debug`, `info`, `warn`, `error` |

Fixed per service in `docker-compose.yml`: `WORKER_ID=workerN`,
`ADVERTISE_ADDR=http://workerN:9000`, `CONTROLLER_URL=http://controller:8080`,
`DATA_DIR=/data` (one named volume per service).

### Start

```bash
export OBJSTORE_CONTAINER_PREFIX=objstore-   # see the warning at the top
docker compose up -d --build
docker compose ps
```

Expected: five containers `Up` (health `starting`, then `healthy`). Startup
order does not matter: workers retry registration until the controller
answers.

### Health and cluster state

```bash
curl -s localhost:8080/healthz
curl -s localhost:8080/cluster
```

Expected: `{"status":"ok"}`, and a cluster view with
`"summary":{"total":4,"alive":4,"syncing":0,"dead":0}` (observed within about
1 s of `up`). Compact per-worker view:

```bash
curl -s localhost:8080/cluster | grep -o '"summary":{[^}]*}'
curl -s localhost:8080/cluster | grep -o '"worker_id":"worker[0-9]","address":"[^"]*","state":"[A-Z]*"'
```

Field meanings: [`API.md`](API.md), section `GET /cluster`.

### Logs

```bash
docker logs objstore-controller
docker logs objstore-worker2
docker compose logs -f            # all services, follow
```

JSON lines. Useful events: controller `controller_started`, `worker_syncing`,
`worker_alive`, `worker_dead`, `worker_reregister_requested`,
`worker_registration_superseded`, `inventory_reconciled`; worker
`worker_started` (with `mode`), `worker_registered`, `worker_reregistering`,
`worker_register_failed`, `heartbeat_failed`, `heartbeat_recovered`,
`chunk_stored`.

### Stop, keep data, reset

```bash
docker compose down        # remove containers; volumes (data) are KEPT
docker compose up -d       # start again with the same data
docker compose down -v     # remove containers AND this project's volumes (factory reset)
```

`down -v` removes only the `objstore` project's volumes
(`objstore_controller-data`, `objstore_worker1-data`, …).

### Running components individually

**Standalone worker** (no controller; writes accepted at once, DEC-026):

```bash
docker volume create objstore-w1-data
docker run -d --name objstore-w1 -e WORKER_ID=worker1 \
  -v objstore-w1-data:/data -p 127.0.0.1:19001:9000 objstore:dev
curl -s http://127.0.0.1:19001/healthz
```

Expected: `{"status":"ok","worker_id":"worker1","registered":false,"accepting_writes":true}`.
Clean up: `docker rm -f objstore-w1 && docker volume rm objstore-w1-data`.

**Native controller and worker** (no Docker; two terminals):

```bash
go build -o bin/ ./cmd/...
LISTEN_ADDR=127.0.0.1:18080 ./bin/controller
```

```bash
WORKER_ID=worker1 LISTEN_ADDR=127.0.0.1:19001 DATA_DIR=./data/worker1 \
  CONTROLLER_URL=http://127.0.0.1:18080 ADVERTISE_ADDR=http://127.0.0.1:19001 ./bin/worker
```

Then `curl -s localhost:18080/cluster` shows worker1 `ALIVE`, and the worker's
`/healthz` shows `"registered":true,"accepting_writes":true`. `ADVERTISE_ADDR`
is required natively because the default (`http://worker1:9000`) is the
Compose DNS name. Stop each with Ctrl-C; `bin/` and `data/` are git-ignored.

---

## 4. Functional testing

### 4.1 Membership (verified at Phase 2)

With the cluster running (§3):

1. `curl -s localhost:8080/cluster | grep -o '"summary":{[^}]*}'` →
   `"summary":{"total":4,"alive":4,"syncing":0,"dead":0}`.
2. Each worker entry has `"address":"http://workerN:9000"`, a large `epoch`
   (≈ 1.8 × 10¹⁸, DEC-029), `last_heartbeat_age_ms` below 3000, and
   `"dead_since":null`.
3. The controller log has `worker_syncing` and `worker_alive` for each
   worker; each worker log has `worker_registered`.

### 4.2 Worker chunk API (verified at Phase 1/2, standalone worker)

Workers are not published to the host in the cluster, so exercise the chunk
API on a standalone worker (§3):

```bash
W=http://127.0.0.1:19001
head -c 1000000 /dev/urandom > chunk.bin
SUM=$(sha256sum chunk.bin | cut -d' ' -f1)
ID=3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000
ZERO=0000000000000000000000000000000000000000000000000000000000000000
```

| Step | Command | Expected |
| --- | --- | --- |
| Store | `curl -s -w ' HTTP %{http_code}\n' -X PUT -H "X-Chunk-SHA256: $SUM" --data-binary @chunk.bin $W/internal/chunks/$ID` | `201`, `"created":true` |
| Store again | same command | `200`, `"created":false` (idempotent) |
| Fetch | `curl -s -o fetched.bin $W/internal/chunks/$ID && sha256sum chunk.bin fetched.bin` | identical hashes |
| Wrong checksum | `curl -s -w ' HTTP %{http_code}\n' -X PUT -H "X-Chunk-SHA256: $ZERO" --data-binary @chunk.bin $W/internal/chunks/${ID%_000000}_000001` | `400`, `CHECKSUM_MISMATCH` |
| Traversal | `curl -s -w ' HTTP %{http_code}\n' -X PUT -H "X-Chunk-SHA256: $SUM" --data-binary @chunk.bin "$W/internal/chunks/..%2Fescape"` | `400`, `INVALID_CHUNK_ID` |
| Inventory | `curl -s $W/internal/workers/worker1/chunks` | one chunk, `"size":1000000` |
| Files | `docker exec objstore-w1 ls -l /data/chunks /data/tmp` | one 1000048-byte file (48-byte header); `tmp` empty |
| Inspect | `docker exec objstore-w1 worker inspect /data/chunks/$ID` | `status: VALID`, exit 0 |
| Delete twice | `curl -s -o /dev/null -w '%{http_code}\n' -X DELETE $W/internal/chunks/$ID` | `204`, then `204` |
| Fetch deleted | `curl -s -w ' HTTP %{http_code}\n' $W/internal/chunks/$ID` | `404`, `CHUNK_NOT_FOUND` |

Corruption on one worker (before deleting):
`docker exec objstore-w1 sh -c "printf garbage >> /data/chunks/$ID"` →
`worker inspect` reports `status: CORRUPT (payload is 1000007 bytes, header
says 1000000; …)` with exit code 1; the inventory reports `"size":1000007`;
GET returns 1000001 bytes (capped at `CHUNK_SIZE + 1`, DEC-025). The worker
never judges validity; the controller will (Phase 4).

### 4.3 Client object API

*Not yet available* (Phases 3–5).

---

## 5. Failure testing

### 5.1 Stop and restart a worker (verified at Phase 2, default timings)

```bash
docker stop objstore-worker2
curl -s localhost:8080/cluster | grep -o '"worker_id":"worker2"[^}]*}'
```

Until `DEAD_AFTER` (10 s) passes, worker2 is still `ALIVE` with a growing
`last_heartbeat_age_ms`. About 10 s after the stop (observed: 10 s), it is
`"state":"DEAD"` with a `dead_since` timestamp; the summary shows
`"alive":3,"dead":1`; the other three stay `ALIVE`. Once `REPAIR_GRACE`
(30 s) more has passed, `"repair_eligible":true`. The controller logs
`worker_dead` with the measured `silence`.

```bash
docker start objstore-worker2
```

Within a second or two (observed: 1 s): worker2 is `ALIVE` with a **new,
larger `epoch`** and `"dead_since":null`. Its log shows
`worker_registered`; the controller log shows `worker_syncing` then
`worker_alive`. Data on its volume is untouched (it re-registered with its
inventory).

### 5.2 Controller restart (verified at Phase 2)

```bash
docker restart objstore-controller
curl -s localhost:8080/cluster | grep -o '"summary":{[^}]*}'
```

The restarted controller has an empty registry; each worker's next heartbeat
is answered `reregister` (controller log `worker_reregister_requested` with
`"reason":"unknown_worker"`), and all four return to `ALIVE` with new epochs
without anything being done to the workers (observed: 4 `ALIVE` within 2 s).
While the controller is down, workers keep running and retry.

### 5.3 Unavailable workers

`docker stop` is the supported way to make a worker unavailable; stopping
several workers shows each going `DEAD` independently. Network-partition
simulation (`docker network disconnect`) and multi-worker data scenarios
arrive with the data path (Phases 6–9).

### 5.4 Corruption, repair, crash consistency

Single-worker corruption visibility: §4.2. Controller-side detection and
repair: *not yet available* (Phases 4, 7–9). Crash mid-write on a worker:
covered by automated tests (§6).

---

## 6. Automated testing and static analysis

Run from the repository root. All verified at Phase 2.

### Unit and integration tests

```bash
go test ./...
```

Expected (all `ok`; `cmd/*` and `internal/wire` print `[no test files]`):

```
ok  	objstore/internal/config
ok  	objstore/internal/controller/api
ok  	objstore/internal/controller/registry
ok  	objstore/internal/httpx
ok  	objstore/internal/ids
ok  	objstore/internal/version
ok  	objstore/internal/worker/agent
ok  	objstore/internal/worker/server
ok  	objstore/internal/worker/store
ok  	objstore/test/integration
```

`test/integration` runs an in-process cluster (controller + 4 workers with
50 ms heartbeats and a 300 ms `DEAD_AFTER`) and builds and kills the real
worker binary; about 5–20 s. `go test -short ./...` skips the timing-based and
process-spawning tests. Add `-count=1` to bypass the cache, `-v` for detail.

### End-to-end tests (Docker)

```bash
go test -tags e2e -count=1 -v ./test/e2e/...
```

Builds `objstore:e2e` once, then runs:

- `TestComposeClusterMembership`: the Compose stack under a **unique project
  name, container prefix (`objstore-e2e-<n>-`), and free host port**, with
  `HEARTBEAT_INTERVAL=1s`, `DEAD_AFTER=4s`, `REPAIR_GRACE=6s`. Checks: 4
  `ALIVE`; `docker stop` worker2 → `DEAD` (others `ALIVE`) → repair-eligible;
  `docker start` → `ALIVE` with a newer epoch; `docker restart` controller →
  all four re-register with newer epochs. Removes only its own project
  (`compose down -v`).
- `TestWorkerContainer`: the standalone worker container checks from Phase 1.

Expected: both `PASS` (about 1 minute including the image build). Set
`E2E_IMAGE=objstore:dev` to reuse an existing image.

### Race detector

On Linux/macOS with a C toolchain: `go test -race ./...`. On **Windows**, run
it in the Go container (DEC-018):

PowerShell:

```powershell
docker run --rm -v "${PWD}:/src:ro" -v objstore-gomod:/go/pkg/mod -v objstore-gocache:/root/.cache/go-build -w /src golang:1.27.3 go test -race -count=1 ./...
```

Git Bash:

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W):/src:ro" -v objstore-gomod:/go/pkg/mod -v objstore-gocache:/root/.cache/go-build -w /src golang:1.27.3 go test -race -count=1 ./...
```

Expected: the same `ok` lines.

### Static analysis

```bash
go vet ./... && go vet -tags e2e ./...
gofmt -l .
go tool staticcheck ./... && go tool staticcheck -tags e2e ./...
```

Expected: no output (exit code 0).

### Full pre-review check

```bash
go build ./... && go vet ./... && go vet -tags e2e ./... && test -z "$(gofmt -l .)" && go tool staticcheck ./... && go tool staticcheck -tags e2e ./... && go test -count=1 ./... && echo ALL-OK
```

Expected last line: `ALL-OK`. Then run the race detector and the e2e tests.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `Conflict. The container name "/worker1" is already in use` on `compose up` | Another project uses the spec's container names | `export OBJSTORE_CONTAINER_PREFIX=objstore-` and retry. Do **not** remove containers you did not create |
| `port is already allocated` for 8080 | Something else uses port 8080 | `export CONTROLLER_PORT=18080` and use that port with `curl` |
| `/cluster` shows fewer than 4 workers right after `up` or a controller restart | Workers have not (re-)registered yet | Wait one heartbeat interval (3 s); check `docker logs <worker>` for `worker_register_failed` |
| A worker stays `SYNCING` | Registration failed during inventory reconciliation (possible from Phase 6) | Check controller logs for `inventory_reconcile_failed`; the worker retries automatically |
| Controller exits with `DEAD_AFTER (…) must be greater than HEARTBEAT_INTERVAL` | Misconfigured timings | Make `DEAD_AFTER` larger (≥ 3 × the heartbeat interval recommended) |
| Worker logs `heartbeat_failed` repeatedly | Controller down or unreachable | Start the controller; the worker re-registers automatically |
| Worker `/healthz` shows `"registered":false` with a controller configured | Registration not yet succeeded | Check worker logs for `worker_register_failed` and the error; check `CONTROLLER_URL` / `ADVERTISE_ADDR` |
| PUT to a worker returns `503 NOT_REGISTERED` | Worker not (yet) registered | Wait for registration; check `/healthz` `accepting_writes` |
| `go: command not found` / `'go' is not recognized` | Go not installed or terminal predates install | Install Go; open a new terminal |
| `failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine` | Docker Desktop not running | Start it; wait for `docker info` to succeed |
| `-race requires cgo` / `C compiler "gcc" not found` (Windows) | Native race detector needs a C toolchain | Use the Docker race command in §6 |
| `ls: C:/Program Files/Git/data/...` from `docker exec` | Git Bash path conversion | `export MSYS_NO_PATHCONV=1` |
| Shell scripts fail in containers with `\r: command not found` | CRLF line endings | `.gitattributes` enforces LF; convert local files with `dos2unix` |
