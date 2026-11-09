# Run and Test Guide

Practical, step-by-step instructions to set up, run, and verify the project.

> **Rule for this document:** every command here was run and its result
> observed at the phase noted. Sections for later phases are marked
> *not yet available* until those phases are implemented.

**Current state (end of Phase 1):** the **worker** is complete as a
standalone container. There is no controller yet, so there is no client API
and no `docker-compose.yml` (Phase 2).

> ⚠️ **Docker name collisions.** Container and volume names are global on a
> Docker host. The commands below use names prefixed `objstore-` to avoid
> clashing with other projects. Before running them, check that
> `docker ps -a` and `docker volume ls` show no `objstore-w1` /
> `objstore-w1-data`. **Never** `docker rm` or `docker volume rm` a resource
> you did not create.

---

## 1. Environment setup

### Required software

| Software | Version verified | Needed for |
| --- | --- | --- |
| Go | 1.27.3 (any 1.27.x works; `go.mod` requires ≥ 1.27.0) | Building and testing |
| Docker Engine | 29.9.2 (Docker Desktop on Windows) | Container image, e2e tests, race detector on Windows |
| Docker Compose | v5.6.1 (`docker compose`) | Running the cluster (Phase 2+) |
| Git (and Git Bash on Windows) | 2.55 | Source control; the shell commands below |
| curl, sha256sum | bundled with Git Bash | Manual API checks |

The shell examples are written for **bash** (Git Bash on Windows, or any
Linux/macOS shell). On Windows, run them in Git Bash, not PowerShell.

### Installing Go

- **Windows:** `winget install GoLang.Go` (installs to `C:\Program Files\Go`).
  **Open a new terminal afterwards**: shells started before the install don't
  have Go on `PATH`. Check with `go version`.
- **macOS:** `brew install go`. **Linux:** see <https://go.dev/doc/install>.

### Docker

Start Docker Desktop and wait until `docker info` prints server information
without errors (see Troubleshooting if it does not).

### Dependencies

```bash
go mod download
```

All dependencies are pinned in `go.mod`/`go.sum`.

---

## 2. Building

### Go binaries

```bash
go build ./...                 # compile everything (no output = success)
go build -o bin/ ./cmd/...     # binaries into bin/ (ignored by git)
```

`bin/worker` is functional. `bin/controller` and `bin/objctl` are still stubs
that exit with code 2.

### Docker image

```bash
docker build -t objstore:dev .
```

One image contains all binaries (DEC-023); its default command is `worker`.
Verify:

```bash
docker run --rm objstore:dev worker version
```

Expected: `worker dev`.

---

## 3. Running a worker (Phase 1)

### Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `WORKER_ID` | *(required)* | Stable identity; `[a-z0-9][a-z0-9_-]{0,62}` |
| `LISTEN_ADDR` | `:9000` | HTTP listen address |
| `DATA_DIR` | `/data` | Root of the persistent volume (`chunks/`, `tmp/`) |
| `CHUNK_SIZE` | `1048576` | Largest accepted chunk payload (provisional, DEC-005) |
| `FSYNC_ON_WRITE` | `true` | fsync file and directory on write/delete |
| `MAX_CONCURRENT_READS` | `16` | Chunk GETs held in memory at once |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

Invalid configuration prints every problem and exits with code 2.

### Start a worker container with a named volume

```bash
export MSYS_NO_PATHCONV=1   # Git Bash only: stop it rewriting /data paths
docker volume create objstore-w1-data
docker run -d --name objstore-w1 -e WORKER_ID=worker1 \
  -v objstore-w1-data:/data -p 127.0.0.1:19001:9000 objstore:dev
W=http://127.0.0.1:19001
curl -s $W/healthz
```

Expected:

```json
{"status":"ok","worker_id":"worker1","registered":false,"accepting_writes":true}
```

(Phase 1 workers run standalone, so they accept writes without registering,
DEC-026.) Port 19001 is published only for this standalone check; in the
Compose cluster, workers are not published to the host.

### Logs

```bash
docker logs objstore-w1
```

JSON lines, for example `startup_temp_cleanup` (with `removed_temp_files`),
`worker_started`, `http_request`, `chunk_stored`, `chunk_put_rejected`,
`chunk_deleted`.

### Run a worker natively (without Docker)

```bash
go build -o bin/ ./cmd/...
WORKER_ID=worker1 LISTEN_ADDR=127.0.0.1:19001 DATA_DIR=./data/worker1 ./bin/worker
```

It logs `startup_temp_cleanup` and `worker_started` and creates
`./data/worker1/chunks` and `./data/worker1/tmp` (`bin/` and `data/` are
git-ignored). Stop it with Ctrl-C (graceful shutdown). Remove `./data` to
reset.

---

## 4. Functional testing — worker API (verified at Phase 1)

With the container from §3 running and `W` set:

```bash
head -c 1048576 /dev/urandom > chunk.bin
SUM=$(sha256sum chunk.bin | cut -d' ' -f1)
ID=3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000
```

**Store a chunk** (expect `201`, `"created":true`):

```bash
curl -s -w ' HTTP %{http_code}\n' -X PUT -H "X-Chunk-SHA256: $SUM" \
  --data-binary @chunk.bin $W/internal/chunks/$ID
```

**Store it again** (idempotent: expect `200`, `"created":false`): repeat the
command above.

**Fetch and compare** (both hashes identical; header carries the checksum):

```bash
curl -s -o fetched.bin -D - $W/internal/chunks/$ID | grep -i -E 'HTTP/|x-chunk-sha256|content-length'
sha256sum chunk.bin fetched.bin
```

**Wrong checksum is rejected** (expect `400` `CHECKSUM_MISMATCH`, nothing stored):

```bash
curl -s -w ' HTTP %{http_code}\n' -X PUT \
  -H "X-Chunk-SHA256: 0000000000000000000000000000000000000000000000000000000000000000" \
  --data-binary @chunk.bin $W/internal/chunks/3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000001
```

**Path traversal is rejected** (expect `400` `INVALID_CHUNK_ID` for `"../escape"`):

```bash
curl -s -w ' HTTP %{http_code}\n' -X PUT -H "X-Chunk-SHA256: $SUM" \
  --data-binary @chunk.bin "$W/internal/chunks/..%2Fescape"
```

**Inventory** (one chunk, `size` 1048576):

```bash
curl -s $W/internal/workers/worker1/chunks
```

**Look at the files and verify one with the inspect tool** (`status: VALID`, exit code 0):

```bash
docker exec objstore-w1 ls -l /data/chunks /data/tmp
docker exec objstore-w1 worker inspect /data/chunks/$ID
```

Each chunk file is 48 bytes larger than its payload (the header, DEC-012).

**Delete** (expect `204` twice: idempotent; then GET → `404` `CHUNK_NOT_FOUND`):

```bash
curl -s -o /dev/null -w 'DELETE HTTP %{http_code}\n' -X DELETE $W/internal/chunks/$ID
curl -s -o /dev/null -w 'DELETE again HTTP %{http_code}\n' -X DELETE $W/internal/chunks/$ID
curl -s -w ' HTTP %{http_code}\n' $W/internal/chunks/$ID
```

---

## 5. Failure testing (Phase 1: single worker)

Re-create the chunk first (the PUT from §4).

**Persistence across stop/start** (hash after restart equals `$SUM`):

```bash
docker stop objstore-w1 && docker start objstore-w1
curl -s $W/internal/chunks/$ID | sha256sum
```

(After `docker start`, wait a moment until `curl -s $W/healthz` answers.)

**Simulated corruption** (append bytes to the chunk file):

```bash
docker exec objstore-w1 sh -c "printf garbage >> /data/chunks/$ID"
docker exec objstore-w1 worker inspect /data/chunks/$ID; echo "exit code: $?"
curl -s $W/internal/workers/worker1/chunks
curl -s $W/internal/chunks/$ID | wc -c
```

Expected: `inspect` reports `status: CORRUPT (payload is 1048583 bytes,
header says 1048576; payload SHA-256 does not match header)` and exit code 1;
the inventory reports `size` 1048583; GET returns 1048577 bytes (capped at
`CHUNK_SIZE + 1`, DEC-025). The worker returns bytes as stored and never
decides validity itself; the controller's verification (Phase 4) rejects them.

**Kill mid-write leaves no partial chunk:** automated in
`TestWorkerKilledMidWriteLeavesNoPartialChunk` (native process) and the e2e
subtest `no partial chunk after kill mid-write` (container); see §6.

**Clean up** (only what you created above):

```bash
docker rm -f objstore-w1 && docker volume rm objstore-w1-data
rm -f chunk.bin fetched.bin
```

Controller restarts, multi-worker failures, and repair: *not yet available*
(Phases 2, 6–9).

---

## 6. Automated testing and static analysis

Run from the repository root. All commands verified at Phase 1.

### Unit and integration tests

```bash
go test ./...
```

Expected:

```
?   	objstore/cmd/controller	[no test files]
?   	objstore/cmd/objctl	[no test files]
?   	objstore/cmd/worker	[no test files]
ok  	objstore/internal/config	...
ok  	objstore/internal/httpx	...
ok  	objstore/internal/ids	...
ok  	objstore/internal/version	...
?   	objstore/internal/wire	[no test files]
ok  	objstore/internal/worker/server	...
ok  	objstore/internal/worker/store	...
ok  	objstore/test/integration	...
```

`test/integration` builds the worker binary (needs `go` on `PATH`), kills it
mid-write, and restarts it: about 5–15 s. Skip it with `go test -short ./...`.
Add `-count=1` to bypass the test cache, `-v` for detail.

### End-to-end tests (Docker)

```bash
go test -tags e2e -count=1 -v ./test/e2e/...
```

Builds `objstore:e2e` from the repository, then runs a worker container on a
uniquely named container and volume (`objstore-e2e-*`, removed afterwards)
and checks: wrong checksum rejected; data survives `docker stop/start`; no
partial chunk after `docker kill` mid-write; `worker inspect` reports VALID;
data survives removing and re-creating the container on the same volume.
Expected: `--- PASS: TestWorkerContainer` with 5 passing subtests (~15 s
after the image is built). Set `E2E_IMAGE=objstore:dev` to reuse an
existing image instead of building.

### Race detector

On Linux/macOS with a C toolchain: `go test -race ./...`.

On **Windows**, `-race` needs cgo and a C compiler, which is not installed.
Run it in the Go container (DEC-018). The named volumes `objstore-gomod` and
`objstore-gocache` cache modules and builds; the source is mounted read-only.

PowerShell:

```powershell
docker run --rm -v "${PWD}:/src:ro" -v objstore-gomod:/go/pkg/mod -v objstore-gocache:/root/.cache/go-build -w /src golang:1.27.3 go test -race -count=1 ./...
```

Git Bash:

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W):/src:ro" -v objstore-gomod:/go/pkg/mod -v objstore-gocache:/root/.cache/go-build -w /src golang:1.27.3 go test -race -count=1 ./...
```

Expected: the same `ok` lines as above. This also runs the crash test on
Linux, exercising real directory fsync and SIGKILL.

### Static analysis

```bash
go vet ./... && go vet -tags e2e ./...
gofmt -l .
go tool staticcheck ./... && go tool staticcheck -tags e2e ./...
```

Expected: no output (exit code 0). Fix formatting with `gofmt -w .`.

### Full pre-review check

```bash
go build ./... && go vet ./... && go vet -tags e2e ./... && test -z "$(gofmt -l .)" && go tool staticcheck ./... && go tool staticcheck -tags e2e ./... && go test -count=1 ./... && echo ALL-OK
```

Expected last line: `ALL-OK`. Then run the race detector and the e2e tests.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `go: command not found` / `'go' is not recognized` | Go not installed, or terminal opened before installing | Install Go; open a new terminal; or call `"C:\Program Files\Go\bin\go.exe"` |
| `go: go.mod requires go >= 1.27.0` | Older Go toolchain | Install Go 1.27.x |
| `failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine` | Docker Desktop not running or still starting | Start Docker Desktop; wait until `docker info` succeeds. If it exits right away, start it again from the Start menu and check for prompts |
| `-race requires cgo` / `C compiler "gcc" not found` (Windows) | Native race detector needs a C toolchain | Use the Docker race command in §6 |
| `ls: C:/Program Files/Git/data/...: No such file or directory` from `docker exec` | Git Bash rewrote `/data/...` into a Windows path | `export MSYS_NO_PATHCONV=1` before running `docker` commands |
| `Conflict. The container name "/…" is already in use` | A container with that name exists (possibly from another project) | Pick a different name. Do **not** remove a container you did not create |
| Worker exits with `invalid configuration: WORKER_ID is required` | `WORKER_ID` not set | Pass `-e WORKER_ID=worker1` |
| PUT returns `400 INVALID_REQUEST` "Content-Length header is required" | Client used chunked transfer encoding | Send a body with a known length (curl `--data-binary @file` does) |
| PUT returns `503 NOT_REGISTERED` | Write gate closed (from Phase 2: worker not yet registered) | Wait for registration; check `/healthz` `accepting_writes` |
| Shell scripts fail in containers with `\r: command not found` | CRLF line endings | `.gitattributes` enforces LF for checked-out files; convert locally created files with `dos2unix` |
