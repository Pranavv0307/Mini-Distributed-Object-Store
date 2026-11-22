# Run and Test Guide

Practical, step-by-step instructions to set up, run, and verify the project.

> **Rule for this document:** every command here was run and its result
> observed at the phase noted. Sections for later phases are marked
> *not yet available* until those phases are implemented.

**Current state (end of Phase 10, review fixes applied; Phase 11 in
progress):** the cluster runs under Docker Compose:
one controller and four workers. **Every client API endpoint of the
specification works:** upload (1 MB chunks, two copies on distinct workers,
atomic commit), verified download with replica fallback, metadata GET and
PATCH, list with pagination, and delete; `objctl` natively and as a Compose
`client` container. Returning workers are reconciled against the metadata;
`/cluster` shows replication health and `/admin/audit` checks the invariants.
**Lost and corrupt copies are re-replicated automatically** after
`REPAIR_GRACE`. Corruption is also found without any client read, by the
worker scrubber (confirmed by the controller) and the periodic inventory
sweep; a controller crash mid-upload leaves nothing behind (Phases 8–9).
Evaluation scripts and results: `EVALUATION.md` (Phase 10). The draft
one-command demo and viva matrix (Phase 11) are `../scripts/demo.sh` and
`VIVA_GUIDE.md`.

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

`bin/controller`, `bin/worker`, and `bin/objctl` are all functional.
`docker compose up --build` (below) also builds the image.

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
| `DB_PATH` | controller | `/data/meta.db` | SQLite metadata database (controller volume) |
| `REPLICATION_FACTOR` | controller | `2` | Copies per chunk |
| `UPLOAD_WINDOW` | controller | `4` | Chunks in flight per upload (1 = sequential) |
| `MAX_OBJECT_SIZE` | controller | `5368709120` | Largest accepted object (5 GiB) |
| `MAX_CONCURRENT_UPLOADS` | controller | `16` | More uploads at once get `503 SERVER_BUSY` |
| `MAX_CONCURRENT_DOWNLOADS` | controller | `32` | More downloads at once get `503 SERVER_BUSY` |
| `PLACEMENT_MIN_FREE_BYTES` | controller | `100000000` | Workers reporting less free space get no new chunks |
| `WORKER_CONNECT_TIMEOUT` | controller | `2s` | Connection timeout for worker calls |
| `WORKER_REQUEST_TIMEOUT` | controller | `10s` | Overall timeout per worker call |
| `CLIENT_READ_TIMEOUT` | controller | `60s` | Max stall of one read of an upload body |
| `JANITOR_INTERVAL` | controller | `10s` | Cleanup round period (also runs right after an aborted upload) |
| `RECONCILER_INTERVAL` | controller | `5s` | Repair round period |
| `MAX_CONCURRENT_REPAIRS` | controller | `4` | Repair copies in flight at once |
| `SCRUB_INTERVAL` | workers | `10m` | Time between full scrubber passes (demo: `30s`) |
| `SCRUB_BYTES_PER_SEC` | workers | `20000000` | Scrubber read throttle; `0` = unthrottled |
| `UPLOAD_STALE_AFTER` | controller | `10m` | An upload with no progress for this long is aborted |
| `INVENTORY_SWEEP_INTERVAL` | controller | `5m` | Live inventory sweep: orphans deleted, lost or wrong-size copies repaired |

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

### 4.3 Upload (verified at Phase 3, Compose cluster)

```bash
head -c 5500000 /dev/urandom > big.bin
sha256sum big.bin
curl -s -w '\nHTTP %{http_code}\n' -F 'metadata={"owner":"alice"}' \
  -F 'file=@big.bin;type=application/octet-stream' localhost:8080/api/v1/objects | tee up.json
ID=$(grep -o '"object_id":"[^"]*"' up.json | cut -d'"' -f4); echo "$ID"
```

Expected: `HTTP 201` and a JSON body with `"size":5500000`,
`"num_chunks":6`, `"custom_metadata":{"owner":"alice"}`, and a
`checksum_sha256` equal to the `sha256sum` output.

**Chunking and distribution** (6 chunks × 2 copies = 12 files, each chunk on
two different workers; file names are the chunk IDs):

```bash
for w in 1 2 3 4; do echo "worker$w: $(docker exec objstore-worker$w sh -c "ls /data/chunks | grep -c ^$ID")"; done
for w in 1 2 3 4; do docker exec objstore-worker$w sh -c "ls /data/chunks | grep ^$ID" | sed "s/^/worker$w /"; done | sort -k2
```

Expected: per-worker counts summing to 12; the sorted list shows each
`<ID>_00000N` exactly twice, on two different workers. Placement is random,
so counts vary per run (one verified run: 5, 4, 0, 3). After the next
heartbeat (3 s), `/cluster` reports the same `chunk_count` per worker:

```bash
curl -s localhost:8080/cluster | grep -o '"worker_id":"worker[0-9]"\|"chunk_count":[0-9]*' | paste - -
```

Verify one stored chunk on disk:
`docker exec objstore-worker1 sh -c "worker inspect /data/chunks/\$(ls /data/chunks | grep ^$ID | head -1)"`
→ `status: VALID`.

The controller logs one `upload_started` and one `upload_committed` line
(with `size`, `chunks`, `duration` in nanoseconds, and `mb_per_s`).

**Validation errors** (all `400`, nothing stored):

| Command | Expected |
| --- | --- |
| `curl -s -w ' HTTP %{http_code}\n' -F 'metadata={}' localhost:8080/api/v1/objects` | `MISSING_FILE` |
| `curl -s -w ' HTTP %{http_code}\n' -F 'metadata={"n":1}' -F 'file=@big.bin' localhost:8080/api/v1/objects` | `INVALID_METADATA` ("value of \"n\" must be a string, got number") |
| `curl -s -w ' HTTP %{http_code}\n' -H 'Content-Type: application/json' -d '{}' localhost:8080/api/v1/objects` | `INVALID_REQUEST` ("expected a multipart/form-data body") |

Metadata, list, and delete: §4.5.

### 4.4 Download (verified at Phase 4, Compose cluster)

```bash
head -c 3500000 /dev/urandom > obj.bin
ID=$(curl -s -F 'file=@obj.bin;filename=report.pdf;type=application/pdf' \
  localhost:8080/api/v1/objects | grep -o '"object_id":"[^"]*"' | cut -d'"' -f4)
curl -s -o got.bin -D headers.txt localhost:8080/api/v1/objects/$ID
grep -i -E '^(HTTP|content-type|content-length|content-disposition|etag|x-object-checksum)' headers.txt
sha256sum obj.bin got.bin; cmp obj.bin got.bin && echo IDENTICAL
```

Expected: `HTTP/1.1 200 OK`, `Content-Type: application/pdf`,
`Content-Length: 3500000`, `Content-Disposition: attachment; filename=report.pdf`,
`Etag` and `X-Object-Checksum-Sha256` both equal to the SHA-256; identical
hashes; `IDENTICAL`.

| Request | Expected |
| --- | --- |
| `curl -s -I localhost:8080/api/v1/objects/$ID` | `HTTP/1.1 200 OK`, `Content-Length: 3500000`, no body (no chunk is fetched) |
| `curl -s -w ' HTTP %{http_code}\n' localhost:8080/api/v1/objects/3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b` | `404`, `OBJECT_NOT_FOUND` |
| `curl -s -w ' HTTP %{http_code}\n' localhost:8080/api/v1/objects/not-a-uuid` | `400`, `INVALID_OBJECT_ID` |

The controller logs `download_completed` with size, duration, and MB/s.


### 4.5 Metadata, list, delete (verified at Phase 5, Compose cluster)

With an object `ID` uploaded with `-F 'metadata={"owner":"alice"}'` (§4.3):

```bash
curl -s -D - localhost:8080/api/v1/objects/$ID/metadata
```

Expected: `HTTP/1.1 200 OK`, `Etag: "v1"`, and JSON with all object fields,
`"custom_metadata":{"owner":"alice"}`, and `"chunks":[{"index":0,…,"replicas":[{"worker_id":"worker1","state":"OK"},{"worker_id":"worker2","state":"OK"}]},…]`
(which workers hold each chunk: the replication demonstration).

| Command | Expected |
| --- | --- |
| `curl -s -X PATCH -d '{"owner":"bob","project":"demo"}' localhost:8080/api/v1/objects/$ID/metadata` | 200, `"metadata_version":2`, `"custom_metadata":{"owner":"bob","project":"demo"}`, `Etag: "v2"` |
| `curl -s -X PATCH -d '{"project":null}' localhost:8080/api/v1/objects/$ID/metadata` | 200, version 3, `{"owner":"bob"}` |
| `curl -s -w ' HTTP %{http_code}\n' -X PATCH -H 'If-Match: "v1"' -d '{"x":"y"}' localhost:8080/api/v1/objects/$ID/metadata` | `VERSION_CONFLICT` "current version 3, expected 1", HTTP 409 |
| same with `If-Match: "v3"` | 200, version 4 |
| `curl -s -w ' HTTP %{http_code}\n' -X PATCH -d '{"n":1}' localhost:8080/api/v1/objects/$ID/metadata` | `INVALID_METADATA`, HTTP 400 |
| `curl -s 'localhost:8080/api/v1/objects?limit=2'` (3 objects stored) | 2 objects and a `"next_cursor":"eyJ…"` |
| `curl -s "localhost:8080/api/v1/objects?limit=2&cursor=$CUR"` (CUR = that cursor) | the third object, `"next_cursor":null` |
| `curl -s -w ' HTTP %{http_code}\n' 'localhost:8080/api/v1/objects?limit=0'` | `INVALID_PAGINATION`, HTTP 400 |
| `curl -s -w 'HTTP %{http_code}\n' -X DELETE localhost:8080/api/v1/objects/$ID` | `HTTP 204` |
| metadata, download, or DELETE again for `$ID` | `OBJECT_NOT_FOUND`, HTTP 404 |

PATCH never changes the object's data or chunk map. After the delete, within
about two seconds no worker holds a file of `$ID`:

```bash
for w in 1 2 3 4; do echo "worker$w: $(docker exec objstore-worker$w sh -c "ls /data/chunks | grep -c ^$ID")"; done
```

Expected: `0` for every worker. Controller log: `metadata_updated` (per
PATCH), `object_deleted`, then `object_purged`.

### 4.6 The `objctl` client (verified at Phase 5)

In the Compose `client` container (no Go needed; mount a directory for
files; `--no-deps` avoids restarting the cluster):

```bash
mkdir -p work && cp big.bin work/
docker compose --profile tools run --rm --no-deps -v "$(pwd -W)/work:/work" client upload /work/big.bin -meta owner=alice
docker compose --profile tools run --rm --no-deps client stat <object_id>
docker compose --profile tools run --rm --no-deps client patch <object_id> owner=bob tmp=
docker compose --profile tools run --rm --no-deps client list -all
docker compose --profile tools run --rm --no-deps -v "$(pwd -W)/work:/work" client download <object_id> -o /work/got.bin
docker compose --profile tools run --rm --no-deps client delete <object_id>
docker compose --profile tools run --rm --no-deps client cluster
```

(`$(pwd -W)` is Git Bash; use `$(pwd)` on Linux/macOS.) `download` prints
`objctl: wrote N bytes to /work/got.bin (sha256 … verified)`; it checks the
length and the whole-object SHA-256 and writes no file if either is wrong.
`patch k=` removes `k`; `patch -if-match v3 …` sets `If-Match`.

Natively: `go build -o bin/ ./cmd/...` then
`OBJSTORE_URL=http://localhost:8080 ./bin/objctl list` (or `-server URL`).

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

### 5.4 Uploads with workers unavailable (verified at Phase 3)

With only one worker alive, uploads are refused and nothing is stored:

```bash
docker stop objstore-worker2 objstore-worker3 objstore-worker4
# wait ~10 s until /cluster shows "alive":1
curl -s -w ' HTTP %{http_code}\n' -F 'file=@big.bin' localhost:8080/api/v1/objects
```

Expected: `{"error":{"code":"INSUFFICIENT_WORKERS","message":"1 eligible
workers, need at least 2"}} HTTP 503`. Restart them with
`docker start objstore-worker2 objstore-worker3 objstore-worker4`.

A worker failing *during* an upload (stopped, rejecting, timing out, or
corrupting data in transit) is covered by automated tests (§6): the upload
still succeeds with ≥ 2 workers, and any bytes the failed worker may have
stored are deleted by the janitor.

### 5.5 Downloads with a worker down (verified at Phase 4)

Find the two workers holding chunk 1 of the object from §4.4, stop one, and
download:

```bash
C1=${ID}_000001
for w in 1 2 3 4; do docker exec objstore-worker$w sh -c "test -f /data/chunks/$C1" && echo worker$w; done
docker stop objstore-worker3          # one of the two holders printed above
curl -s localhost:8080/api/v1/objects/$ID | cmp - obj.bin && echo IDENTICAL
docker start objstore-worker3
```

Expected: `IDENTICAL`, both immediately after the stop (the controller still
thinks the worker is `ALIVE`, the read fails over) and after it is `DEAD`
(the worker is no longer tried). Replicas on a stopped worker are **not**
marked `CORRUPT`: being unreachable says nothing about the data.

### 5.6 Corruption detection on download (verified at Phase 4)

Corrupt one copy of chunk 1 (on a holder from §5.5), then download several
times (copies are read in random order):

```bash
docker exec objstore-worker3 sh -c "printf garbage >> /data/chunks/$C1"
for i in 1 2 3 4 5 6; do curl -s localhost:8080/api/v1/objects/$ID | cmp -s - obj.bin || echo MISMATCH; done
docker logs objstore-controller 2>&1 | grep corruption_detected
```

Expected: no `MISMATCH`, and one log line
`"msg":"corruption_detected",…,"chunk_index":1,"worker_id":"worker3","reason":"size_mismatch: got 1000001 bytes, want 1000000","detected_by":"read","marked_corrupt":true`.
After that the corrupt copy is never read again. Repair of the bad copy is
Phases 7–8.

Corrupt **both** copies of chunk 0:

```bash
C0=${ID}_000000
for w in 1 2 3 4; do docker exec objstore-worker$w sh -c "test -f /data/chunks/$C0 && printf garbage >> /data/chunks/$C0"; done
curl -s -w ' HTTP %{http_code}\n' localhost:8080/api/v1/objects/$ID
```

Expected: `{"error":{"code":"STORAGE_UNAVAILABLE","message":"chunk 0 of object … has no available valid copy"}} HTTP 503`;
nothing of the object is returned. (If a *later* chunk has no valid copy,
the response has already started: the controller cuts the connection, so
the client sees a body shorter than `Content-Length` and never wrong bytes.
Verified: with both copies of chunk 2 of a 3.5 MB object corrupted,
`curl -sS -o got.bin …` prints `curl: (18) end of response with 1500000 bytes
missing`, exits with code 18, and `got.bin` holds the first 2,000,000 bytes,
identical to the original.) Error
messages never name workers; details are in the controller log
(`download_unavailable`, `download_aborted`).

### 5.7 Delete with a worker down (verified in automated tests)

`TestDeleteWithWorkerDown` (in-process) and `TestComposeObjectAPI` (Compose):
`DELETE` returns 204 while a holder is stopped; the files on the live holders
disappear within one janitor round; the stopped holder's files stay (its
pending deletes wait); after `docker start`, its files are removed and the
object's rows are purged (`object_purged` in the controller log).

### 5.8 Degraded operation and worker return (verified at Phase 6, default timings)

Helpers (Git Bash):

```bash
st() { curl -s localhost:8080/cluster | grep -o '"storage":{.*}'; }
au() { curl -s localhost:8080/admin/audit | grep -o '"ok":[a-z]*,"steady_state":[a-z]*'; }
```

With one 5.5 MB object stored (`$ID`):

| Step | Command | Observed |
| --- | --- | --- |
| Healthy | `st; au` | `"total":6,"fully_replicated":6`; `"ok":true,"steady_state":true` |
| How many copies worker2 holds | `curl -s localhost:8080/api/v1/objects/$ID/metadata \| grep -o '"worker_id":"worker2"' \| wc -l` | `4` (varies: random placement) |
| Stop it | `docker stop objstore-worker2`, wait ~10 s, `st; au` | `"fully_replicated":2,"under_replicated":4`; `"ok":true,"steady_state":false`, I5 4 violations |
| Still serving | `curl -s localhost:8080/api/v1/objects/$ID \| cmp - big.bin && echo IDENTICAL` | `IDENTICAL` |
| Return within grace | `docker start objstore-worker2`, wait ~2 s, `st; au` | `"fully_replicated":6`, `"pending_deletes":0`; `"ok":true,"steady_state":true` |
| What reconciliation did | `docker logs objstore-controller 2>&1 \| grep inventory_reconciled \| tail -1` | `"worker_id":"worker2",…,"inventory":4,"matched":4,"missing_removed":0,…,"orphans_queued":0` |

`under_replicated` counts chunks with one usable copy; it returns to 0 when
the worker comes back, with no files written or deleted (no data movement).

**Size damage while away** (appended bytes):

```bash
C=$(docker exec objstore-worker2 sh -c "ls /data/chunks | grep ^$ID | head -1")
docker exec objstore-worker2 sh -c "printf garbage >> /data/chunks/$C"
docker restart objstore-worker2      # wait ~4 s
docker logs objstore-controller 2>&1 | grep inventory_reconciled | tail -1
st; au
```

Observed (Phase 6, before repair existed): `"inventory":4,"matched":3,…,"size_mismatch_corrupt":1`
(logged at WARN); the copy is `CORRUPT` in the metadata;
`"fully_replicated":5,"under_replicated":1`, `"replicas_by_state":{"CORRUPT":1,"OK":11}`;
`"ok":true,"steady_state":false`. Since Phase 7 the repair loop then
rewrites the corrupt copy in place within one `RECONCILER_INTERVAL` (no
grace for corruption), after which the counts are back to 6 / 0 and the
audit is steady (`TestCorruptReplicaRepairedInPlace`).

**Wiped volume.** Stop the worker, empty its volume, start it:

```bash
docker stop objstore-worker2
docker run --rm -v objstore_worker2-data:/data alpine:3.24 sh -c 'rm -f /data/chunks/*'
docker start objstore-worker2
```

> This exact sequence is executed and verified by the automated test
> `TestComposeResync` (on its own uniquely named volume). The manual run of
> the `rm` step was blocked by Claude Code's safety check during this phase,
> so run it yourself; the volume name is `objstore_<worker>-data` (the project
> name `objstore` comes from `docker-compose.yml`). Expected: the controller
> logs `inventory_reconciled` with `missing_removed` equal to the copies the
> worker held; the metadata shows no replicas on it; `under_replicated` equals
> that count; the audit is `ok` with `steady_state` false; downloads are still
> identical. Since Phase 7 the lost copies are then re-replicated within one
> `RECONCILER_INTERVAL` (no grace: the rows are gone, so the copies are lost,
> not "may come back"; `TestWipedWorkerRepairedImmediately`).

Orphans (a chunk file no metadata claims) are queued and deleted by the
janitor when the worker registers (`TestOrphanOnReturn`).

### 5.9 Self-healing after a worker loss (verified at Phase 7, default timings)

The M7 demonstration. With three 5.5 MB objects stored (18 chunks):

```bash
ch() { curl -s localhost:8080/cluster | grep -o '"chunks":{[^}]*}'; }
rp() { curl -s localhost:8080/cluster | grep -o '"repair":{[^}]*}'; }
au() { curl -s localhost:8080/admin/audit | grep -o '"ok":[a-z]*,"steady_state":[a-z]*'; }
ch; au                                   # 18 fully replicated; ok, steady
docker stop objstore-worker3             # note the time
```

| When (after `docker stop`) | Observed |
| --- | --- |
| ~10 s (`DEAD_AFTER`) | worker3 `DEAD`; `ch` → `"fully_replicated":12,"under_replicated":6`; downloads identical; no copies made yet (grace) |
| ~41 s (10 s + `REPAIR_GRACE` 30 s + ≤ 5 s round) | `au` → `"ok":true,"steady_state":true`; `ch` → 18 fully replicated on the 3 remaining workers; `rp` → `"completed":6,"failed":0,"bytes_copied":5500000` |

The controller log shows one `repair_completed` per chunk, e.g.
`"chunk_index":2,"source":"worker2","target":"worker4","in_place":false,"superseded":"worker3","bytes":1000000`,
and a `repair_round` line (`"repaired":6`, duration ≈ 120 ms). Then:

```bash
docker start objstore-worker3
for id in $IDS; do docker exec objstore-worker3 sh -c "ls /data/chunks | grep -c ^$id"; done
ch; au
```

Within a few seconds worker3's superseded files are deleted (`0` for each
object), `pending_deletes` is 0, there is no over-replication, and the audit
is steady. (`$IDS` holds the three object IDs.)

If worker3 returns **before** `REPAIR_GRACE` has passed, nothing is copied
and nothing is deleted (`TestNoRepairWithinGrace`).

### 5.10 Scrubber, inventory sweep, crash recovery (verified at Phase 9 review)

Start the stack with short intervals so the background checks are visible
(the defaults are 10 min / 5 min):

```bash
export SCRUB_INTERVAL=5s INVENTORY_SWEEP_INTERVAL=5s HEARTBEAT_INTERVAL=1s RECONCILER_INTERVAL=1s
docker compose up -d
```

**Scrubber (no client read).** Upload an object, pick a holder of chunk 0
from `/metadata`, and overwrite 4 bytes of that copy in place (same size, so
only a checksum can tell):

```bash
docker exec worker3 sh -c "printf ZZZZ | dd of=/data/chunks/${ID}_000000 bs=1 seek=1000 conv=notrunc 2>/dev/null"
```

Within one scrub interval the controller logs `corruption_detected` with
`"detected_by":"scrubber"`, then `repair_started` / `repair_completed`, and
the replica is `OK` again (rewritten in place). Observed (verified run,
`SCRUB_INTERVAL=5s`): detected and rewritten within one scrub interval (in
that run under 1 s, as a pass started right after the damage), with exactly
one `corruption_detected` and one `repair_completed`: later heartbeats from
that worker no longer report it.

**Deleted chunk file.** `docker exec worker3 rm /data/chunks/<chunk_id>`: the
next inventory sweep logs `corruption_detected` with
`"detected_by":"inventory_sweep"` and repair rewrites the file (verified run:
file back after 4 s; download identical; audit ok and steady).

**Controller killed mid-upload.** Start a large upload, `docker kill
controller` while it streams, then `docker start controller`: the controller
logs `startup_recovery`; within a few `JANITOR_INTERVAL`s the object is gone
from `/cluster` (`objects_by_status` empty), `pending_deletes` is 0, and no
chunk files of it remain on any volume (verified run, 300 MB upload killed
after 4 chunks were stored: clean 8 s after the restart). Automated:
`TestComposeControllerKilledMidUpload`.

Crash mid-write on a worker: `TestWorkerKilledMidWriteLeavesNoPartialChunk`
(no partial chunk is ever visible).

---

## 6. Automated testing and static analysis

Run from the repository root. All verified at the Phase 8–10 review.

### Unit and integration tests

```bash
go test ./...
```

Expected (all `ok`; `cmd/*` and `internal/wire` print `[no test files]`):

```
ok  	objstore/internal/config
ok  	objstore/internal/controller/api
ok  	objstore/internal/controller/custommeta
ok  	objstore/internal/controller/download
ok  	objstore/internal/controller/integrity
ok  	objstore/internal/controller/janitor
ok  	objstore/internal/controller/meta
ok  	objstore/internal/controller/placement
ok  	objstore/internal/controller/registry
ok  	objstore/internal/controller/workerclient
ok  	objstore/internal/httpx
ok  	objstore/internal/ids
ok  	objstore/internal/version
ok  	objstore/internal/worker/agent
ok  	objstore/internal/worker/scrub
ok  	objstore/internal/worker/server
ok  	objstore/internal/worker/store
ok  	objstore/test/integration
```

`test/integration` runs in-process clusters (controller with SQLite,
upload coordinator, janitor, and real workers behind a fault injector that
can fail, delay, or corrupt PUTs; 50 ms heartbeats, 1 s `DEAD_AFTER`; the
live inventory sweep every 25 ms, so every test runs with it; each worker has
a scrubber the tests trigger explicitly) and builds and kills the real worker
binary; about 50–60 s. Upload tests cover
the 5.5 MB checkpoint, edge sizes, window sizes, metadata before/after the
file, invalid requests, insufficient workers, worker failure/stop/timeout/
corruption mid-upload, truncated bodies, client disconnect, size limit,
concurrent uploads, and `SERVER_BUSY`; download tests cover verification and
fallback; object tests cover metadata, PATCH (merge, If-Match, limits,
concurrency, chunks untouched), list paging (including during concurrent
uploads), delete (immediate invisibility, cleanup, worker down, during a
download), and the `objctl` binary; `TestSpecConformance` walks every row of
the spec's endpoint and status tables; resync tests cover a worker's stop
and return within grace (no data movement), a wiped volume, size damage, an
orphan, and the `SYNCING` races with reconciliation held open; repair tests
cover the M7 demo (with logged detection and heal times), no repair within
grace, in-place repair of a corrupt copy, immediate repair of a wiped worker,
an unrecoverable chunk rescued by a returning holder, over-replication
trimming, the concurrency limit, and no mass repair after a controller
restart; integrity tests (Phases 8–9) cover scrubber detection without a
client read and exactly one repair, a false scrubber report (not trusted),
both copies corrupt (503), a deleted chunk file and an orphan found by the
sweep, a controller crash mid-upload, I4 during concurrent writes, and
`TestInvariantsUnderChurn` (uploads, PATCHes, deletes while a worker dies,
is repaired around, and returns; then steady state and byte-identical
downloads). `go test -short ./...` skips the timing-based and
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
- `TestComposeSelfHealing`: the M7 demo with `DEAD_AFTER=4s`,
  `REPAIR_GRACE=6s`, `RECONCILER_INTERVAL=1s`: stops the worker holding most
  copies of three 3 MB objects, checks downloads while degraded, waits for the
  audit to be steady again (logs detection and time to full RF: 4.1 s and
  10.3 s in the verified run), checks repair counters and downloads, starts
  the worker and checks its superseded files are deleted and nothing is
  over-replicated.
- `TestComposeResync` (repair rounds disabled with `RECONCILER_INTERVAL=1h`,
  since it checks detection, not repair): stops the worker holding most replicas (under-replicated
  count, downloads and uploads still work, audit ok but not steady), starts it
  (everything back to normal, same files, no pending deletes), then stops it,
  empties its volume with `docker run … rm -f /data/chunks/*`, and starts it
  (missing replicas dropped, audit ok with I5 = lost copies, downloads intact).
- `TestComposeObjectAPI`: drives `objctl` in the Compose `client` container:
  upload, stat (chunk map with replicas), patch, list, download (verified by
  `objctl`), delete (files removed from all volumes), and delete with a holder
  stopped (its files removed after `docker start`).
- `TestComposeDownload`: uploads 3.5 MB, downloads and compares; stops a
  holder of chunk 1 and downloads again (before and after `DEAD`); appends
  bytes to one copy on disk and downloads 6 times (all correct, controller logs
  `corruption_detected`); corrupts both copies of chunk 0 and expects `503`.
- `TestComposeUpload`: uploads a 5.5 MB object through the real stack and
  checks 6 chunks stored as 12 files, each chunk on 2 distinct worker volumes;
  then stops 3 workers and checks the next upload gets
  `503 INSUFFICIENT_WORKERS` with nothing stored.
- `TestComposeIntegrity` (`SCRUB_INTERVAL=2s`, `INVENTORY_SWEEP_INTERVAL=2s`):
  overwrites 4 bytes of one copy in place; the scrubber finds it with no
  client read, the controller logs `"detected_by":"scrubber"`, the copy is
  repaired, and after two more scrub passes there is still exactly one
  `repair_completed` and one `corruption_detected` (3 s in the verified run).
  Then removes a chunk file; the sweep logs `"detected_by":"inventory_sweep"`
  and the file is rewritten.
- `TestComposeControllerKilledMidUpload`: streams an upload, `docker kill`s
  the controller after chunks are stored, restarts it: `startup_recovery` is
  logged, no object is listed, no chunk file is left on any volume, and the
  audit is ok and steady.
- `TestWorkerContainer`: the standalone worker container checks from Phase 1.

Expected: all nine `PASS` (about 3 minutes including the image build). Set
`E2E_IMAGE=objstore:dev` to reuse an existing image.

### Evaluation scripts (Phase 10)

See [`EVALUATION.md`](EVALUATION.md) for the commands and the measured
results: `scripts/phase10-bench-compose.sh` (Compose throughput and latency),
`scripts/phase10-chaos.sh` (worker loss: detection and time to full RF), and
`scripts/phase10-benchmark.sh` (the in-process benchmark below).

### Benchmark: sequential vs windowed upload

```bash
go test -run '^$' -bench UploadWindow -benchtime 10x -count 3 ./test/integration/
```

Uploads an 8 MB object with `UPLOAD_WINDOW` = 1, 2, 4, 8 (in-process, 5 ms
simulated latency per PUT, fsync off). Verified medians: 70.4, 112.7, 122.1,
112.2 MB/s, so windowed is about 1.7× sequential (DEC-036). Absolute numbers
depend on the machine.

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
| PATCH returns `409 VERSION_CONFLICT` | `If-Match` names an old `metadata_version` | GET the metadata, use the current `ETag`, retry |
| List returns `400 INVALID_PAGINATION` | `limit` outside 1–1000, or a cursor not issued by this server (e.g. edited, or from a reset database) | Start the listing again without `cursor` |
| A deleted object's files are still on a worker | That worker was down; its deletes wait until it is `ALIVE` | Start the worker; the janitor removes them within one round |
| `objctl: download incomplete` / `checksum mismatch` | The download was cut off or corrupted in transit; no output file was written | Retry; check the controller log |
| Download returns `503 STORAGE_UNAVAILABLE` | Some chunk has no valid copy on an `ALIVE` worker: both holders down, or both copies corrupt | Check the controller log (`download_unavailable`, `corruption_detected`); start the holders. Both copies corrupt cannot be fixed automatically: re-upload |
| `curl: (18) end of response with … bytes missing` on download | A chunk failed after the response started; the controller cut the connection rather than send wrong bytes | Check the controller log for `download_aborted`; retry |
| Upload returns `503 INSUFFICIENT_WORKERS` | Fewer than 2 workers are `ALIVE` (or the others reported less than `PLACEMENT_MIN_FREE_BYTES` free) | Check `/cluster`; start workers; free disk space |
| Upload returns `503 UPLOAD_FAILED` | Workers failed during the upload and no distinct second worker was left | Check controller log `replica_write_failed` for the reason; retry |
| Upload returns `503 SERVER_BUSY` | `MAX_CONCURRENT_UPLOADS` uploads in progress | Retry later or raise the limit |
| Upload returns `400 INVALID_REQUEST` mentioning the body | Client disconnected, or the body stalled longer than `CLIENT_READ_TIMEOUT` | Retry; nothing was committed, and stored chunks are cleaned up by the janitor |
| Controller exits with `meta: database schema version … is not supported` | The `controller-data` volume holds a database from an incompatible build | Use a matching build, or reset with `docker compose down -v` (deletes all data) |
| `/admin/audit` shows `"ok":false` | A safety invariant is violated: a bug | Read `checks[].examples` and the controller log `audit_violation`; report it |
| `/admin/audit` shows `"steady_state":false` | Some chunk lacks RF usable copies: a worker is down within `REPAIR_GRACE`, or repair is still running | Check `/cluster` `repair.waiting_for_grace` / `in_flight`; it becomes steady after the grace period plus a round |
| `/cluster` `repair.failed` keeps growing | No eligible target (fewer than RF+1 ALIVE workers for a chunk whose holder is lost), or sources keep failing | Controller log `repair_failed` (first occurrence per chunk and reason at WARN); start more workers |
| `/cluster` `repair.unrecoverable` > 0, log `data_unrecoverable` | A chunk has no valid copy on any live or returning worker | Start any stopped holders (it is repaired from them automatically); otherwise the object must be re-uploaded |
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
