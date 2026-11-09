# Development Log

Chronological record of each phase: what was built, why it looks the way it
does, what went wrong, and what is left. Newest entries at the bottom.

---

## Phase 0 — Setup and design lock-in (2026-11-09)

### What was done

- Read the project specification (PDF) and the architecture document in full.
  Extracted requirements, components, milestones, and open questions; agreed
  the phase roadmap with the owner (Phases 0–11 = milestones M0–M11).
- Installed Go 1.27.3 via `winget install GoLang.Go` (Go was not previously
  installed on the development machine).
- Initialised a git repository on branch `main`. **No commits** (the owner
  commits after each phase review).
- Created the Go module `objstore` (`go 1.27.0` minimum) with stub binaries
  `cmd/controller`, `cmd/worker`, `cmd/objctl` (each prints "not implemented"
  and exits 2) and `internal/version` with a unit test as a toolchain smoke
  test.
- Pinned `staticcheck` as a `go.mod` tool dependency (`go tool staticcheck`).
- Added `.gitignore`, `.gitattributes` (LF line endings), `.editorconfig`,
  `.env.example` (the configuration contract from design doc §5.4 plus two new
  parameters).
- Wrote the documentation set: `README.md`, `docs/ARCHITECTURE.md`,
  `docs/IMPLEMENTATION_PLAN.md`, `docs/DECISIONS.md` (DEC-001…DEC-022),
  `docs/RUN_AND_TEST.md`, `docs/API.md` (the API contract, an M0 deliverable),
  this log, and copies of the source documents in `docs/reference/`.

### Important decisions

- **Worker `SYNCING` state (DEC-006).** Evaluated whether a worker used during
  inventory reconciliation causes races. It does: a placement after the
  inventory snapshot leads to false "missing" row removal and needless orphan
  deletion; a quick restart lets the controller write to the worker between
  the snapshot and registration; stale `OK` rows make the download pre-flight
  lie; repair accounting is overstated. Adopted a registration-driven
  `SYNCING` state with epochs plus worker-side write gating. Approved by the
  owner.
- **`pending_deletes` table (DEC-007).** Found an orphan-deletion race that
  `SYNCING` does not cover (a scheduled orphan delete destroying a chunk that a
  later repair re-claimed on the same worker), and the "timeout is not proof of
  failure" gap in upload retries. Deletion intent becomes a durable row,
  mutually exclusive with claims inside SQLite transactions. Approved by the
  owner.
- Resolved the remaining design-doc ambiguities: repair target rule (DEC-008),
  periodic inventory sweep (DEC-009), inventory endpoint location (DEC-010),
  conditional claims and commit (DEC-011), and every §9 open decision
  (DEC-012…DEC-017, DEC-020, DEC-021).
- Tooling: pure-Go SQLite driver (DEC-003) and the race detector in a Linux
  container (DEC-018), both because Windows has no C toolchain here.

### Problems encountered

- **Go missing.** Installed with winget; the MSI install took several minutes.
  Shells started before the install don't see Go on `PATH`; open a new
  terminal or use `"C:\Program Files\Go\bin\go.exe"` directly.
- **`go mod init` wrote `go 1.27.3`.** Lowered to `go 1.27.0` so any 1.27
  patch release builds the module.
- **Docker Desktop was not running.** Started it to run the race detector in
  a container.
- **The Read tool cannot render the PDF** (no poppler). Extracted it with
  `pdftotext -layout`; the text version is kept in `docs/reference/`. The
  PDF's configuration table is garbled by extraction; the intended values are
  1 controller, 4 workers, 1+ clients, 1 MB chunks, RF 2, Docker, persistent
  worker storage (consistent with spec §9).

### Tests performed

See the Phase 0 section of [`RUN_AND_TEST.md`](RUN_AND_TEST.md) and the
completion report. `go build`, `go vet`, `gofmt -l`, `go tool staticcheck`,
and `go test` passed natively on Windows (Go 1.27.3). `go test -race -count=1
./...` and `go vet ./...` passed inside `golang:1.27.3` (linux/amd64), from
both PowerShell and Git Bash. `go build -o bin/ ./cmd/...` produced the three
stub binaries, which exit with code 2 as intended.

### Known limitations / remaining work

- DEC-005 (chunk size) and DEC-013 (string-only metadata) are PROVISIONAL
  pending owner confirmation.
- No Dockerfile or Compose file yet (Phases 1–2).
- The module path `objstore` should be renamed if the repository is published
  (DEC-001).

---

## Phase 1 — Worker as a container (2026-11-09)

### What was implemented

- **`internal/ids`**: object-ID generation (UUIDv4 via `crypto/rand`), strict
  validation of object, chunk, and worker IDs, chunk-ID construction/parsing.
- **`internal/config`**: worker configuration from environment variables,
  reporting all invalid variables at once.
- **`internal/wire`**: shared JSON types, the `X-Chunk-SHA256` header name,
  stable error codes.
- **`internal/httpx`**: JSON responses and error bodies; middleware that logs
  one structured line per request and turns panics into 500s.
- **`internal/worker/store`**: the chunk store: 48-byte header format,
  temp → fsync → rename → dir-fsync writes, always-replace PUT, striped
  per-chunk locks, capped reads, idempotent delete, metadata-only inventory,
  startup temp cleanup, and an `Inspect` debug function. Platform files for
  directory fsync and disk-full detection (Linux vs. Windows).
- **`internal/worker/server`**: the five worker endpoints, write gate, bounded
  concurrent reads.
- **`cmd/worker`**: configuration, graceful shutdown on SIGINT/SIGTERM,
  `worker inspect FILE...` and `worker version` subcommands.
- **`Dockerfile`/`.dockerignore`**: one multi-stage image with all binaries
  (`golang:1.27.3-alpine` → `alpine:3.24`, non-root uid 10001), 27.6 MB.
- **Tests**: unit tests (ids, config, httpx, store, server), an integration
  test that kills the real worker process mid-write, and a Docker e2e test.

### Important decisions

DEC-023 (one Alpine image, non-root), DEC-024 (PUT always replaces),
DEC-025 (GET loads under a striped lock with bounded concurrency; read cap
`CHUNK_SIZE + 1`), DEC-026 (standalone write gate open in Phase 1),
DEC-027 (unreadable header → `CHUNK_CORRUPT`), DEC-028 (`internal/ids`).
DEC-012 now documents the exact on-disk layout.

Design points worth knowing:

- The worker **never judges payload validity** on read. It returns every byte
  after the header, so appended/truncated/flipped bytes reach the controller's
  checksum (the authority) instead of being silently trimmed.
- The inventory's `size` is file size − header, from `stat` only, so it is
  cheap and still exposes length corruption.
- Locks are held only around local file operations (rename, unlink, read into
  memory), never across network I/O.

### Problems encountered and resolutions

1. **Crash test timed out on Windows.** The test waited for the partial temp
   file to grow using `DirEntry.Info()`, whose size comes from the directory
   entry; Windows does not update it while the file is open for writing.
   Switched to `os.Stat(path)`.
2. **`gofmt` / `staticcheck` findings** in test code (map alignment, an unused
   field, an unused helper). Fixed.
3. **Git Bash path conversion.** `docker exec … ls /data/chunks` was rewritten
   to `C:/Program Files/Git/data/chunks`. Documented
   `export MSYS_NO_PATHCONV=1` in `RUN_AND_TEST.md`.
4. **Incident: collision with another Docker project (my error).** The first
   manual walkthrough used the generic container name `worker1`. A stopped
   container with that name already existed from the owner's separate Compose
   project `dos`. The `docker run` failed, but the script kept going: it
   started the `dos` worker1 container, wrote a 7-byte stray file
   `chunks/3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000` into its volume
   `dos_worker1-data`, then removed the container with `docker rm -f`. The
   volume `dos_worker1-data` itself was not removed and was otherwise
   unchanged. The `worker1-data` volume that was removed was the one the
   script had just created. Remediation awaits owner approval. Prevention:
   all documented commands and tests now use `objstore-` prefixed or
   uniquely suffixed names, scripts abort if names exist, and
   `RUN_AND_TEST.md` carries a warning. The spec-mandated Compose names
   (`controller`, `worker1`…`worker4`) will collide with the `dos` project
   in Phase 2; this is raised with the owner.

### Tests performed and results

All on 2026-11-09:

- `go build`, `go vet` (default and `-tags e2e`), `gofmt -l` (empty),
  `go tool staticcheck` (default and `-tags e2e`): pass.
- `go test -count=3 ./...` natively on Windows: pass (integration crash test
  included).
- `go test -race -count=3 ./...` in `golang:1.27.3` (Linux): pass.
- `go test -tags e2e -count=1 -v ./test/e2e/...`: `TestWorkerContainer` and
  all 5 subtests pass.
- Manual `curl` walkthrough of `RUN_AND_TEST.md` §3–5 in Git Bash against a
  container: every documented output observed (201/200 PUT, matching hashes,
  400 `CHECKSUM_MISMATCH`, 400 `INVALID_CHUNK_ID` for `..%2Fescape`,
  inventory, inspect VALID → CORRUPT after appending, persistence across
  stop/start, 204/204/404 delete sequence).
- Native run of `bin/worker`: starts, serves `/healthz`, creates its
  directories.

### Known limitations

- Standalone only: no registration, heartbeats, or free-space reporting yet
  (Phase 2).
- No worker-side scrubber yet (Phase 8).
- `FSYNC_ON_WRITE` durability across power loss is implemented but cannot be
  demonstrated with container stops (architecture §5.11); the on/off cost is
  measured in Phase 10.
- The worker trusts the controller completely (no authentication, §5.10).

### Work remaining

Phase 2: controller skeleton, registry with `SYNCING`/epochs, registration
and heartbeats (the worker's write gate closes until registered), failure
detector, `/healthz`, `/cluster`, `docker-compose.yml`.

---

## Between Phases 1 and 2 — owner decisions and incident remediation (2026-11-09)

- **Stray file removed (owner-approved).** The 7-byte file
  `chunks/3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000` that the Phase 1
  walkthrough had written into the owner's `dos_worker1-data` volume was
  deleted (after checking its content was exactly `garbage`) using a
  throwaway `alpine` container. A check afterwards showed no other file in that
  volume modified in the preceding two hours. The deleted `dos` worker1
  *container* must be recreated by the owner from that project's Compose
  file; its data volume is intact.
- **Chunk size confirmed as 1 MB = 1,000,000 bytes** (DEC-005 → ACCEPTED,
  replacing the provisional 1 MiB). Changed `config.DefaultChunkSize`, its
  test, `.env.example`, the integration and e2e tests (which now derive sizes
  from `config.DefaultChunkSize`), and all docs; recorded as a deviation from
  the architecture document in `ARCHITECTURE.md` §10. The Phase 1 e2e test
  payload (1,045,000 bytes) became oversized and was reduced to 990,000 bytes.

---

## Phase 2 — Membership and failure detection (2026-11-09)

### What was implemented

- **`internal/config`**: `LoadController` (listen address, `HEARTBEAT_INTERVAL`,
  `DEAD_AFTER`, `REPAIR_GRACE`, `FAILURE_CHECK_INTERVAL`; rejects
  `DEAD_AFTER <= HEARTBEAT_INTERVAL`); worker `CONTROLLER_URL` and
  `ADVERTISE_ADDR` (default `http://<WORKER_ID>:<port>`); duration and URL
  loaders.
- **`internal/wire`**: register/heartbeat request and response types,
  heartbeat actions, `/cluster` view types, `REGISTRATION_SUPERSEDED` code.
- **`internal/controller/registry`**: worker states `SYNCING`/`ALIVE`/`DEAD`,
  epochs from boot time (DEC-029), `BeginRegistration` /
  `CompleteRegistration` (epoch-checked), `Heartbeat` (reregister on
  unknown, `DEAD`, or stale epoch), `CheckLiveness`, `Alive()` (excludes
  `SYNCING`), `RunFailureDetector`, derived repair eligibility. Injectable
  clock.
- **`internal/controller/api`**: register (validate → SYNCING → reconcile hook
  → ALIVE if current → reply), heartbeat, `/healthz`, `/cluster`;
  `InventoryReconciler` interface with `NoopReconciler` (Phase 6 supplies the
  real one).
- **`internal/worker/server`**: the on/off write flag became `writeGate`, which
  counts admitted requests and can close-and-drain (DEC-030);
  `OpenWrites`/`CloseWritesAndDrain` replace `SetAcceptingWrites`.
- **`internal/worker/store`**: `Stats()` (chunk count, used bytes) and
  `FreeBytes()` (`statfs` on Linux; `-1` on Windows).
- **`internal/worker/agent`**: the membership loop (close+drain → snapshot →
  register with backoff+jitter → open → heartbeat → on `reregister`, repeat).
- **`cmd/controller`**: real binary with failure detector and graceful
  shutdown. **`cmd/worker`**: runs the agent when `CONTROLLER_URL` is set,
  standalone otherwise (DEC-026, narrowed).
- **`docker-compose.yml`**: project `objstore`, controller + `worker1..4`,
  one named volume each, loopback-only published port, optional container
  prefix (DEC-032), healthchecks.
- **Tests**: registry (fake clock), API handlers, gate draining, agent (fake
  controller), in-process cluster integration tests, Compose e2e test.

### Important decisions

DEC-029 (epochs from boot time), DEC-030 (draining write gate), DEC-031
(heartbeat failures keep the gate open), DEC-032 (Compose naming and port),
DEC-033 (controller owns liveness timing; `FAILURE_CHECK_INTERVAL`). DEC-026
narrowed to an explicit standalone mode. DEC-006 updated: draining removes
the residual "late repair PUT" case it had anticipated.

**`SYNCING` evaluation (owner's §4.5 question), as built.** The worker is
unusable while `SYNCING` (excluded from `Registry.Alive()`), its writes are
gated and drained before the snapshot, and `ALIVE` is reached only through
an epoch-checked transition. Tests cover a superseded registration
(`TestSupersededRegistrationCannotMarkAlive`, `TestSupersededDuringReconcile`),
death during sync (`TestDeathDuringSyncBlocksCompletion`), the drain
(`TestCloseWritesDrainsInFlightPut`), and refusing writes until registered
(`TestWritesRefusedUntilRegistered`). The metadata-dependent races (stale
`OK` rows counted as effective, false "missing") are tested in Phase 6, when
reconciliation has metadata to compare.

### Problems encountered and resolutions

1. **Agent test hung.** The test helper returned a buffered result channel;
   `TestRunStopsOnCancel` read the single value and the cleanup then waited on
   the same channel forever. Replaced it with a channel closed on completion
   plus an error variable.
2. **Sleeps in agent tests.** Two tests used fixed sleeps to let failures
   happen; the fake controller now counts rejected requests, and the tests wait
   for a count.
3. **Registry test arithmetic.** A repair-eligibility check forgot that an
   earlier step had already advanced the fake clock; the test, not the code,
   was wrong.
4. **Escaped newline lost** when editing a Go string through a Python heredoc;
   fixed with a direct edit.
5. **Phase 1 e2e payload too large** after the chunk-size change (see above).
6. **Docker safety.** Every manual Docker command was run with the
   `objstore-` prefix after checking that no such names, volumes, or listener
   on port 8080 existed; the e2e test uses unique project names, prefixes and
   ports, and removes only its own project. Native processes were stopped by
   PID. No leftover resources after any run.

### Tests performed and results (2026-11-09)

- `go build`, `go vet` (default and `-tags e2e`), `gofmt -l` (empty),
  `go tool staticcheck` (default and `-tags e2e`): pass.
- `go test -count=3 ./...` (Windows): pass.
- `go test -race -count=3 ./...` in `golang:1.27.3` (Linux): pass.
- `go test -tags e2e -count=1 -v ./test/e2e/...`: `TestComposeClusterMembership`
  (3 subtests) and `TestWorkerContainer` (5 subtests) pass.
- Manual Compose walkthrough with default timings and
  `OBJSTORE_CONTAINER_PREFIX=objstore-`: 4 `ALIVE` ≈ 1 s after `up`; worker2
  `DEAD` 10 s after `docker stop`; `ALIVE` ≈ 1 s after `docker start` with a
  new epoch; 4 `ALIVE` ≈ 2 s after `docker restart` of the controller;
  `docker compose down -v` removed only `objstore_*` resources.
- Manual standalone worker walkthrough re-run with 1 MB chunks (all numbers in
  `RUN_AND_TEST.md` §4.2 observed).
- Native controller + worker pair: worker registers, `ALIVE`.

### Known limitations

- Inventory reconciliation is a no-op until Phase 6 (no metadata yet).
- `free_bytes` is `-1` on Windows (development only).
- `/cluster` lists only workers; chunk and repair counters come in Phases 6–8.
- Network-partition simulation is not yet demonstrated (optional scenario 14).
- The in-memory registry means `/cluster` is briefly empty after a controller
  restart (by design).

### Work remaining

Phase 3: metadata DB (SQLite), upload path (chunking, placement on `ALIVE`
workers, claim-before-write, `pending_deletes`, atomic commit, windowed
upload).

---

## Phase 3 — Write path (2026-11-09)

### What was implemented

- **Dependencies:** `modernc.org/sqlite` v1.62.0 (pure Go, DEC-003) and
  `golang.org/x/sync` v0.24.0 (`errgroup`, DEC-004). Verified in the driver
  source that the `_txlock=immediate` and `_pragma` DSN options exist.
- **`internal/controller/meta`**: embedded schema (`objects`, `chunks`,
  `replicas`, `pending_deletes`, `schema_version`), `Open` with WAL / FULL
  sync / foreign keys / busy timeout / `BEGIN IMMEDIATE`, and the write-path
  transactions `CreateObject`, `ClaimChunk`, `MarkReplicaOK`, `ReplaceReplica`,
  `CommitObject` (re-verifies I1, I2, replication), `MarkDeleting`,
  `DuePendingDeletes`, `CompletePendingDelete`, `FailPendingDelete`,
  `FinalizeDeletedObjects`, plus `GetObject`, `ChunkMap`, `Counts` (DEC-034).
- **`internal/controller/placement`**: `Eligible` (ALIVE, not excluded,
  enough free space) and the `Random` policy (DEC-038).
- **`internal/controller/workerclient`**: PUT/GET/DELETE chunk with connect
  and request timeouts and typed worker errors.
- **`internal/controller/custommeta`**: DEC-013 validation (still
  provisional).
- **`internal/controller/upload`**: the coordinator (DEC-035, DEC-036,
  DEC-039).
- **`internal/controller/janitor`**: minimal cleanup loop (DEC-037).
- **`internal/controller/api`**: `POST /api/v1/objects` (concurrency cap,
  per-read deadlines, error mapping), `/healthz` now pings the database.
- **`internal/config`**: controller settings `DB_PATH`, `CHUNK_SIZE`,
  `REPLICATION_FACTOR`, `UPLOAD_WINDOW`, `MAX_OBJECT_SIZE`,
  `MAX_CONCURRENT_UPLOADS`, `PLACEMENT_MIN_FREE_BYTES`,
  `WORKER_CONNECT_TIMEOUT`, `WORKER_REQUEST_TIMEOUT`, `CLIENT_READ_TIMEOUT`,
  `JANITOR_INTERVAL`.
- **`cmd/controller`**: opens the database, runs the janitor, serves uploads;
  shutdown order is HTTP drain → background loops → database close.
- **`docker-compose.yml`**: controller gets the write-path settings and
  `DB_PATH=/data/meta.db` on its volume; `CHUNK_SIZE` is shared with the
  workers.
- **Tests**: unit tests for every new package; in-process integration tests
  with a worker fault injector; a sequential-vs-windowed benchmark; a Compose
  upload e2e test. Test helpers were generalised to `testing.TB` so the
  benchmark can reuse them.

### Important decisions

DEC-034 to DEC-039. The janitor was planned for Phase 5 but a minimal
version was needed now, because the Phase 3 acceptance criterion says a
failed upload leaves no object or chunk behind.

### Problems encountered and resolutions

1. **End-of-file detection (DEC-039).** Review of the first draft found that
   `io.ReadFull` cannot tell a short last chunk from a truncated body. A
   later `NextPart` call would also have caught the truncation, so no
   incomplete object could actually have been committed, but the check now
   happens where the data is read.
2. **Wrong error reported for an oversized upload (500 instead of 400).**
   When the read loop stopped with a client error it cancelled the in-flight
   chunk writes, whose `context canceled` error was then returned first. The
   read error now takes precedence. Found by `TestUploadTooLarge`.
3. **Test-helper bugs:** a truncation guard that always failed, and a
   cleanup check that counted the files of committed objects as leftovers.
   Both fixed in the tests.
4. **Janitor starvation risk** (found in design review): fetching due deletes
   without a worker filter could fill every batch with entries for a dead
   worker. `DuePendingDeletes` now takes the ALIVE worker list.
5. **Shell quoting** broke a large documentation heredoc; doc edits are now
   applied with Python scripts in the scratchpad.

### Tests performed and results (2026-11-09)

- `go build`, `go vet` (default and `-tags e2e`), `gofmt -l` (empty),
  `go tool staticcheck` (default and `-tags e2e`): pass.
- `go test -count=3 ./...` (Windows): pass. `go test -race -count=3 ./...` in
  `golang:1.27.3` (Linux): pass.
- `go test -tags e2e -count=1 -v ./test/e2e/...`: `TestComposeClusterMembership`,
  `TestComposeUpload`, `TestWorkerContainer` pass; no leftover resources.
- `BenchmarkUploadWindow` (3 runs): medians 70.4 / 112.7 / 122.1 / 112.2 MB/s
  for windows 1 / 2 / 4 / 8.
- Manual Compose walkthrough (`OBJSTORE_CONTAINER_PREFIX=objstore-`, guarded):
  5.5 MB upload → 201, 6 chunks, checksum matching `sha256sum`, 12 files
  (5/4/0/3 per worker), each chunk on 2 distinct workers, `/cluster` chunk
  counts matching, `worker inspect` VALID, the three 400 cases, and
  `503 INSUFFICIENT_WORKERS` with three workers stopped; `down -v` left
  nothing.

### Known limitations

- **Controller crash mid-upload** leaves an `UPLOADING` object (invisible to
  clients) and its files; Phase 9 startup recovery cleans these up.
- Download, metadata GET/PATCH, list, and delete are Phases 4–5. Stored
  objects can be checked only via tests and worker volumes.
- Random placement is uneven for small samples (one run: 0 of 12 replicas on
  worker3); rendezvous hashing is stretch S1.
- DEC-013 (string-only metadata) remains provisional.

### Work remaining

Phase 4: download with per-chunk verification, replica fallback, pre-flight
503, mid-stream abort, and marking `CORRUPT` replicas.

---

## Phase 4 — Read path (2026-11-09)

### What was implemented

- **`internal/controller/download`**: the download coordinator, which also
  writes the HTTP response: READY check, pre-flight over effective replicas,
  chunk 0 fetched before the response starts, per-chunk size + SHA-256
  verification (worker asked for at most size + 1 bytes), random replica
  order with fallback, failure classification and `CORRUPT` marking on a
  detached context, flush after every chunk, abort mid-stream with
  `http.ErrAbortHandler`, whole-object checksum check before the last chunk,
  `HEAD` without fetching, sanitised `Content-Disposition`, `ETag` and
  `X-Object-Checksum-SHA256`.
- **`meta.MarkReplicaCorrupt`**: conditional `OK → CORRUPT`.
- **`internal/controller/api`**: `GET /api/v1/objects/{object_id}` (also HEAD)
  with a concurrent-download cap; upload 503 messages no longer include
  wrapped error details.
- **`internal/config`** / **Compose**: `MAX_CONCURRENT_DOWNLOADS` (32).
- **Tests**: coordinator unit tests (fake worker client, real SQLite);
  integration tests against real workers; a Compose download e2e test.

### Important decisions

DEC-040 (failure classification), DEC-041 (chunk 0 first, flush per chunk,
abort), DEC-042 (whole-object checksum guard), DEC-043 (sequential,
bounded, HEAD), DEC-044 (client errors never name workers).

### Problems encountered and resolutions

1. **503 with the wrong `Content-Length`.** The first draft set the object's
   headers before fetching chunk 0; when chunk 0 failed, the JSON error
   inherited `Content-Length: <object size>` and the client could not parse
   it. Headers are now set only after chunk 0 is verified. Found by
   `TestNoValidReplicaOnChunkZeroIsClean503`.
2. **Abort delivered nothing.** Without flushing, the 200 and earlier chunks
   were still buffered when the connection was cut, so the client saw a bare
   connection failure. The response is now flushed after the status line and
   every chunk. Found by `TestMidStreamFailureTruncates` and
   `TestAssemblyChecksumGuard`.
3. **Worker IDs leaked to clients.** The manual walkthrough showed a 503
   message listing failing workers; an upload `UPLOAD_FAILED` message also
   named one, and upload 503s could include wrapped database errors naming
   workers. Messages are now generic, with details in the log (DEC-044), and
   tests assert it.
4. **Wrong curl message in the draft guide.** I had written the message from
   memory; the verified run prints `curl: (18) end of response with 1500000
   bytes missing`. Corrected.
5. **Test bug:** one integration test compared a finished upload against
   empty data.

### Tests performed and results (2026-11-09)

- `go build`, `go vet` (default and `-tags e2e`), `gofmt -l` (empty),
  `go tool staticcheck` (default and `-tags e2e`): pass.
- `go test -count=3 ./...` (Windows, 16 packages): pass.
  `go test -race -count=3 ./...` in `golang:1.27.3` (Linux): pass.
- `go test -tags e2e -count=1 -v ./test/e2e/...`: `TestComposeClusterMembership`,
  `TestComposeDownload`, `TestComposeUpload`, `TestWorkerContainer` pass; no
  leftovers.
- Manual Compose walkthroughs (guarded `objstore-` names): download identical
  with correct headers; HEAD; 404/400; identical with a holder stopped; 6 of 6
  identical after appending bytes to one copy, with one
  `corruption_detected` log line; both copies of chunk 0 corrupted → 503 (twice:
  verification, then pre-flight); both copies of chunk 2 corrupted → 200,
  2,000,000 correct bytes, curl exit 18.

### Known limitations

- Sequential fetch only (prefetch is stretch S2); download throughput is
  bounded by one chunk round trip at a time.
- `CORRUPT` replicas are not repaired yet (Phases 7–8); an object with a
  `CORRUPT` copy is under-replicated until then.
- A copy damaged in transit (not on disk) would be marked `CORRUPT`; very
  unlikely with TCP, and harmless once repair exists.

### Work remaining

Phase 5: metadata GET (chunk map) and PATCH, list with pagination, client
delete, conformance checklist.

---

## Phase 5 — Complete API surface (2026-11-09)

### What was implemented

- **`internal/controller/meta`**: `UpdateMetadata` (one read-merge-write
  transaction, If-Match version check, no-op detection) and `ListObjects`
  (keyset pagination, `limit + 1` probe).
- **`internal/controller/custommeta`**: `ParsePatch` (strings or null) and
  `Merge` (limits on the merged result).
- **`internal/controller/api/objects.go`**: `GET`/`PATCH .../metadata`,
  `GET /api/v1/objects`, `DELETE /api/v1/objects/{id}`; the cursor codec;
  `If-Match` parsing; delete kicks the janitor through `Config.OnDelete`.
- **`internal/wire`**: `ObjectMetadata`, `ChunkView`, `ReplicaView`,
  `ObjectSummary`, `ObjectList`; codes `VERSION_CONFLICT`, `INVALID_PAGINATION`.
- **`internal/controller/download`**: `corruption_detected` is logged only
  when a replica row actually changes (a delete racing a download produced
  misleading log lines).
- **`cmd/objctl`**: the full client (DEC-049). **`docker-compose.yml`**:
  `client` service in the `tools` profile.
- **Tests**: store tests, `custommeta` tests, integration tests for every
  endpoint and race, `TestSpecConformance`, `TestObjctl`, and the Compose
  `TestComposeObjectAPI`. `buildWorker` was generalised to `buildBinary`, and
  the worker fault injector gained a GET latency.

### Important decisions

DEC-045 to DEC-050. Client delete reuses the Phase 3 abort path and janitor.

### Problems encountered and resolutions

1. **PATCH rejected plain `curl -d`** (form-urlencoded content type). Found
   in the manual walkthrough; the `Content-Type` check was removed (DEC-045)
   and the test inverted to assert acceptance.
2. **Misleading `corruption_detected` logs** when a download raced a delete
   (the file vanished, so the read saw 404, but there was no row to mark).
   Now logged only when a row changes.
3. **`objctl list` printed `"objects": null`** on an empty store. Fixed, with
   a test.
4. **409 message carried an internal `meta: ` prefix.** Trimmed.
5. **Test bugs:** comparing structs containing maps with `!=` (compile
   error), a pointless self-comparison helper (replaced with a comparison
   against the uploaded data), and an invalid percent-encoded cursor that
   Go's query parser drops before the handler sees it.
6. **A large documentation edit failed on escaping** inside an inline Python
   heredoc; doc edits use scratchpad scripts or the Edit tool.

### Tests performed and results (2026-11-09)

- `go build`, `go vet` (default and `-tags e2e`), `gofmt -l` (empty),
  `go tool staticcheck` (default and `-tags e2e`): pass.
- `go test -count=3 ./...` (Windows, 16 packages): pass.
  `go test -race -count=3 ./...` (Linux container): pass.
- `go test -tags e2e -count=1 -v ./test/e2e/...`: 5 tests pass; no leftovers.
- `TestSpecConformance`: 19 of 19 rows pass.
- Manual Compose walkthroughs (guarded `objstore-` names): metadata with
  `ETag "v1"` and replica locations; PATCH add/update/remove via plain `curl -d`;
  If-Match 409 then 200; invalid value 400; list paging 2 + 1 with a cursor;
  `limit=0` 400; delete 204 then 404 everywhere, files gone from all 4 workers,
  `object_purged`; every `objctl` command of `RUN_AND_TEST.md` §4.6 in the
  client container and natively.

### Known limitations

- Listing order uses the upload *start* time, so an object committing during
  a listing may be missed by that pass (DEC-047).
- A deleted object whose holder never returns keeps an invisible `DELETING`
  row and pending deletes (worker decommissioning is out of scope).
- DEC-013 (string-only metadata values) is still provisional.

### Work remaining

Phase 6: full inventory reconciliation on registration, `/cluster` chunk
totals, audit v1 (I1, I2, I3, I5, I7).
