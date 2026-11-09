# Decision Log

Significant implementation decisions, in the order they were made. The original
architecture's decisions D1–D22 live in
[`reference/Object_Store_Architecture.md`](reference/Object_Store_Architecture.md) §1.6
and are **not** repeated here; this log records decisions taken during
implementation: resolutions of ambiguities in that document, deviations from it,
and technology choices it deliberately left open.

**Status values:** `ACCEPTED` (in force) · `PROVISIONAL` (in force, awaiting
explicit owner confirmation; may change) · `SUPERSEDED by DEC-nnn`.

| ID | Title | Status | Phase |
| --- | --- | --- | --- |
| DEC-001 | Go 1.27, module path `objstore` | ACCEPTED | 0 |
| DEC-002 | Standard-library HTTP and routing, no framework | ACCEPTED | 0 |
| DEC-003 | SQLite via pure-Go `modernc.org/sqlite` | ACCEPTED | 0 (used from 3) |
| DEC-004 | `golang.org/x/sync` for bounded fan-out | ACCEPTED | 0 (used from 3) |
| DEC-005 | Chunk size = 1 MiB (1,048,576 bytes) | PROVISIONAL | 0 |
| DEC-006 | `SYNCING` worker state, registration epochs, worker write gating | ACCEPTED | 0 (built in 2/6) |
| DEC-007 | `pending_deletes` table for exact physical cleanup | ACCEPTED | 0 (built in 3/5/9) |
| DEC-008 | Repair target rule and in-place corrupt repair | ACCEPTED | 0 (built in 7/8) |
| DEC-009 | Periodic inventory pull for the orphan sweep | ACCEPTED | 0 (built in 9) |
| DEC-010 | Inventory endpoint is hosted on the worker | ACCEPTED | 0 (built in 1) |
| DEC-011 | Claims and commit are conditional on object status | ACCEPTED | 0 (built in 3) |
| DEC-012 | Chunk file format: header carries the checksum | ACCEPTED | 0 (built in 1) |
| DEC-013 | Custom metadata: string values, size limits | PROVISIONAL | 0 (built in 3/5) |
| DEC-014 | PATCH: `null` removes a key; optional `If-Match` → 409 | ACCEPTED | 0 (built in 5) |
| DEC-015 | Error body format, error codes, malformed IDs → 400 | ACCEPTED | 0 |
| DEC-016 | Max object size 5 GiB, over-limit → 400 | ACCEPTED | 0 (built in 3) |
| DEC-017 | Content-Type taken from the multipart file part | ACCEPTED | 0 (built in 3) |
| DEC-018 | Race detector runs inside a Linux `golang` container | ACCEPTED | 0 |
| DEC-019 | `staticcheck` pinned through the `go.mod` `tool` directive | ACCEPTED | 0 |
| DEC-020 | Heartbeat replies carry an explicit `action` | ACCEPTED | 0 (built in 2) |
| DEC-021 | Wire formats: RFC 3339 UTC timestamps, lowercase-hex SHA-256 | ACCEPTED | 0 |
| DEC-022 | LF line endings enforced via `.gitattributes` | ACCEPTED | 0 |
| DEC-023 | One Alpine image for all binaries, non-root user | ACCEPTED | 1 |
| DEC-024 | Chunk PUT always writes and atomically replaces | ACCEPTED | 1 |
| DEC-025 | Chunk GET loads into memory under a striped lock, bounded concurrency | ACCEPTED | 1 |
| DEC-026 | Phase 1 workers run standalone with the write gate open | ACCEPTED | 1 (superseded in 2) |
| DEC-027 | Unreadable chunk header → 500 `CHUNK_CORRUPT` | ACCEPTED | 1 |
| DEC-028 | Identifier package is `internal/ids` (not `chunkid`) | ACCEPTED | 1 |

---

## DEC-001 — Go 1.27, module path `objstore`

- **Context.** Architecture §9.1 left the language open (recommended Go). The
  owner chose Go. Go was not installed on the development machine.
- **Alternatives.** Java (architecture's fallback); Go with a hosted module path
  such as `github.com/<owner>/<repo>`.
- **Choice.** Go 1.27 (installed 1.27.3 via `winget install GoLang.Go`);
  `go.mod` declares `go 1.27.0` so any 1.27.x toolchain builds it. Module path
  `objstore` (no host component).
- **Why.** No remote repository exists yet, and inventing a hosting path would
  be wrong. A dotless module path works for an application that is never
  imported by others.
- **Consequences.** If the project is published, rename the module with
  `go mod edit -module <path>` plus a search-and-replace of imports. A future
  standard-library package named `objstore` would collide (negligible risk).

## DEC-002 — Standard-library HTTP and routing, no framework

- **Context.** Two HTTP servers (controller, worker) and one client.
- **Alternatives.** chi, gin, echo, gorilla/mux.
- **Choice.** `net/http` with `http.ServeMux` method+wildcard patterns
  (`"GET /api/v1/objects/{object_id}"`), `r.PathValue`, and `log/slog`.
- **Why.** Go ≥ 1.22 routing covers everything the API needs; fewer
  dependencies; streaming request/response bodies are first-class.
- **Consequences.** Middleware (request IDs, logging, recovery) is a few
  hand-written wrapper functions.

## DEC-003 — SQLite via pure-Go `modernc.org/sqlite`

- **Context.** Architecture D7 chose an embedded transactional DB; §9.2
  recommends SQLite with WAL and a busy timeout.
- **Alternatives.** `github.com/mattn/go-sqlite3` (cgo); bbolt/Badger (KV, no
  SQL for reconciler queries); Postgres (extra container).
- **Choice.** `modernc.org/sqlite` through `database/sql`. Settings: WAL
  journal, `busy_timeout`, foreign keys on, `synchronous=FULL` for commit
  durability, and **all write transactions start with `BEGIN IMMEDIATE`** (DSN
  `_txlock=immediate`).
- **Why.** No C toolchain is needed on Windows or in the container build, so
  images can be static. `BEGIN IMMEDIATE` takes the write lock up front,
  avoiding `SQLITE_BUSY` failures when a deferred read transaction later tries
  to upgrade to a write while another writer holds the lock.
- **Consequences.** Somewhat slower than the cgo driver (irrelevant next to
  network and disk). Writers are serialised, which matches the architecture's
  "short transactions, never across a network call" rule (§3.5). The exact
  version is pinned when the dependency is added in Phase 3.

## DEC-004 — `golang.org/x/sync` for bounded fan-out

- **Context.** Bounded concurrency is required everywhere (§5.1, D19): two
  parallel replica writes per chunk, upload window, repair concurrency.
- **Alternatives.** Hand-rolled `sync.WaitGroup` + channel semaphores.
- **Choice.** `errgroup.Group` (with `SetLimit` and context cancellation) and
  `semaphore.Weighted`.
- **Why.** Maintained by the Go team, small, gives correct first-error
  cancellation semantics that are easy to get subtly wrong by hand.
- **Consequences.** One small external dependency. (It is already present in
  `go.sum` transitively through `staticcheck`; it becomes a direct dependency
  in Phase 3.)

## DEC-005 — Chunk size = 1 MiB (1,048,576 bytes) — PROVISIONAL

- **Context.** The specification says "1 MB"; the architecture (§3.1, D3) chose
  1 MiB and requires one definition used everywhere. The owner will confirm.
- **Alternatives.** 1 MB = 1,000,000 bytes.
- **Choice.** 1,048,576 bytes, held in one constant/config value
  (`CHUNK_SIZE`), never repeated as a literal.
- **Why.** Matches the architecture; power-of-two buffers.
- **Consequences.** Switching to 1,000,000 is a one-line default change plus
  doc/test expectations (e.g. a "5.5 MiB → 6 chunks" checkpoint). Tests must
  derive expected chunk counts from the configured size, not hard-code them.
  `CHUNK_SIZE` is configurable only so tests can use small chunks.

## DEC-006 — `SYNCING` worker state, registration epochs, worker write gating

- **Context.** Architecture §4.5 marks a registering worker `SYNCING`, reconciles
  its inventory, then marks it `ALIVE`; but the §3.3 state diagram omits
  `SYNCING`, and §2.2 says the failure detector sets `SYNCING` "from heartbeat
  age". The owner asked for an evaluation of whether a worker used during
  reconciliation causes races.
- **Analysis (races if the worker were `ALIVE` during reconciliation).**
  1. *Placement vs. inventory snapshot.* The inventory is a snapshot taken
     before registration. A chunk placed on the worker after the snapshot has
     an `OK` row but is absent from the snapshot, so reconciliation would drop
     the row ("missing"), and the file would later be swept as an orphan. No
     data loss (the other replica exists) but false under-replication, wasted
     repair, and needless deletion.
  2. *Fast restart.* A worker restarting inside `DEAD_AFTER` is still `ALIVE`
     in the registry, so the controller may PUT to it between the snapshot and
     registration, which causes race 1 again.
  3. *Reads and pre-flight.* A worker returning with a wiped volume would have
     its stale `OK` rows counted as effective: the download pre-flight passes,
     the read gets 404, and if no other replica works the download aborts
     mid-stream instead of returning a clean 503.
  4. *Repair accounting.* Effective-replica counts are overstated until
     reconciliation finishes; the reconciler may also choose dead sources.
- **Alternatives.** (a) Mark `ALIVE` first and rely on fallbacks (rejected:
  races 1–4). (b) Timestamp each replica row and only reconcile rows older than
  the snapshot (rejected: needs cross-machine clock comparison, which D11
  forbids). (c) `SYNCING` state plus worker-side gating (chosen).
- **Choice.**
  - `SYNCING` is a **registration-driven** state, not a heartbeat-age state.
    A `SYNCING` worker is excluded from placement, read selection, and repair
    source/target selection; its replicas are **not effective**. Heartbeats are
    accepted and refresh `last_heartbeat`. The failure detector may still move
    `SYNCING → DEAD` if heartbeats stop.
  - State machine: `(unknown) → SYNCING` on register; `SYNCING → ALIVE` when
    reconciliation finishes; `ALIVE|SYNCING → DEAD` on heartbeat silence;
    `DEAD → SYNCING` on re-register. Any heartbeat from an unknown or `DEAD`
    worker gets a "re-register" reply (DEC-020).
  - Each registration gets a monotonically increasing **epoch**.
    Reconciliation runs synchronously in the register handler as short DB
    transactions (no network calls). It flips the worker to `ALIVE` only if
    the epoch it started with is still current, so a slow, superseded
    reconciliation can never mark a worker `ALIVE`.
  - **Worker-side gating:** until a worker receives a successful registration
    reply, its chunk PUT and DELETE endpoints return `503 NOT_REGISTERED`.
    The inventory snapshot is taken while writes are blocked. This closes
    race 2 deterministically. GET and inventory listing stay available because
    they are read-only.
- **Consequences.** Brief unavailability of a returning worker for
  placement/reads (reconciliation is DB-only and fast). One subtle benign case
  remains: a repair PUT started before the worker went `DEAD`, landing after
  the snapshot, can still cause one spurious "missing" drop; the orphan sweep
  and reconciler converge it. Tests for races 1–3 are required in Phases 2/6.
  `docs/ARCHITECTURE.md` documents the corrected state machine.

## DEC-007 — `pending_deletes` table for exact physical cleanup

- **Context.** Two problems the architecture leaves open:
  1. *Orphan-delete race.* Chunk X is repaired onto worker3 and worker2's row
     is dropped. worker2 returns with X, so X is classified as an orphan and
     scheduled for deletion. Meanwhile worker3 dies and the reconciler picks
     worker2 as the repair target for X (it holds no row), PUTs X, and marks it
     `OK`. The previously scheduled orphan delete then removes X, leaving an
     `OK` row whose file is gone.
  2. *Timeouts are not failures.* §4.1 replaces a failed `PENDING` row with
     a row on a different worker; but a timed-out PUT may have succeeded, so
     the original worker may hold an unclaimed file that only an eventual
     orphan sweep would remove.
- **Alternatives.** Re-check the DB immediately before each DELETE (still a
  check-then-act window); in-memory per-(chunk, worker) locks (lost on
  restart); rely solely on the orphan sweep (slow, and does not fix race 1).
- **Choice.** A durable table `pending_deletes(chunk_id, worker_id, reason,
  created_at, attempts)`, unique on `(chunk_id, worker_id)`.
  - A failed or timed-out replica write moves its claim into
    `pending_deletes` **in the same transaction** that records the
    replacement `PENDING` row.
  - Orphan detection inserts into `pending_deletes` **in the same
    transaction** that confirms no replica row claims `(chunk, worker)`.
  - Object deletion converts each replica row into a `pending_deletes` entry.
  - Placement and repair never choose a worker that has a `pending_deletes`
    entry for that chunk (checked inside the claim transaction).
  - The janitor issues idempotent DELETEs and removes an entry only after the
    worker confirms (204, including "already absent").
- **Why.** Every decision to delete becomes a durable fact that is mutually
  exclusive with a claim, enforced by SQLite's serialised writes. Cleanup is
  exact and survives controller restarts.
- **Consequences.** One extra table and a janitor loop over it. The orphan
  sweep (DEC-009) remains a backstop for the one residual case: a delayed PUT
  arriving at the worker after the janitor's DELETE was confirmed.

## DEC-008 — Repair target rule and in-place corrupt repair

- **Context.** §4.7(b) says the target must hold "no replica of this chunk" yet
  should "prefer the worker whose replica is CORRUPT", which holds one.
- **Choice.** A target must hold no `OK` or `PENDING` replica of the chunk and
  no `pending_deletes` entry for it. When the preferred target holds a
  `CORRUPT` row, that row transitions `CORRUPT → PENDING` (as in the §3.3
  replica diagram) and the atomic temp-then-rename write replaces the bad file.
  A schema-level `UNIQUE(chunk_id, worker_id)` on replicas enforces invariant
  I3 for every state.
- **Consequences.** Repair never creates duplicate replica rows; a failed
  in-place repair leaves the row `CORRUPT` for the next round.

## DEC-009 — Periodic inventory pull for the orphan sweep

- **Context.** Invariant I4 ("after the janitor has run, no chunk file is
  unclaimed") cannot hold if inventories arrive only at registration.
- **Choice.** The janitor periodically (`INVENTORY_SWEEP_INTERVAL`, default
  5 min) calls `GET /internal/workers/{worker_id}/chunks` on each `ALIVE`
  worker and applies the orphan rule from §4.5 through `pending_deletes`. The
  janitor's own cadence is `JANITOR_INTERVAL` (default 10 s). Both parameters
  are additions to §5.4.
- **Consequences.** A full inventory listing every few minutes per worker:
  cheap at this scale.

## DEC-010 — Inventory endpoint is hosted on the worker

- **Context.** The spec lists `GET /internal/workers/{worker_id}/chunks`
  without saying which process serves it.
- **Choice.** Each worker serves it for its own ID and returns `404` for any
  other `worker_id`. The controller's metadata view of a worker's replicas is
  exposed separately through `/cluster` and object metadata.
- **Why.** Architecture §2.3 gives the worker's Chunk API the "list its
  inventory" duty; it reports disk truth, not metadata.

## DEC-011 — Claims and commit are conditional on object status

- **Context.** The janitor may abort an upload as stale (`UPLOADING →
  DELETING`) while a slow handler is still running.
- **Choice.** Every replica-claim insert and the final commit are conditional
  in SQL (`... WHERE status = 'UPLOADING'`). If the condition fails, the
  handler stops and the upload fails; the delete path already owns cleanup.
  Repairs likewise claim only for `READY` objects and mark `OK` only if the
  object is still `READY` and the row is still `PENDING`.
- **Consequences.** No resurrected or half-committed objects. Bytes written by
  a losing handler end up as orphans, cleaned by `pending_deletes` or the sweep.

## DEC-012 — Chunk file format: header carries the checksum

- **Context.** §9.3: sidecar file vs. header for the worker-side checksum.
- **Choice.** One file per chunk: a fixed-size header (magic, format version,
  payload length, SHA-256 of the payload) followed by the payload. Written via
  temp file → fsync → rename → fsync(dir). A debug command prints headers.
- **Why.** A single atomic rename covers data and checksum together; a sidecar
  needs two renames and can tear between them.
- **Consequences.** Files are not raw chunk bytes, so the inspection tool is
  needed for demos (`worker inspect FILE`). Corruption demos (appending/altering
  bytes) are still detected via the length and checksum. Directory fsync is a
  no-op on Windows; production runs in Linux containers.
- **Layout (fixed in Phase 1, `internal/worker/store/format.go`).** 48-byte
  header, big-endian: bytes 0–3 magic `OSCK`; byte 4 format version `1`;
  bytes 5–7 reserved (zero); bytes 8–15 payload size (uint64); bytes 16–47
  SHA-256 of the payload; payload follows. Directory layout:
  `DATA_DIR/chunks/<chunk_id>` (committed) and `DATA_DIR/tmp/<chunk_id>.*.tmp`
  (in progress, deleted at startup). The header records the *expected*
  size and checksum, and the file is renamed into place only after the
  received bytes matched both, so a committed header is always truthful.

## DEC-013 — Custom metadata: string values, size limits — PROVISIONAL

- **Choice.** Custom metadata is a flat JSON object of string → string. Limits
  (from §5.7): at most 50 keys, key ≤ 128 bytes, value ≤ 1 KiB, keys non-empty.
  Non-string values (numbers, booleans, nested objects) are rejected with 400.
  In a PATCH body, `null` is the deletion marker (DEC-014), not a stored value.
- **Why.** Mirrors S3 user metadata; easy to bound; no type ambiguity.
- **Consequences.** Clients wanting numbers must stringify them. Awaiting
  owner confirmation; relaxing to "any JSON scalar" later is backwards
  compatible.

## DEC-014 — PATCH: `null` removes a key; optional `If-Match` → 409

- **Context.** §9.4 and §9.5.
- **Choice.** PATCH is a JSON merge on `custom_metadata` (RFC 7396 semantics on
  a flat map): string values add or update; `null` removes; absent keys are
  untouched. Each object carries an integer `metadata_version`, incremented
  on every successful PATCH and returned as the `ETag` (`"v<version>"`) on
  metadata responses. If the request carries `If-Match` and it does not match
  the current version, the response is **409 Conflict**. Without `If-Match`
  the last writer wins (SQLite serialises them).
- **Consequences.** 409 has exactly one meaning in this system.

## DEC-015 — Error body format, error codes, malformed IDs → 400

- **Choice.** Every error response is
  `{"error": {"code": "<STABLE_CODE>", "message": "<human text>"}}` with
  `Content-Type: application/json`. Codes are listed in `docs/API.md`. A
  syntactically invalid object ID (not a canonical lowercase UUID) returns
  **400 `INVALID_OBJECT_ID`**; a well-formed but unknown or non-`READY` ID
  returns **404 `OBJECT_NOT_FOUND`**.
- **Why.** Stable codes let tests and clients branch without parsing text.

## DEC-016 — Max object size 5 GiB, over-limit → 400

- **Context.** §9.9: a configurable cap of "a few GiB".
- **Choice.** `MAX_OBJECT_SIZE` default 5,368,709,120 bytes. Exceeding it
  returns 400 `OBJECT_TOO_LARGE` and aborts the upload via the delete path.
  The spec's status table has no 413, and §5.8 maps "over limits" to 400.
- **Consequences.** The limit is enforced while streaming (the size is unknown
  up front with multipart), so some bytes may already be stored before
  rejection; the abort path cleans them up.

## DEC-017 — Content-Type taken from the multipart file part

- **Choice.** The object's `content_type` is the `Content-Type` header of the
  `file` part; if absent or empty, `application/octet-stream`. No content
  sniffing. The original filename comes from the part's `filename` parameter
  and is sanitised before being echoed in `Content-Disposition`.
- **Why.** Predictable; the client knows best; sniffing can mislabel.

## DEC-018 — Race detector runs inside a Linux `golang` container

- **Context.** `go test -race` requires cgo and a C toolchain. On the Windows
  development machine, `CGO_ENABLED=0` and no gcc is installed.
- **Choice.** Run race tests with
  `docker run --rm -v "<repo>:/src" -w /src golang:1.27.3 go test -race ./...`
  (exact commands in `docs/RUN_AND_TEST.md`). Plain `go test ./...` runs
  natively.
- **Consequences.** Race tests also exercise Linux semantics (fsync on
  directories, rename), which matches production.

## DEC-019 — `staticcheck` pinned through the `go.mod` `tool` directive

- **Choice.** `go get -tool honnef.co/go/tools/cmd/staticcheck@latest`
  (resolved to module `honnef.co/go/tools` v0.8.2) and run as
  `go tool staticcheck ./...`.
- **Why.** Every developer and CI runs the exact same version without a
  separate install step.
- **Consequences.** `go.mod`/`go.sum` list staticcheck's modules as indirect
  requirements; they are not linked into the binaries.

## DEC-020 — Heartbeat replies carry an explicit `action`

- **Choice.** `POST /internal/workers/heartbeat` returns 200 with
  `{"action": "ok"}` or `{"action": "reregister"}` (unknown worker, worker
  marked `DEAD`, or stale epoch). Non-2xx statuses are reserved for malformed
  requests and controller errors.
- **Why.** Control flow does not overload HTTP status codes; a proxy or
  restart error is never confused with a resync request.

## DEC-021 — Wire formats: RFC 3339 UTC timestamps, lowercase-hex SHA-256

- **Choice.** Timestamps are RFC 3339 with nanoseconds in UTC
  (`2026-11-09T14:10:00.123456789Z`). Checksums are 64-character lowercase hex
  SHA-256. Sizes are integer bytes.

## DEC-022 — LF line endings enforced via `.gitattributes`

- **Context.** Development happens on Windows; shell scripts and Dockerfiles
  run inside Linux containers and fail with CRLF.
- **Choice.** `* text=auto eol=lf` in `.gitattributes`; `.editorconfig` agrees.

## DEC-023 — One Alpine image for all binaries, non-root user

- **Context.** Three binaries (controller, worker, objctl) and a demo that
  needs `docker exec` into worker containers to inspect or corrupt files.
- **Alternatives.** One image per binary; distroless or `scratch` runtime.
- **Choice.** A single multi-stage `Dockerfile`: build all binaries with
  `CGO_ENABLED=0` in `golang:1.27.3-alpine`, copy them into `alpine:3.24`;
  Compose selects the command (`worker`, later `controller`). The runtime
  runs as uid/gid 10001 (`objstore`), and `/data` is created owned by it so a
  fresh named volume mounted there inherits that ownership.
- **Why.** One build, one cache, one tag. Alpine provides a shell, `find`,
  and busybox `wget` for demos and healthchecks at ~28 MB total. Non-root
  limits the damage of a compromised process.
- **Consequences.** Chunk files are created with mode `0600` owned by
  `objstore`; `docker exec` (which runs as the same user) can read and modify
  them for demos.

## DEC-024 — Chunk PUT always writes and atomically replaces

- **Context.** PUT must be idempotent (§5.5), and the controller repairs a
  corrupt replica by re-sending the correct bytes to the same worker (DEC-008).
- **Alternatives.** Check for an existing file with the same checksum and
  skip the write (requires reading and hashing the existing file under a
  lock, and must still replace it if it turns out to be corrupt).
- **Choice.** Every valid PUT writes a fresh temp file and atomically renames
  it over any existing file. The response is 201 if no file existed and 200
  if one was replaced.
- **Why.** One code path covers first write, retry, and in-place repair; no
  "existing file looks fine" decision is ever made by the worker.
- **Consequences.** A retried PUT costs one extra chunk write (rare). The
  200/201 distinction is informational; the controller must not rely on it
  for correctness.

## DEC-025 — Chunk GET loads into memory under a striped lock, bounded concurrency

- **Context.** A GET must never observe a half-replaced file, and a slow
  network client must not block writers. On Windows (where tests also run) an
  open file cannot be renamed over or deleted.
- **Alternatives.** Stream directly from the open file (holding the file open
  for the duration of the network transfer); rely on POSIX semantics of
  open-file-survives-rename (not portable).
- **Choice.** 256 striped `RWMutex`es keyed by a hash of the chunk ID. GET
  reads the file into memory under the read lock (at most
  `CHUNK_SIZE + header + 1` bytes), releases it, then sends. PUT's final
  rename and DELETE's unlink take the write lock. A semaphore
  (`MAX_CONCURRENT_READS`, default 16) bounds how many chunks are held in
  memory: worst case ≈ 16 MiB per worker.
- **Why.** Lock hold times are bounded by local disk speed, never by the
  network; memory is bounded; behaviour is identical on Linux and Windows.
- **Consequences.** A file grossly larger than a chunk is returned truncated to
  `CHUNK_SIZE + 1` bytes; that is still a size mismatch to the controller, so
  corruption stays detectable.

## DEC-026 — Phase 1 workers run standalone with the write gate open

- **Context.** DEC-006 requires workers to refuse writes until registered,
  but no controller exists in Phase 1.
- **Choice.** The write gate is implemented in Phase 1 (`503 NOT_REGISTERED`
  is tested), but `cmd/worker` opens it at startup. Phase 2 changes
  `cmd/worker` to open it only after a successful registration. `/healthz`
  reports both `registered` and `accepting_writes` so the mode is visible.
- **Consequences.** Superseded by the Phase 2 registration behaviour; no
  configuration flag is added for standalone mode.

## DEC-027 — Unreadable chunk header → 500 `CHUNK_CORRUPT`

- **Context.** With a header in each file (DEC-012), a corrupted header means
  the worker cannot even tell where the payload starts.
- **Alternatives.** Return the whole file as the body (the controller would
  then see a mismatch); return 404.
- **Choice.** 500 with the distinct code `CHUNK_CORRUPT`. Payload-level
  corruption (appended, truncated, or flipped payload bytes) is **not**
  detected by the worker on GET; those bytes are returned as stored.
- **Why.** 404 would be a lie (the file exists) and would be treated as
  "missing"; a distinct code lets the controller treat it as a failed
  verification (mark `CORRUPT` after cross-check, Phase 4/8).

## DEC-028 — Identifier package is `internal/ids` (not `chunkid`)

- **Context.** Phase 0 planned `internal/chunkid`. Worker-ID and object-ID
  validation belong with chunk-ID validation (the controller validates all
  three).
- **Choice.** `internal/ids` provides `NewObjectID`, `ValidObjectID`,
  `ValidChunkID`, `ChunkID`, `ParseChunkID`, `ValidWorkerID`. Worker IDs must
  match `^[a-z0-9][a-z0-9_-]{0,62}$`.
