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
| DEC-005 | Chunk size = 1 MB (1,000,000 bytes) | ACCEPTED (owner-confirmed) | 0, changed in 1 |
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
| DEC-026 | Standalone worker mode (no `CONTROLLER_URL`) keeps the write gate open | ACCEPTED (narrowed in 2) | 1 |
| DEC-027 | Unreadable chunk header → 500 `CHUNK_CORRUPT` | ACCEPTED | 1 |
| DEC-028 | Identifier package is `internal/ids` (not `chunkid`) | ACCEPTED | 1 |
| DEC-029 | Registration epochs start at controller boot time (ns) | ACCEPTED | 2 |
| DEC-030 | Write gate drains in-flight writes before the inventory snapshot | ACCEPTED | 2 |
| DEC-031 | Heartbeat failures do not close the write gate | ACCEPTED | 2 |
| DEC-032 | Compose: spec container names with optional prefix; loopback-only port | ACCEPTED | 2 |
| DEC-033 | Liveness timing owned by the controller (`HEARTBEAT_INTERVAL` sent at registration; `FAILURE_CHECK_INTERVAL`) | ACCEPTED | 2 |
| DEC-034 | Metadata schema and transaction rules; commit re-verifies invariants | ACCEPTED | 3 |
| DEC-035 | Upload failure handling: per-upload failed-worker set, atomic replace, abort on a detached context | ACCEPTED | 3 |
| DEC-036 | Upload resource bounds: window, pooled buffers, concurrent-upload cap, per-read client timeout | ACCEPTED | 3 |
| DEC-037 | Phase 3 janitor: pending deletes for ALIVE workers only, exponential backoff, kick on abort | ACCEPTED | 3 |
| DEC-038 | Placement free-space threshold `PLACEMENT_MIN_FREE_BYTES` = 100 MB | ACCEPTED | 3 |
| DEC-039 | End of the file part only on a clean `io.EOF` | ACCEPTED | 3 |
| DEC-040 | Read-path failure classification: bad data, missing, unreadable header → `CORRUPT`; unreachable → skip | ACCEPTED | 4 |
| DEC-041 | Chunk 0 fetched before the response starts; flush per chunk; abort on later failure | ACCEPTED | 4 |
| DEC-042 | Whole-object checksum verified before the last chunk is sent | ACCEPTED | 4 |
| DEC-043 | Sequential download, one chunk in memory, `MAX_CONCURRENT_DOWNLOADS`, `HEAD` without fetching | ACCEPTED | 4 |
| DEC-044 | Client error messages never name workers | ACCEPTED | 4 |

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

## DEC-005 — Chunk size = 1 MB (1,000,000 bytes) — ACCEPTED

- **Context.** The specification says "1 MB"; the architecture (§3.1, D3) chose
  1 MiB (1,048,576 bytes) and requires one definition used everywhere.
- **Alternatives.** 1 MiB = 1,048,576 bytes (the architecture's choice; was
  this decision's provisional value during Phases 0–1).
- **Choice.** **1,000,000 bytes**, confirmed by the owner after Phase 1. Held in
  one constant (`config.DefaultChunkSize`) and the `CHUNK_SIZE` setting,
  never repeated as a literal.
- **Why.** It is what the specification says; the owner chose the
  specification's wording over the architecture document's.
- **Consequences.** This **deviates from the architecture document** §3.1/D3
  (listed in `ARCHITECTURE.md` §9). Example: a 5,500,000-byte object has 6
  chunks (5 × 1,000,000 + 500,000). Tests derive expected chunk counts from the
  configured size. `CHUNK_SIZE` remains configurable only so tests can use
  small chunks. No stored data is affected (no data existed in this format).

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
  placement/reads (reconciliation is DB-only and fast). *(Phase 0 text:)* "One
  subtle benign case remains: a repair PUT started before the worker went
  `DEAD`, landing after the snapshot." **Phase 2 update:** with the draining
  gate (DEC-030) this case cannot occur: any PUT admitted before the gate
  closed is drained into the snapshot, and any later PUT is refused. Race 2 is
  tested in Phase 2 (`TestCloseWritesDrainsInFlightPut`,
  `TestWritesRefusedUntilRegistered`); races 1, 3, 4 involve metadata and are
  tested in Phase 6. `docs/ARCHITECTURE.md` §4 and §7 document the state
  machine and protocol.

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
  memory: worst case ≈ 16 MB per worker.
- **Why.** Lock hold times are bounded by local disk speed, never by the
  network; memory is bounded; behaviour is identical on Linux and Windows.
- **Consequences.** A file grossly larger than a chunk is returned truncated to
  `CHUNK_SIZE + 1` bytes; that is still a size mismatch to the controller, so
  corruption stays detectable.

## DEC-026 — Standalone worker mode (no `CONTROLLER_URL`) keeps the write gate open

- **Context.** DEC-006 requires workers to refuse writes until registered,
  but no controller existed in Phase 1.
- **Choice (Phase 1).** The write gate was implemented (`503 NOT_REGISTERED`
  tested), but `cmd/worker` opened it at startup.
- **Narrowed in Phase 2.** The behaviour is kept only as an explicit
  *standalone mode*: when `CONTROLLER_URL` is empty, the gate opens at startup
  and no agent runs. When `CONTROLLER_URL` is set (always under Compose), the
  gate opens only after registration (DEC-030). `/healthz` reports
  `registered` and `accepting_writes`, and the `worker_started` log line
  reports `mode` (`standalone` or `cluster`).
- **Why keep it.** Standalone workers are useful for isolated tests and the
  Phase 1 container test; there is no risk in production because Compose
  always sets `CONTROLLER_URL`.

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

## DEC-029 — Registration epochs start at controller boot time (ns)

- **Context.** DEC-006 gives each registration an epoch so a superseded
  registration cannot mark a worker `ALIVE`, and heartbeats carry the epoch.
  The registry is in memory, so a restarted controller would restart a simple
  counter at 1, and a heartbeat from before the restart could carry an epoch
  equal to a newly issued one.
- **Alternatives.** Counter from 1 (collision after restart); random 64-bit
  epochs (no ordering, which makes logs and tests harder); persisting the
  counter (extra state to recover).
- **Choice.** The first epoch is the controller's boot time in Unix
  nanoseconds, plus one per registration. Epochs are strictly increasing
  within one controller run and, in practice, across runs.
- **Consequences.** Epochs are large numbers (~1.8 × 10¹⁸), fine for Go
  `uint64` and for JSON consumers that parse 64-bit integers (`jq` and
  JavaScript lose precision above 2⁵³; epochs are compared only by Go code).
  Even a coincidental collision would be harmless: the worker is looked up by
  ID first, and an unknown worker is always told to re-register.

## DEC-030 — Write gate drains in-flight writes before the inventory snapshot

- **Context.** DEC-006 says the inventory snapshot is taken "while writes are
  blocked". Simply refusing new requests is not enough: a PUT admitted just
  before the gate closed can still rename its chunk into place after the
  snapshot, so the controller would believe the chunk is missing (race 1 of
  DEC-006, on the worker side).
- **Alternatives.** A global `RWMutex` held by each write for its whole
  duration (cannot honour a context deadline); snapshot without draining and
  accept spurious "missing" results.
- **Choice.** `writeGate` (in `internal/worker/server`) counts admitted
  requests. A request **increments the counter and checks `open` under one
  mutex**, so a request is either counted before the close or refused.
  `CloseWritesAndDrain(ctx)` closes the gate, then waits until the count is
  zero (or `ctx` ends; the gate stays closed either way). The agent runs
  close+drain → snapshot → register → open on every (re-)registration.
- **Consequences.** A slow PUT delays re-registration by at most its own
  duration (bounded by the server's 2-minute read timeout and the controller's
  per-request timeout). Tested by `TestCloseWritesDrainsInFlightPut`.

## DEC-031 — Heartbeat failures do not close the write gate

- **Context.** If the controller is down or unreachable, should a worker stop
  accepting writes?
- **Choice.** No. Failed heartbeats are logged (first failure, then every
  10th, then on recovery) and retried on the next tick; the gate stays open.
  Only a `reregister` reply restarts the registration cycle.
- **Why.** The only client of the worker API is the controller. If the
  controller can reach the worker but the worker's heartbeats fail (an
  asymmetric partition), the controller's own liveness view already excludes
  the worker after `DEAD_AFTER`; closing the gate would add nothing but
  failures. When heartbeats succeed again, a `DEAD` worker is told to
  re-register, which drains and snapshots as usual.

## DEC-032 — Compose: spec container names with optional prefix; loopback-only port

- **Context.** The spec's failure demo is literally `docker stop worker2`, so
  containers should be named `controller`, `worker1`…`worker4` (D22). Docker
  container names are global per host, and the development machine already
  has another project using exactly those names.
- **Choice.** `container_name: ${OBJSTORE_CONTAINER_PREFIX:-}workerN`: spec
  names by default; setting `OBJSTORE_CONTAINER_PREFIX=objstore-` gives
  `objstore-worker2` etc. Volumes and the network are namespaced by the
  Compose project name `objstore` (`objstore_worker1-data`, …). The controller
  port is published on `127.0.0.1` only (`CONTROLLER_PORT`, default 8080),
  because the API is unauthenticated (§5.10). Workers are not published. The
  image is `${OBJSTORE_IMAGE:-objstore:dev}`. Restart policy `no`.
- **Consequences.** On a machine with conflicting names, the demo commands
  use the prefixed names (`docker stop objstore-worker2`). The e2e test uses a
  unique project name, prefix, and port, so it never touches other
  containers or volumes.

## DEC-033 — Liveness timing owned by the controller

- **Choice.** `HEARTBEAT_INTERVAL` is configured on the controller and sent to
  each worker in the registration reply (`heartbeat_interval_ms`); workers have
  no heartbeat setting, so the two can never disagree. The controller refuses
  to start if `DEAD_AFTER <= HEARTBEAT_INTERVAL`. The failure detector runs
  every `FAILURE_CHECK_INTERVAL` (default 1 s; a new parameter), so a silent
  worker is declared `DEAD` between `DEAD_AFTER` and
  `DEAD_AFTER + FAILURE_CHECK_INTERVAL` after its last heartbeat.
- **Consequences.** Measured in the Compose walkthrough: `DEAD` 10 s after
  `docker stop` with the defaults (3 s / 10 s / 1 s).

## DEC-034 — Metadata schema and transaction rules; commit re-verifies invariants

- **Context.** D7/DEC-003 chose SQLite; the schema and the exact transaction
  boundaries were left to implementation.
- **Choice.** Tables `objects`, `chunks` (`UNIQUE(object_id, idx)`, FK with
  cascade), `replicas` (`PRIMARY KEY(chunk_id, worker_id)`: invariant I3 for
  every state), `pending_deletes` (no FKs, so entries outlive the rows they
  came from; indexed by `object_id` and due time), plus `schema_version`. The
  schema is embedded (`internal/controller/meta/schema.sql`) and applied in a
  transaction at startup; an unknown version refuses to start. Times are Unix
  nanoseconds on the controller's clock.
  Every upload-side write (`ClaimChunk`, `MarkReplicaOK`, `ReplaceReplica`,
  `CommitObject`) first runs `UPDATE objects SET last_activity = ... WHERE
  status = 'UPLOADING'` in the same transaction and fails with
  `ErrNotUploading` if no row matched (DEC-011). `CommitObject` also
  re-checks, from the rows, chunk indexes 0..n-1 without gaps (I1), the size
  sum (I2), and at least RF `OK` replicas with no non-`OK` replica per chunk,
  refusing with `ErrIncomplete` otherwise.
- **Why.** The commit is the single moment an object becomes visible; checking
  the invariants inside that transaction means a coordinator bug cannot
  publish an incomplete object.
- **Consequences.** A commit costs a few aggregate queries over the object's
  chunk rows (negligible). Tests: `internal/controller/meta`.

## DEC-035 — Upload failure handling

- **Context.** Architecture §4.1 and §5.5: retry a failed replica on a
  different worker, never retry a worker that already failed in the request,
  abort through the delete path. DEC-007: a timeout is not proof of failure.
- **Choice.**
  - Each upload keeps a **failed-worker set**. Any PUT error (connection
    refused, timeout, 5xx, checksum rejection 400) adds the worker to it, and
    it is excluded from placement for the rest of that upload.
  - Replacement is one transaction, `ReplaceReplica`: the failed `PENDING`
    row becomes a pending delete, and a new `PENDING` row is recorded for a
    worker chosen among eligible workers that have not failed and do not
    already hold this chunk. The loop ends because every iteration adds a
    worker to the failed set; with none left the upload fails with
    `503 UPLOAD_FAILED`.
  - **Abort** = `MarkDeleting(UPLOADING -> DELETING)`, which moves all the
    object's replica rows (OK and PENDING) into pending deletes in one
    transaction. It runs on `context.WithoutCancel(request)` with a 30 s
    timeout, so it completes after a client disconnect, and then kicks the
    janitor. If even the abort fails, the object stays `UPLOADING` and is
    cleaned up by the Phase 9 stale-upload sweep.
- **Consequences.** An in-flight PUT whose claim was moved by an abort may land
  after the janitor's DELETE; such a file is an orphan for the Phase 9
  inventory sweep (DEC-009). Tests: `TestUploadSurvivesWorkerFailureMidUpload`,
  `TestUploadSurvivesWorkerStopMidUpload`, `TestUploadFailsWhenWorkersRunOut`,
  `TestUploadTimeoutIsNotTreatedAsFailure`,
  `TestUploadRetriesAfterTransitCorruption`.

## DEC-036 — Upload resource bounds

- **Choice.**
  - **Window** (`UPLOAD_WINDOW`, default 4): the reader takes a window slot
    *before* reading each chunk, so at most `Window` chunk buffers exist per
    upload, and a full window stops reading the client (backpressure).
    `Window = 1` is the sequential M3a variant.
  - **Pooled buffers** (`sync.Pool` of `CHUNK_SIZE` slices) avoid allocating
    1 MB per chunk.
  - **Concurrent uploads** are capped (`MAX_CONCURRENT_UPLOADS`, default 16);
    excess requests get `503 SERVER_BUSY` immediately rather than queuing
    unbounded memory. Worst-case upload memory is about 16 x 4 x 1 MB = 64 MB.
  - **Stalled clients:** before each body read the handler sets the
    connection's read deadline to now + `CLIENT_READ_TIMEOUT` (60 s), and it
    clears the deadline when the upload ends, so it never leaks into the next
    request on a keep-alive connection. The controller's HTTP server has no
    overall read/write timeout because uploads and downloads are arbitrarily
    large.
  - Every worker call has `WORKER_CONNECT_TIMEOUT` (2 s) and
    `WORKER_REQUEST_TIMEOUT` (10 s).
- **Measured** (`BenchmarkUploadWindow`: 8 MB object, 4 in-process workers,
  5 ms simulated latency per PUT, fsync off, 3 runs, median): window 1:
  70.4 MB/s; window 2: 112.7; window 4: 122.1; window 8: 112.2. Windowed is
  about 1.7x sequential (M3b acceptance met). Window 8 is not faster than 4
  for an 8-chunk object.

## DEC-037 — Phase 3 janitor scope

- **Choice.** One loop (`JANITOR_INTERVAL`, default 10 s; also triggered by
  `Kick()` after an abort). Each round: (1) fetch due pending deletes **only
  for currently ALIVE workers** (so a backlog for a dead worker cannot starve
  the rest); (2) send idempotent DELETEs with bounded concurrency (8);
  (3) remove confirmed entries, or record the failure with exponential
  backoff (interval x 2^attempts, capped at 5 min); (4) remove `DELETING`
  objects that have no replica rows and no pending deletes left (log event
  `object_purged`). Stale-upload aborts, the inventory orphan sweep, and
  startup recovery are Phase 9.
- **Consequences.** A `DELETING` object whose worker never returns keeps its
  row (invisible to clients) until the worker returns.

## DEC-038 — Placement free-space threshold

- **Context.** Placement rule 3 (architecture §5.3): skip workers over a
  fullness threshold if free space is reported.
- **Choice.** `PLACEMENT_MIN_FREE_BYTES`, default 100,000,000 (100 MB). A
  worker that reported less free space is not chosen; a worker reporting `-1`
  (unknown, e.g. on Windows) is not skipped. Free space comes from heartbeats,
  so it can be one heartbeat interval old; a worker that fills up anyway
  rejects the write (`503 INSUFFICIENT_STORAGE`) and the upload retries
  elsewhere.

## DEC-039 — End of the file part only on a clean `io.EOF`

- **Context.** The first coordinator draft read chunks with `io.ReadFull`,
  which reports `io.ErrUnexpectedEOF` both for a normal short last chunk and
  when the multipart reader reaches the end of a body truncated before its
  closing boundary.
- **Choice.** `readChunk` loops over `Read` and passes errors through
  unchanged. Only `io.EOF` (the multipart reader saw the closing boundary)
  ends the file; any other error aborts the upload.
- **Consequences.** A truncated body is rejected where it is read. (The draft
  would also have failed one step later, when `NextPart` met the missing
  boundary, so this is defence in depth rather than a fix for an observed
  defect.) Test: `TestTruncatedUploadIsNeverCommitted`.

## DEC-040 — Read-path failure classification

- **Context.** §4.2: on mismatch mark the replica `CORRUPT`, on unreachable
  try the other one; §4.9 lists "read miss" as a way to detect a deleted
  chunk file; §3.6 says the controller's checksum is the authority.
- **Choice.**
  | Worker response | Verdict |
  | --- | --- |
  | 200 with wrong size or SHA-256 | `CORRUPT` (reason `size_mismatch` / `checksum_mismatch`) |
  | 404 `CHUNK_NOT_FOUND` | `CORRUPT` (reason `missing`) |
  | 500 `CHUNK_CORRUPT` (unreadable header) | `CORRUPT` (reason `header_corrupt`) |
  | Timeout, connection error, other 5xx, 503 | Skip this replica; state unchanged |
  `MarkReplicaCorrupt` only changes an `OK` row; it runs on a context detached
  from the request so the verdict is recorded even if the client leaves.
  Each detection is logged as `corruption_detected` with `detected_by: read`.
- **Why.** A missing file is as lost as a corrupt one; treating it as
  `CORRUPT` lets Phases 7–8 repair both through one path (D16). The
  controller verifies against its own checksum, so no cross-check is needed
  (the §3.6 cross-check is for *worker*-reported suspicion, Phase 8).
  Unreachability is a liveness question, and only heartbeats decide liveness
  (§4.6).
- **Consequences.** A copy damaged in transit between worker and controller
  would be marked `CORRUPT` although the file is fine; TCP checksums make this
  very unlikely, and the in-place repair (Phase 8) would simply rewrite it.

## DEC-041 — Chunk 0 before the response starts; flush per chunk; abort on later failure

- **Context.** §4.2 sends headers after the pre-flight, so any chunk failure
  after that can only be an aborted connection.
- **Choice.** After the pre-flight, chunk 0 is fetched and verified *before*
  any object header is set and before the status line, so a first chunk with
  no valid copy is a clean JSON `503`. (A first draft set the object headers
  earlier; its 503 then inherited the object's `Content-Length`, which the
  unit tests caught.) After `200`, the response is flushed after the status
  line and after every chunk. If a later chunk has no valid copy, the handler
  panics with `http.ErrAbortHandler`, closing the connection.
- **Why flush.** Without it, the 200 and earlier chunks sit in the server's
  buffer, and an abort delivers nothing at all. The client could not tell a
  failure mid-object from a connection failure. With flushing, the client
  receives `200` and a verified prefix, and sees the truncation against
  `Content-Length`.
- **Consequences.** One flush per 1 MB chunk (negligible).

## DEC-042 — Whole-object checksum verified before the last chunk is sent

- **Context.** §3.6: the object-level checksum catches assembly bugs
  (missing, duplicated, or reordered chunks) that per-chunk checks cannot.
- **Choice.** The download hashes the stream as it writes. After hashing the
  final chunk and *before* writing it, the result is compared with the
  object's stored checksum; on mismatch the connection is aborted, so the last
  chunk is not sent. For an empty object the check happens before the status
  line.
- **Consequences.** The client never receives a complete wrong object.
  Tested with a deliberately wrong stored checksum (`TestAssemblyChecksumGuard`).

## DEC-043 — Sequential download, bounded concurrency, `HEAD`

- **Choice.** Chunks are fetched one at a time (M4 says "start with
  sequential fetch"; parallel prefetch is stretch S2, and `DOWNLOAD_PREFETCH`
  is reserved for it). Each download holds at most one chunk in memory.
  Concurrent downloads are capped at `MAX_CONCURRENT_DOWNLOADS` (default 32,
  ≈ 32 MB); more get `503 SERVER_BUSY`. `HEAD` runs the 404/503 checks and
  returns the headers without fetching any chunk.

## DEC-044 — Client error messages never name workers

- **Context.** The spec says clients do not need to know chunk locations (§5.2);
  D2 puts the controller in the data path for that reason. The first Phase 4
  walkthrough showed a 503 message listing the workers that failed, and an
  upload `UPLOAD_FAILED` message also named a worker.
- **Choice.** Download 503s and upload 503s carry only a summary (for
  example "chunk 0 of object … has no available valid copy"). Details, including
  worker IDs and per-replica reasons, go to the controller log
  (`download_unavailable`, `download_aborted`, `upload_unavailable`,
  `replica_write_failed`). 400 messages keep their detail because they describe
  the client's own request. Tested by `TestErrorsDoNotRevealWorkers` and in
  `TestUploadFailsWhenWorkersRunOut`.
