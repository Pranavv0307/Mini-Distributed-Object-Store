# Implementation Plan

Phase-by-phase roadmap. Phases map 1:1 to the architecture milestones M0–M11
([`reference/Object_Store_Architecture.md`](reference/Object_Store_Architecture.md) §10) and
keep their order. Work proceeds **one phase at a time**; each phase ends with
all tests passing, documentation updated, a completion report, and a stop for
owner review. The owner makes the git commit for each phase.

**Status values:** NOT STARTED · IN PROGRESS · COMPLETED.

| Phase | Milestone | Theme | Depends on | Status |
| --- | --- | --- | --- | --- |
| 0 | M0 | Setup and design lock-in | — | COMPLETED |
| 1 | M1 | Worker: durable chunk store over HTTP, in Docker | 0 | COMPLETED |
| 2 | M2 | Membership, failure detection, `SYNCING`, Compose cluster | 1 | COMPLETED |
| 3 | M3a+M3b | Write path: metadata DB, chunk, place, replicate, commit | 2 | COMPLETED |
| 4 | M4 | Read path: verify, fall back, stream in order | 3 | COMPLETED |
| 5 | M5 | Complete client API, delete path, error mapping | 4 | COMPLETED |
| 6 | M6 | Degraded operation, full inventory reconciliation, audit v1 | 5 | COMPLETED (awaiting review) |
| 7 | M7 | Self-healing re-replication (reconciler) | 6 | NOT STARTED |
| 8 | M8 | Data integrity: scrubber, cross-check, corrupt repair | 7 | NOT STARTED |
| 9 | M9 | Crash consistency, janitor, recovery, audit v2 | 8 | NOT STARTED |
| 10 | M10 | Evaluation: benchmarks, chaos, invariant tests | 9 | NOT STARTED |
| 11 | M11 | Packaging: README demo, one-command demo | 10 | NOT STARTED |
| S1–S5 | Stretch | Rendezvous hashing, prefetch, Range, metrics, zones | 9 | Optional |

## Testing levels used throughout

- **Unit:** `go test ./...` next to each package.
- **Integration:** an in-process cluster harness (`test/integration`, from Phase 2)
  that runs the controller and N workers on `httptest` servers with temp
  directories. Failures are injected by stopping servers, editing files, or
  wrapping transports. No sleeps: tests poll with deadlines or wait on
  explicit readiness signals, with short timing values from test config.
- **End-to-end:** `test/e2e` behind the `e2e` build tag, driving the real
  `docker compose` stack (from Phase 2).
- **Race:** `go test -race ./...` inside a Linux `golang` container (DEC-018).
- **Static:** `gofmt -l`, `go vet`, `go tool staticcheck`.

---

## Phase 0 — Setup and design lock-in (M0)

- **Objectives.** Repository, toolchain, documentation skeleton, and every
  open design decision recorded, before any functionality is built.
- **Scope.** Git repo (no commits); Go module; directory layout; stub
  binaries; `.gitignore`, `.gitattributes`, `.editorconfig`, `.env.example`;
  docs set; API contract; decision log; copies of the source documents.
- **Out of scope.** Any controller/worker functionality; Dockerfile and Compose
  (Phases 1–2).
- **Deliverables.** `go.mod` (Go 1.27, staticcheck pinned as a tool);
  `cmd/{controller,worker,objctl}` stubs; `internal/version`; `README.md`;
  `docs/{ARCHITECTURE,IMPLEMENTATION_PLAN,DEVELOPMENT_LOG,DECISIONS,RUN_AND_TEST,API}.md`;
  `docs/reference/*`.
- **Acceptance criteria.** `go build ./...`, `go vet ./...`, `gofmt -l .`
  (empty), `go tool staticcheck ./...`, `go test ./...` pass natively, and
  `go test -race ./...` passes in the `golang:1.27.3` container. Every
  §9 open decision has a DEC entry.
- **Testing.** Toolchain smoke test (`internal/version`).
- **Status.** COMPLETED. Two decisions remain PROVISIONAL pending owner
  confirmation: DEC-005 (chunk size), DEC-013 (string-only metadata).

## Phase 1 — Worker as a container (M1)

- **Objectives.** A standalone, durable, atomic, idempotent chunk store
  reachable over HTTP, packaged as a container with a named volume.
- **Dependencies.** Phase 0.
- **Planned components.** `internal/ids` (planned as `internal/chunkid`; format + strict validation);
  `internal/config` (env parsing with defaults and validation);
  `internal/httpx` (JSON errors, middleware); `internal/worker/store`
  (header file format DEC-012, temp → fsync → rename → dir fsync, startup temp
  cleanup, inventory, per-chunk locking); `internal/worker/server` (PUT/GET/
  DELETE/inventory/healthz per `docs/API.md`); `cmd/worker`; multi-stage
  `Dockerfile` (Alpine runtime so `docker exec` corruption demos work);
  `cmd/chunkinspect` or `worker inspect` debug tool. The write gate (DEC-006) is
  built here but **open by default** (configurable) until Phase 2 adds
  registration.
- **Acceptance criteria.** (M1 checkpoint) store/fetch/compare bytes;
  data survives `docker stop/start`; killing the process mid-write leaves no
  visible half-chunk after restart; wrong expected checksum rejected; PUT and
  DELETE idempotent; `../`-style and malformed IDs rejected; inventory excludes
  temp files and reports correct sizes.
- **Testing.** Unit tests for store (crash simulation via leftover temp files,
  concurrent PUT/DELETE on the same ID, concurrent PUTs of the same ID,
  corrupted header/payload detection) and HTTP handler tests; manual Docker
  verification script documented in `RUN_AND_TEST.md`; race run.
- **Status.** COMPLETED. As built: the identifier package is
  `internal/ids` (DEC-028); the debug tool is the `worker inspect`
  subcommand; the write gate exists but is open at startup (DEC-026). Every
  acceptance criterion is covered by an automated test: store/fetch/compare
  (store, server, e2e), persistence across stop/start and container
  re-creation (e2e), kill mid-write (integration with the native binary, and
  e2e with `docker kill`), wrong checksum (all levels), idempotent PUT/DELETE,
  traversal IDs, inventory contents.

## Phase 2 — Membership and failure detection (M2)

- **Objectives.** The controller always knows which workers are alive, and
  shows it.
- **Dependencies.** Phase 1.
- **Planned components.** `internal/controller/registry` (states `SYNCING`,
  `ALIVE`, `DEAD`; epochs; controller-clock liveness); failure detector loop;
  register and heartbeat handlers (`reregister` action, DEC-020); worker
  membership agent (register with retry/backoff, then heartbeats; write gate
  closed until registered); `/healthz`, `/cluster` (workers only);
  `docker-compose.yml` (controller + 4 explicit workers, named volumes, one
  published port, healthchecks); integration harness `test/integration`.
  Inventory reconciliation is a trivial step at this phase (no metadata yet)
  but the `SYNCING → ALIVE` epoch protocol is real.
- **Acceptance criteria.** `/cluster` shows 4 ALIVE; `docker stop worker2` →
  DEAD within ~`DEAD_AFTER`; `docker start worker2` → ALIVE; `docker restart
  controller` → all workers re-register untouched; a superseded registration
  never flips a worker to ALIVE (epoch test); a worker refuses writes until
  registered.
- **Testing.** Registry unit tests with a fake clock; integration tests for
  death, return, controller restart, concurrent re-registration; e2e test
  with Compose (`-tags e2e`).
- **Status.** COMPLETED. As built:
  `internal/controller/registry` (state machine, epochs from boot time
  DEC-029, failure detector), `internal/controller/api` (register, heartbeat,
  `/healthz`, `/cluster`; `InventoryReconciler` hook with a no-op
  implementation until Phase 6), `internal/worker/agent` (membership loop),
  draining write gate in `internal/worker/server` (DEC-030),
  `cmd/controller`, `docker-compose.yml` (DEC-032). Every acceptance
  criterion has an automated test: 4 ALIVE, stop → DEAD, start → ALIVE,
  controller restart → re-registration (in-process integration and Compose
  e2e); superseded registration and death during sync (registry and API unit
  tests); writes refused until registered (server, agent, integration).
  Also in this phase: the chunk size changed to 1,000,000 bytes (DEC-005).

## Phase 3 — Write path (M3a sequential, then M3b windowed)

- **Objectives.** Upload any object: chunked, replicated twice on distinct
  ALIVE workers, committed atomically.
- **Dependencies.** Phase 2.
- **Planned components.** `internal/controller/meta` (SQLite via
  `modernc.org/sqlite`, embedded migrations; tables `objects`, `chunks`,
  `replicas` with `UNIQUE(chunk_id, worker_id)`, `pending_deletes`);
  placement (random distinct ALIVE, excluding workers with a pending delete
  for the chunk); worker client (timeouts, bounded retries, checksum header);
  upload coordinator (streaming multipart, per-chunk + whole-object SHA-256,
  claim-before-write, retry on a different worker with the failed claim moved
  to `pending_deletes` in the same transaction, conditional claims and commit
  DEC-011); minimal abort via delete path; then M3b bounded window with
  backpressure.
- **Acceptance criteria.** 5.5 MB (5,500,000-byte) upload → 6 chunks, 12 replica rows, each
  chunk on 2 distinct workers; files on volumes under expected names; stopping
  a worker mid-upload still succeeds with ≥2 alive; with 1 alive → 503 and no
  object or chunk remains (after cleanup); empty object → 0 chunks; exact
  multiple of chunk size → no empty trailing chunk; metadata part before or
  after file; windowed ≥ sequential throughput in a benchmark.
- **Testing.** Unit tests for chunker edge cases, metadata store transactions,
  placement constraints; integration tests with failure-injecting transports
  (timeouts, checksum rejection, worker death mid-upload); concurrency test of
  many simultaneous uploads; Go benchmark sequential vs windowed.
- **Status.** COMPLETED. As built:
  `internal/controller/meta` (SQLite schema and conditional transactions,
  DEC-034), `internal/controller/placement` (random distinct eligible
  workers, free-space rule DEC-038), `internal/controller/workerclient`,
  `internal/controller/custommeta` (DEC-013 validation),
  `internal/controller/upload` (window, claim-before-write, retry with
  failed-worker set, abort, DEC-035/036/039), a minimal
  `internal/controller/janitor` (pending deletes on ALIVE workers, purge of
  finished `DELETING` objects, DEC-037; planned for Phase 5 but needed here
  for the "no object or chunk remains" criterion), `POST /api/v1/objects`.
  All acceptance criteria are covered by automated tests: 5.5 MB → 6 chunks,
  12 rows, files under chunk-ID names (`TestUploadDistributesAndReplicates`,
  `TestComposeUpload`); worker stopped mid-upload
  (`TestUploadSurvivesWorkerStopMidUpload`); one worker alive → 503 and
  nothing stored (`TestUploadInsufficientWorkers`, `TestComposeUpload`);
  upload failing mid-way leaves nothing (`TestUploadFailsWhenWorkersRunOut`);
  edge sizes; metadata before/after the file; windowed ≥ sequential
  (`BenchmarkUploadWindow`: ≈ 1.7×). Known gap until Phase 9: a controller
  crash mid-upload leaves an `UPLOADING` object (invisible to clients) and
  its chunk files.

## Phase 4 — Read path (M4)

- **Objectives.** Downloads return exactly the original bytes, in order, even
  with a worker down; corrupt data is never returned.
- **Dependencies.** Phase 3.
- **Planned components.** Download coordinator: READY lookup, pre-flight
  effective-replica check (503), headers, sequential in-order streaming,
  per-chunk size + SHA-256 verification before sending, random replica choice
  with fallback, mark `CORRUPT` on mismatch, abort the connection on a
  mid-stream failure.
- **Acceptance criteria.** Upload/download checksums identical (I6); download
  works with a holder stopped; a hand-altered replica is skipped and marked
  `CORRUPT`; zero effective replicas → 503 before any body; mid-stream failure
  → truncated response, never wrong bytes.
- **Testing.** Integration tests with stopped workers, corrupted files,
  missing files, both replicas bad; concurrency test of parallel downloads.
- **Status.** COMPLETED. As built:
  `internal/controller/download` (pre-flight, chunk 0 before headers, per-chunk
  verification with random replica order and fallback, `CORRUPT` marking,
  whole-object checksum guard, flush per chunk, abort mid-stream, `HEAD`),
  `meta.MarkReplicaCorrupt`, `GET /api/v1/objects/{id}` with
  `MAX_CONCURRENT_DOWNLOADS` (DEC-040 to DEC-044). Acceptance: checksums
  identical (`TestDownloadRoundTrip`, `TestComposeDownload`); holder stopped
  (`TestDownloadWithWorkerStopped`, e2e); hand-altered replica skipped and
  marked `CORRUPT` (`TestDownloadSkipsAndMarksCorruptReplica`, 5 kinds of
  damage, e2e); zero effective replicas → 503 before any body
  (`TestDownloadBothHoldersDown`, `TestDownloadBothReplicasCorrupt`);
  mid-stream failure → truncated, never wrong (`TestMidStreamFailureTruncates`,
  `TestDownloadBothReplicasCorrupt/last_chunk`). Prefetch (S2) not done.

## Phase 5 — Complete API surface (M5)

- **Objectives.** Every spec endpoint works with correct statuses and JSON
  errors.
- **Dependencies.** Phase 4.
- **Planned components.** Metadata GET (with chunk map) and PATCH (merge,
  `null` removes, `If-Match` → 409); list with keyset cursor pagination;
  delete (logical `DELETING`, 204, replicas → `pending_deletes`, minimal
  janitor that drains `pending_deletes` and removes object rows when done);
  validation and limits; `objctl` client commands; conformance checklist test.
- **Acceptance criteria.** Every row of the spec's endpoint and status tables
  produces the expected result; PATCH leaves replica rows unchanged; delete →
  instantly 404, files gone shortly after; delete with a worker down → 204 and
  completes when it returns.
- **Testing.** Conformance test table; pagination stability under concurrent
  inserts; PATCH/DELETE race test.
- **Status.** COMPLETED. As built: `meta.UpdateMetadata`,
  `meta.ListObjects`; `custommeta.ParsePatch`/`Merge`; handlers in
  `internal/controller/api/objects.go`; client delete reuses
  `MarkDeleting` + janitor (built in Phase 3); `cmd/objctl` and the Compose
  `client` service (DEC-045 to DEC-050). Acceptance: `TestSpecConformance`
  (19 rows, all pass); PATCH leaves the chunk map unchanged
  (`TestMetadataPatch`); delete → instant 404 everywhere, files gone shortly
  after (`TestDelete`, `TestComposeObjectAPI`); delete with a worker down →
  204, completes on return (`TestDeleteWithWorkerDown`, e2e); pagination
  stable under concurrent inserts (`TestListStableUnderConcurrentUploads`);
  PATCH/DELETE and concurrent PATCH races (`TestConcurrentPatchesAreNotLost`,
  `TestDelete`).

## Phase 6 — Degraded operation and worker resync (M6)

- **Objectives.** Correct behaviour during a failure and on worker return,
  before repair exists.
- **Dependencies.** Phase 5.
- **Planned components.** Full §4.5 reconciliation table on register (missing →
  drop row; size mismatch → `CORRUPT`; orphan → `pending_deletes`;
  over-replication → keep); `/cluster` chunk totals; audit v1 (I1, I2, I3, I5,
  I7) at `/admin/audit`.
- **Acceptance criteria.** Uploads/downloads keep working with a worker down;
  under-replicated counts grow then return to normal on restart inside grace
  with no data movement; wiped volume → missing replicas dropped; races 1–3 of
  DEC-006 covered by tests; audit passes after each scenario.
- **Status.** COMPLETED (awaiting review). As built:
  `meta.ReconcileInventory` + `internal/controller/resync` (DEC-051),
  `meta.ChunkHealth` and the `/cluster` storage section (DEC-052),
  `meta.Audit` and `GET /admin/audit` (DEC-053: "passes" means the safety
  invariants hold; I5 is reported as `steady_state`). Acceptance:
  degraded uploads/downloads and growing under-replicated counts, return
  within grace with no data movement (`TestDegradedOperationAndReturnWithinGrace`,
  `TestComposeResync`); wiped volume (`TestWipedWorkerReturns`, e2e); size
  damage (`TestSizeMismatchOnReturn`); orphan (`TestOrphanOnReturn`); DEC-006
  races 1 and 3 with metadata (`TestSyncingWorkerNotUsedDuringReconciliation`,
  `TestWipedWorkerPreflightIs503`); audit `ok` after every scenario.

## Phase 7 — Self-healing re-replication (M7)

- **Objectives.** After a worker is gone beyond `REPAIR_GRACE`, full RF is
  restored automatically.
- **Dependencies.** Phase 6.
- **Planned components.** Level-triggered reconciler: find chunks with fewer
  than RF effective replicas, prioritise 1-copy chunks, bounded concurrency,
  in-flight set, controller-mediated verified copy, target rule DEC-008,
  conditional marking DEC-011; over-replication trimming (low priority, via
  `pending_deletes`); zero-effective alarm; events and counters.
- **Acceptance criteria.** The main demo (M7 checkpoint 1–5): stop worker2,
  under-replicated → 0 on remaining workers after grace, audit passes, restart
  → resync and trimming, audit passes; detection time and time-to-full-RF
  recorded.

## Phase 8 — Data integrity (M8)

- **Objectives.** Corruption is detected on every path, never returned, and
  repaired automatically.
- **Dependencies.** Phase 7.
- **Planned components.** Throttled worker scrubber reporting suspects via
  heartbeat; controller cross-check against metadata before marking `CORRUPT`;
  `CORRUPT` repaired through the Phase 7 path with no grace, preferring
  in-place rewrite; both-replicas-corrupt → 503 with a data-loss event.
- **Acceptance criteria.** M8 checkpoint: appended bytes → correct download,
  replica marked `CORRUPT` then repaired, file valid again; deleted chunk file
  → detected and repaired; both corrupt → refused, logged; scrubber detects
  without any client read.

## Phase 9 — Crash consistency and janitor (M9)

- **Objectives.** The system is clean after a crash at any point.
- **Dependencies.** Phase 8.
- **Planned components.** Full janitor (stale `UPLOADING` → `DELETING`, finish
  `DELETING`, drain `pending_deletes`, periodic inventory orphan sweep
  DEC-009); controller startup recovery pass (UPLOADING → DELETING; PENDING
  rows → `pending_deletes`); reconciler warm-up; audit v2 (I4 via inventories).
- **Acceptance criteria.** M9 checkpoint: kill controller mid-upload → no
  visible object and no leftover files after janitor; kill worker mid-write →
  no half-chunk; client Ctrl-C → cleaned up; delete during worker downtime →
  stale chunks removed on return; audit v2 passes.

## Phase 10 — Evaluation (M10)

- **Objectives.** Turn behaviour into numbers; systematic correctness tests.
- **Planned components.** Benchmarks (sizes 1 MB/100 MB/1 GB; 1/4/16
  clients; sequential vs windowed; fsync on/off; degraded reads; detection and
  repair times; scrubber detection time) with medians and p99; chaos script
  with audit; invariant tests for every §7 scenario.
- **Acceptance criteria.** Results table in docs, reproducible from scripts.

## Phase 11 — Packaging (M11)

- **Objectives.** A stranger can build, run, and see the full demo from the
  README alone.
- **Planned components.** Final README; one-command demo covering every spec
  demonstration; trimmed architecture doc with diagram, limitations,
  trade-offs.
- **Acceptance criteria.** From a fresh clone plus `docker compose down -v`,
  the README alone gets through the full demo.
