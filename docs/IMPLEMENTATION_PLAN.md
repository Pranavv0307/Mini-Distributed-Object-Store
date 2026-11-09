# Implementation Plan

Phase-by-phase roadmap. Phases map 1:1 to the architecture milestones M0–M11
([`reference/Object_Store_Architecture.md`](reference/Object_Store_Architecture.md) §10) and
keep their order. Work proceeds **one phase at a time**; each phase ends with
all tests passing, documentation updated, a completion report, and a stop for
owner review. The owner makes the git commit for each phase.

**Status values:** NOT STARTED · IN PROGRESS · COMPLETED.

| Phase | Milestone | Theme | Depends on | Status |
| --- | --- | --- | --- | --- |
| 0 | M0 | Setup and design lock-in | — | COMPLETED (awaiting review) |
| 1 | M1 | Worker: durable chunk store over HTTP, in Docker | 0 | NOT STARTED |
| 2 | M2 | Membership, failure detection, `SYNCING`, Compose cluster | 1 | NOT STARTED |
| 3 | M3a+M3b | Write path: metadata DB, chunk, place, replicate, commit | 2 | NOT STARTED |
| 4 | M4 | Read path: verify, fall back, stream in order | 3 | NOT STARTED |
| 5 | M5 | Complete client API, delete path, error mapping | 4 | NOT STARTED |
| 6 | M6 | Degraded operation, full inventory reconciliation, audit v1 | 5 | NOT STARTED |
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
- **Status.** COMPLETED (awaiting review). Two decisions remain PROVISIONAL
  pending owner confirmation: DEC-005 (chunk size), DEC-013 (string-only
  metadata).

## Phase 1 — Worker as a container (M1)

- **Objectives.** A standalone, durable, atomic, idempotent chunk store
  reachable over HTTP, packaged as a container with a named volume.
- **Dependencies.** Phase 0.
- **Planned components.** `internal/chunkid` (format + strict validation);
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
- **Acceptance criteria.** ~5.5 MiB upload → 6 chunks, 12 replica rows, each
  chunk on 2 distinct workers; files on volumes under expected names; stopping
  a worker mid-upload still succeeds with ≥2 alive; with 1 alive → 503 and no
  object or chunk remains (after cleanup); empty object → 0 chunks; exact
  multiple of chunk size → no empty trailing chunk; metadata part before or
  after file; windowed ≥ sequential throughput in a benchmark.
- **Testing.** Unit tests for chunker edge cases, metadata store transactions,
  placement constraints; integration tests with failure-injecting transports
  (timeouts, checksum rejection, worker death mid-upload); concurrency test of
  many simultaneous uploads; Go benchmark sequential vs windowed.

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
- **Planned components.** Benchmarks (sizes 1 MiB/100 MiB/1 GiB; 1/4/16
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
