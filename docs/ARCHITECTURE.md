# Architecture

This document describes the system **as implemented** and the target design
for parts not yet built. Every section carries an implementation status. The
original design, with full rationale for decisions D1–D22, is in
[`reference/Object_Store_Architecture.md`](reference/Object_Store_Architecture.md)
(called "the design doc" below). Where this implementation differs from or
clarifies the design doc, the difference is listed in
[§9 Deviations](#9-deviations-and-clarifications-vs-the-design-doc) with a link
to [`DECISIONS.md`](DECISIONS.md).

> **Current status (end of Phase 1):** the **worker's chunk store and chunk
> API** are implemented and run in Docker (§2, §3.1). The worker runs
> standalone; membership with the controller arrives in Phase 2. Everything
> controller-side below is still the target design.

---

## 1. Overview

A single **controller** and four **workers**, run with Docker Compose. Clients
talk only to the controller's REST API. The controller cuts each object into
fixed-size chunks (1 MiB, provisional, DEC-005), stores every chunk on **two
distinct workers** (replication factor RF = 2), records all metadata in an
embedded SQLite database, detects dead workers through heartbeats, and repairs
lost or corrupt replicas in the background.

```
 client ──REST──▶ controller ──chunk PUT/GET/DELETE──▶ worker1..worker4
                    │  ▲                                   │
                    │  └────── register / heartbeat ───────┘
                    └── SQLite metadata (persistent volume)
 Workers never talk to each other. Only the controller decides.
```

Guiding principles (design doc §1.5): one brain, many hands; metadata is the
truth; immutable data; intent first, action second, commit last; desired vs.
observed state reconciled in loops; trust nothing you didn't verify.

## 2. Components

| Process | Module | Responsibility | Package (planned) | Status |
| --- | --- | --- | --- | --- |
| Worker | Chunk store | Header-format chunk files, atomic durable writes, temp cleanup, inventory | `internal/worker/store` | **Implemented (Phase 1)** |
| Worker | Chunk API | PUT/GET/DELETE chunk, list inventory, healthz, write gate | `internal/worker/server`, `cmd/worker` | **Implemented (Phase 1)** |
| Worker | Membership agent | Register (with inventory, retried), heartbeat, write gate | `internal/worker/agent` | Phase 2 |
| Worker | Scrubber | Throttled re-verification; suspects reported via heartbeat | `internal/worker/scrub` | Phase 8 |
| Controller | API front end | Client REST API, validation, error mapping | `internal/controller/api` | Phases 3–5 |
| Controller | Metadata store | SQLite schema, migrations, short transactions | `internal/controller/meta` | Phase 3 |
| Controller | Worker registry | In-memory worker states, epochs, controller-clock liveness | `internal/controller/registry` | Phase 2 |
| Controller | Failure detector | `ALIVE`/`SYNCING` → `DEAD` on heartbeat silence | `internal/controller/registry` | Phase 2 |
| Controller | Placement engine | Distinct ALIVE workers; pluggable (random first) | `internal/controller/placement` | Phase 3 |
| Controller | Worker client | The only code calling workers: timeouts, retries, checksum headers | `internal/controller/workerclient` | Phase 3 |
| Controller | Upload / download coordinators | Streaming write and read paths | `internal/controller/upload`, `.../download` | Phases 3–4 |
| Controller | Reconciler | Level-triggered repair of under-replicated/corrupt chunks | `internal/controller/reconciler` | Phase 7 |
| Controller | Janitor | Stale uploads, unfinished deletes, `pending_deletes`, orphan sweep | `internal/controller/janitor` | Phases 5, 9 |
| Controller | Recovery | Startup recovery pass, reconciler warm-up | `internal/controller/recovery` | Phase 9 |
| Controller | Observability | `/healthz`, `/cluster`, audit, structured event log | `internal/controller/api`, `.../audit` | Phases 2, 6, 9 |
| Client | `objctl` | CLI for the REST API; demos and benchmarks | `cmd/objctl` | Phase 5 |

Shared packages (implemented in Phase 1): `internal/ids` (object/chunk/worker
ID generation and validation, DEC-028), `internal/config` (environment
configuration), `internal/wire` (JSON types, header names, error codes),
`internal/httpx` (JSON errors, request logging, panic recovery),
`internal/version`.

All binaries ship in one Alpine image running as a non-root user (DEC-023);
Compose selects the command.

## 3. Data model

Logical records (stored in SQLite tables in Phase 3):

- **Object:** `object_id` (UUIDv4), `name` (not unique), `size`, `content_type`,
  `created_at`, `modified_at`, `checksum_sha256` (whole object), `num_chunks`,
  `status` (`UPLOADING` | `READY` | `DELETING`), `last_activity`,
  `metadata_version` (DEC-014), `custom_metadata` (string → string, DEC-013).
- **Chunk:** `chunk_id` = `{object_id}_{idx:06d}`, `object_id`, `idx`, `size`,
  `checksum_sha256`.
- **Replica:** `chunk_id`, `worker_id`, `state` (`PENDING` | `OK` | `CORRUPT`);
  `UNIQUE(chunk_id, worker_id)` (DEC-008).
- **Pending delete:** `chunk_id`, `worker_id`, `reason`, `created_at`,
  `attempts`; `UNIQUE(chunk_id, worker_id)` (DEC-007).
- **Worker (in memory, rebuilt from heartbeats):** `worker_id`, `address`,
  `state` (`SYNCING` | `ALIVE` | `DEAD`), `epoch`, `last_heartbeat`
  (controller clock), `dead_since`, free/used bytes, chunk count.

**Effective replica** = replica in state `OK` on a worker in state `ALIVE`.
Reads use only effective replicas; safety is measured by counting them.

### 3.1 Worker storage (implemented, Phase 1)

```
DATA_DIR/                       (named Docker volume, owned by uid 10001)
  chunks/<chunk_id>             committed: 48-byte header + payload
  tmp/<chunk_id>.<random>.tmp   in-progress writes; all deleted at startup
```

Header (DEC-012): magic `OSCK`, format version, payload size, payload
SHA-256. The header records the size and checksum the controller *expected*,
and is only committed if the received bytes matched both.

**Write (PUT) protocol.** Validate ID and headers → stream the body into a new
temp file while hashing → reject on size or checksum mismatch (temp file
deleted, existing file untouched) → fsync file → under the chunk's lock,
rename over `chunks/<chunk_id>` → fsync directory → ack. A crash before the
rename leaves only a temp file (removed at next startup); a crash after it
leaves the complete new file. A PUT always replaces (DEC-024), so retries are
idempotent and in-place repair needs no special path.

**Read (GET).** Under the chunk's read lock, load the file (capped at
`CHUNK_SIZE + 49` bytes) into memory, release the lock, send. The worker
returns bytes as stored plus the stored checksum and **does not judge
validity**; the controller verifies against metadata. A destroyed header is
reported as `CHUNK_CORRUPT` (DEC-027). At most `MAX_CONCURRENT_READS`
chunks are held in memory (DEC-025).

**Delete.** Under the chunk's lock, unlink; absent is success; fsync directory.

**Inventory.** List `chunks/` (never `tmp/`), keep only valid chunk-ID names,
report `size = file size − 48` from metadata alone. Appended or truncated
bytes therefore show up as a size mismatch at reconciliation time (Phase 6).

**Locking.** 256 striped `RWMutex`es keyed by a hash of the chunk ID. Locks are
held only for local file operations, never across network I/O.

**Write gate.** PUT/DELETE return 503 `NOT_REGISTERED` while closed. Phase 1
opens it at startup (standalone, DEC-026); Phase 2 opens it only after
registration (DEC-006).

## 4. State machines

**Object** (design doc §3.3, unchanged):
`UPLOADING → READY` (atomic commit) → `DELETING` → removed;
`UPLOADING → DELETING` on failure, staleness, or controller restart (abort =
delete). Only `READY` is visible to clients.

**Replica:** `PENDING → OK` (worker acked) → `CORRUPT` (verified mismatch);
`CORRUPT → PENDING` when an in-place repair starts (DEC-008). A failed or
abandoned `PENDING` claim, and a replica removed from an object being deleted,
move to `pending_deletes` (DEC-007). A replica found missing during
reconciliation is removed.

**Worker** (corrected; DEC-006):

```
             register                  reconciliation done
 (unknown) ───────────▶ SYNCING ─────(epoch still current)─────▶ ALIVE
                          ▲  │                                     │
                 register │  │ no heartbeat                        │ no heartbeat
                          │  ▼ for DEAD_AFTER                      │ for DEAD_AFTER
                          DEAD ◀───────────────────────────────────┘
```

That is: `SYNCING → ALIVE` after reconciliation; `SYNCING|ALIVE → DEAD` after
`DEAD_AFTER` without heartbeats; `DEAD → SYNCING` on re-registration. A
heartbeat from an unknown or `DEAD` worker is answered with `reregister`.
"Repair-eligible" is derived: `DEAD` for longer than `REPAIR_GRACE`.

## 5. Data flows (target design; statuses per phase)

- **Upload (Phase 3):** admission (≥ RF ALIVE workers, else 503) → object row
  `UPLOADING` → for each chunk: read ≤ chunk size from the multipart stream,
  hash, place on RF distinct workers, **claim** (chunk + `PENDING` replica rows,
  conditional on `UPLOADING`), PUT in parallel, mark `OK` → commit in one
  transaction (`READY`, conditional on `UPLOADING`). A failed or timed-out PUT
  moves that claim to `pending_deletes` and retries on a different worker in
  the same transaction. Bounded window of chunks in flight (M3b). Any fatal
  failure → delete path.
- **Download (Phase 4):** `READY` check → load chunk map → pre-flight (every
  chunk has ≥ 1 effective replica, else 503) → headers → for each chunk in
  index order: fetch from a random effective replica, verify size + SHA-256,
  on mismatch mark `CORRUPT` and try the next → write. Mid-stream failure
  aborts the connection.
- **Metadata (Phase 5):** single-transaction reads and merges; no worker calls.
- **Delete (Phase 5):** transaction `READY → DELETING` and replicas →
  `pending_deletes` → 204 → janitor issues idempotent DELETEs until confirmed
  → object, chunk rows removed.
- **Register / resync (Phases 2, 6):** worker cleans temp files, snapshots
  its inventory while writes are gated, registers; controller assigns an epoch,
  marks `SYNCING`, reconciles (§4.5 table) in short DB transactions, marks
  `ALIVE` if the epoch is still current.
- **Repair (Phase 7–8):** reconciler round finds chunks of `READY` objects with
  fewer than RF effective replicas (counting replicas on repair-eligible
  `DEAD` workers and `CORRUPT` replicas as lost), sorts by risk, and with
  bounded concurrency claims a target, copies from a verified source through
  the controller, marks `OK`, and drops the superseded row.

## 6. Invariants

I1–I7 from the design doc §3.4 are the correctness contract and will be checked
by the audit (Phases 6, 9):

| # | Invariant |
| --- | --- |
| I1 | A READY object has chunk rows for every idx 0..num_chunks−1, no gaps or duplicates |
| I2 | Sum of chunk sizes = object size |
| I3 | No two replica rows of one chunk on the same worker (enforced by `UNIQUE`) |
| I4 | After the janitor has run, no chunk file on any worker is unclaimed |
| I5 | Steady state: every chunk of every READY object has exactly RF effective replicas |
| I6 | SHA-256 of downloaded bytes = stored object checksum |
| I7 | A non-READY object is never returned or listed |

Additional implementation invariants:

| # | Invariant | Source |
| --- | --- | --- |
| I8 | No `(chunk, worker)` pair has both a live claim (`PENDING`/`OK`) and a `pending_deletes` entry | DEC-007 |
| I9 | A worker in state `SYNCING` is never chosen for placement, reads, or repair | DEC-006 |
| I10 | No DB transaction is open across a network call | Design doc §3.5 |

## 7. Failure handling (summary)

See the design doc §5.2 (failure model) and §7 (failure scenario matrix). The
scenarios are implemented and demonstrated in Phases 6–9; each one will be
listed here with its test once implemented.

## 8. Trade-offs and limitations

Inherited from the design doc §8: single controller (SPOF); controller in the
data path (throughput ceiling); synchronous 2-way replication (write latency;
needs ≥ 2 workers); RF = 2 (one failure at a time); immutable objects; repair
grace period (longer vulnerability window); single-file metadata DB.

## 9. Deviations and clarifications vs. the design doc

| Topic | Design doc | Implementation | Decision |
| --- | --- | --- | --- |
| Worker `SYNCING` state | §4.5 uses it; §3.3 diagram omits it; §2.2 says it derives from heartbeat age | Registration-driven state with epochs; excluded from placement, reads, and repair; workers gate writes until registered | DEC-006 |
| Physical cleanup | Failed claims removed; orphans "scheduled for deletion" | Durable `pending_deletes` table, mutually exclusive with claims | DEC-007 |
| Repair target | "No replica of this chunk" but "prefer the CORRUPT holder" | No `OK`/`PENDING` replica and no pending delete; `CORRUPT → PENDING` in place | DEC-008 |
| Orphan sweep | Inventories only at registration | Janitor also pulls inventories periodically | DEC-009 |
| `GET /internal/workers/{id}/chunks` | Host unspecified | Served by the worker | DEC-010 |
| Stale-upload abort vs. live handler | Not addressed | Claims and commit conditional on status | DEC-011 |
| New parameters | — | `JANITOR_INTERVAL`, `INVENTORY_SWEEP_INTERVAL` | DEC-009 |
