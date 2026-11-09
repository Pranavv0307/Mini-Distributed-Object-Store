# Architecture

This document describes the system **as implemented** and the target design
for parts not yet built. Every section carries an implementation status. The
original design, with full rationale for decisions D1–D22, is in
[`reference/Object_Store_Architecture.md`](reference/Object_Store_Architecture.md)
(called "the design doc" below). Where this implementation differs from or
clarifies the design doc, the difference is listed in
[§14 Deviations](#14-deviations-and-clarifications-vs-the-design-doc) with a link
to [`DECISIONS.md`](DECISIONS.md).

> **Current status (end of Phase 6):** implemented: the worker's chunk store
> and chunk API (§3.1); cluster membership with the `SYNCING` protocol (§7);
> the metadata database, the upload path, and a minimal janitor (§8); the
> verified download path (§9); metadata, list, delete, and `objctl` (§10);
> inventory reconciliation, `/cluster` storage totals, and audit v1 (§11).
> **Every client endpoint of the specification works.** Not yet: repair,
> the scrubber, and full janitor/recovery (target design below).

---

## 1. Overview

A single **controller** and four **workers**, run with Docker Compose. Clients
talk only to the controller's REST API. The controller cuts each object into
fixed-size chunks (1 MB = 1,000,000 bytes, DEC-005), stores every chunk on **two
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
| Worker | Membership agent | Register (with inventory, retried), heartbeat, write gate | `internal/worker/agent` | **Implemented (Phase 2)** |
| Worker | Scrubber | Throttled re-verification; suspects reported via heartbeat | `internal/worker/scrub` | Phase 8 |
| Controller | API front end | Membership endpoints (Phase 2, implemented); client REST API, validation, error mapping (Phases 3–5) | `internal/controller/api`, `cmd/controller` | Partly (Phase 2) |
| Controller | Metadata store | SQLite schema, migrations, short conditional transactions | `internal/controller/meta` | **Implemented (Phase 3)** |
| Controller | Worker registry | In-memory worker states, epochs, controller-clock liveness | `internal/controller/registry` | **Implemented (Phase 2)** |
| Controller | Failure detector | `ALIVE`/`SYNCING` → `DEAD` on heartbeat silence | `internal/controller/registry` | **Implemented (Phase 2)** |
| Controller | Placement engine | Distinct ALIVE workers with free space; pluggable (random) | `internal/controller/placement` | **Implemented (Phase 3)** |
| Controller | Worker client | The only code calling workers: timeouts, checksum headers | `internal/controller/workerclient` | **Implemented (Phase 3)** |
| Controller | Upload / download coordinators | Streaming write and read paths | `internal/controller/upload`, `.../download` | **Implemented (Phases 3, 4)** |
| Controller | Reconciler | Level-triggered repair of under-replicated/corrupt chunks | `internal/controller/reconciler` | Phase 7 |
| Controller | Janitor | `pending_deletes` drain and purge of `DELETING` objects for aborted uploads and client deletes (Phases 3, 5, implemented); stale uploads, orphan sweep (Phase 9) | `internal/controller/janitor` | Partly |
| Controller | Recovery | Startup recovery pass, reconciler warm-up | `internal/controller/recovery` | Phase 9 |
| Controller | Observability | `/healthz`, `/cluster`, audit, structured event log | `internal/controller/api`, `.../audit` | `/healthz`, `/cluster` (workers), event log: implemented; audit Phases 6, 9 |
| Client | `objctl` | CLI for the REST API with local download verification; Compose `client` service | `cmd/objctl` | **Implemented (Phase 5)** |

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
- **Register / resync (Phases 2, 6, implemented):** worker cleans temp files, snapshots
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

| I11 | A worker accepts chunk PUT/DELETE only while it holds a successful registration (or runs standalone) | DEC-006, DEC-030 |
| I12 | A registration whose epoch is no longer current never marks a worker `ALIVE` | DEC-006, DEC-029 |

## 7. Membership and failure detection (implemented, Phase 2)

### 7.1 Controller side

- **Registry** (`internal/controller/registry`): in memory, one mutex, no I/O
  under the lock. Per worker: address, state, epoch, `RegisteredAt`,
  `LastHeartbeat` (controller receive time), `DeadSince`, reported capacity.
  Rebuilt from registrations after a controller restart (by design, §3.5 of
  the design doc).
- **Registration** (`POST /internal/workers/register`):
  `BeginRegistration` (→ `SYNCING`, new epoch) → `InventoryReconciler`
  (no-op until Phase 6) → `CompleteRegistration` (→ `ALIVE` only if the
  epoch is current and the state is still `SYNCING`) → reply. A superseded
  registration gets 409; a failed reconciliation gets 503 and leaves the
  worker `SYNCING` (unusable) until a retry succeeds.
- **Heartbeat**: refreshes `LastHeartbeat` and capacity if the worker is
  known, not `DEAD`, and the epoch matches; otherwise the reply is
  `reregister`. A heartbeat never revives a `DEAD` worker.
- **Failure detector**: every `FAILURE_CHECK_INTERVAL`, any `SYNCING` or
  `ALIVE` worker silent for more than `DEAD_AFTER` becomes `DEAD`
  (`DeadSince` = now). Idempotent; safe to run concurrently with requests.
- **Epochs** start at boot time in nanoseconds (DEC-029).

### 7.2 Worker side

`internal/worker/agent` with `CONTROLLER_URL` set:

1. `SetRegistered(false)`; **close and drain the write gate** (wait until no
   admitted PUT/DELETE is in flight, DEC-030).
2. Snapshot the inventory; `POST register`; on failure retry with exponential
   backoff and jitter (200 ms → 5 s), retaking the snapshot each attempt.
3. On success: `SetRegistered(true)`, open the gate.
4. Heartbeat every `heartbeat_interval_ms` (from the registration reply).
   Failures are logged and retried; the gate stays open (DEC-031). A
   `reregister` reply → back to step 1.

The HTTP server starts before registration (so the controller can reach the
worker as soon as it is `ALIVE`); reads, inventory, and health are always
served.

### 7.3 Why the `SYNCING` protocol is race-free

- **No write lands after the snapshot and before `ALIVE`.** The gate is
  closed and drained before the snapshot and opens only after the controller
  has replied (which happens after `ALIVE`). From the controller's side,
  `SYNCING` workers are excluded from placement and reads (`Registry.Alive()`
  returns only `ALIVE`), so no write is even attempted.
- **Superseded registrations cannot win.** `CompleteRegistration` checks the
  epoch under the registry mutex. A delayed, older registration that arrives
  *after* a newer one completed starts its own new epoch; the worker's
  heartbeats then carry the older epoch from its latest reply, are answered
  `reregister`, and the worker re-registers. The system converges, at the cost
  of one extra registration.
- **Death during sync.** If reconciliation outlasts `DEAD_AFTER` without
  heartbeats, the detector marks the worker `DEAD`, and `CompleteRegistration`
  refuses (the state is no longer `SYNCING`).
- **No late writes at all.** Every PUT/DELETE either was admitted before the
  gate closed (and is drained, so its effect is in the snapshot) or arrives
  after (and is refused with 503). This also removes the "repair PUT landing
  after the snapshot" residual case that DEC-006 anticipated: such a PUT is
  either drained into the snapshot or refused. (Whether the controller then
  records it as `OK` or as a pending delete is the data path's concern,
  Phases 3 and 6.)

### 7.4 Deployment (Compose)

`docker-compose.yml`: project `objstore`; services `controller` and
`worker1`…`worker4` from one image; container names
`${OBJSTORE_CONTAINER_PREFIX}<service>` (DEC-032); one named volume per
service mounted at `/data`; only the controller's port 8080 is published,
bound to 127.0.0.1; workers reach the controller at `http://controller:8080`
and advertise `http://workerN:9000`; restart policy `no`; healthchecks with
busybox `wget` on `/healthz`.

### 7.5 Measured behaviour (default timings 3 s / 10 s / 1 s)

| Event | Observed |
| --- | --- |
| `docker compose up` → 4 `ALIVE` | ≈ 1 s |
| `docker stop worker2` → `DEAD` | 10 s |
| `docker start worker2` → `ALIVE` (new epoch) | ≈ 1 s |
| `docker restart controller` → 4 `ALIVE` (new epochs) | ≈ 2 s |

## 8. Write path (implemented, Phase 3)

### 8.1 Metadata store

SQLite (`modernc.org/sqlite`) at `DB_PATH` on the controller volume; schema
in `internal/controller/meta/schema.sql` (DEC-034). WAL, `synchronous=FULL`,
foreign keys, 10 s busy timeout, and every write transaction is
`BEGIN IMMEDIATE`; 16 concurrent uploads run without `SQLITE_BUSY` failures
(`TestConcurrentWriters`). No transaction spans a network call: the
coordinator alternates "short transaction" and "network I/O", and each
transaction re-checks its precondition (`status = 'UPLOADING'`, the expected
`PENDING` row, no pending delete for the target).

| Operation | One transaction that... |
| --- | --- |
| `CreateObject` | inserts the object as `UPLOADING` |
| `ClaimChunk` | checks `UPLOADING`, checks no pending delete on the targets (I8), inserts the chunk and its `PENDING` replicas |
| `MarkReplicaOK` | checks `UPLOADING`, flips that `PENDING` row to `OK` |
| `ReplaceReplica` | checks `UPLOADING`, moves the failed `PENDING` row to `pending_deletes`, inserts a `PENDING` row on the new worker |
| `CommitObject` | checks `UPLOADING`, re-verifies I1, I2, full replication, then sets `READY` with size, checksum, metadata |
| `MarkDeleting` | moves the object to `DELETING` and all its replica rows to `pending_deletes` (abort now; client delete in Phase 5) |
| `FinalizeDeletedObjects` | removes `DELETING` objects with no replicas and no pending deletes (cascade to chunks) |

### 8.2 Upload coordinator (`internal/controller/upload`)

1. **Admission:** fewer than RF eligible workers → `503 INSUFFICIENT_WORKERS`
   before anything is created. **Concurrency cap:** more than
   `MAX_CONCURRENT_UPLOADS` → `503 SERVER_BUSY`.
2. **Parts** are read in order; `metadata` may come before (validated at
   once) or after `file` (validated before commit; failure aborts).
3. **File part:** create the object `UPLOADING`; then loop: take a window
   slot (backpressure), read up to `CHUNK_SIZE` bytes into a pooled buffer
   (only a clean `io.EOF` ends the file, DEC-039), hash it, and hand it to a
   goroutine that places, claims, and writes it.
4. **Per chunk:** choose RF distinct eligible workers (random policy,
   excluding this upload's failed workers) → `ClaimChunk` → PUT to all targets
   in parallel → `MarkReplicaOK` per ack. On a PUT error or timeout: add the
   worker to the failed set, pick another worker not holding the chunk,
   `ReplaceReplica`, retry (DEC-035).
5. **Commit** in one conditional transaction; respond `201`.
6. **Any failure:** `MarkDeleting` on a detached context, kick the janitor,
   respond with the mapped error.

### 8.3 Janitor (minimal, `internal/controller/janitor`)

Every `JANITOR_INTERVAL` or on kick: DELETE due pending deletes on ALIVE
workers (bounded concurrency, idempotent; failures back off exponentially),
then purge `DELETING` objects with nothing left (DEC-037).

### 8.4 Crash windows (Phase 3 status)

| Crash or failure | What is left | Who cleans it |
| --- | --- | --- |
| Client disconnects mid-upload | `UPLOADING` object, claims | Handler aborts → janitor (tested) |
| Worker fails or times out on a PUT | Possibly bytes on that worker | Claim → pending delete → janitor (tested) |
| Worker stops mid-upload | As above, deleted when it returns | Janitor (tested) |
| Controller crashes mid-upload | `UPLOADING` object, `PENDING`/`OK` rows | **Not yet**: Phase 9 startup recovery |
| Controller crashes between a worker's rename and `MarkReplicaOK` | Claimed file; row still `PENDING` | Phase 9 recovery (`PENDING` → pending delete) |
| PUT lands after the janitor's DELETE for an aborted upload | Orphan file | Phase 9 inventory sweep |

### 8.5 Measured (Phase 3)

- Compose, default settings (fsync on), one 5.5 MB upload: 6 chunks, 12 files,
  ~166 ms (`upload_committed` log: ≈ 33 MB/s).
- Window benchmark (in-process, fsync off, 5 ms per PUT): see DEC-036.
- Random placement shows small-sample unevenness (one manual run put 0 of 12
  replicas on worker3), as the design doc anticipates for the random policy;
  rendezvous hashing is stretch S1.

## 9. Read path (implemented, Phase 4)

`internal/controller/download`, served at `GET`/`HEAD /api/v1/objects/{id}`:

1. Validate the ID (400); load the object, which must be `READY` (else 404, I7),
   and its chunk map (plain reads, no transaction held).
2. **Pre-flight:** every chunk must have an effective replica (`OK` on an
   `ALIVE` worker); otherwise `503 STORAGE_UNAVAILABLE`. `HEAD` stops here.
3. **Chunk 0** is fetched and verified before the response starts (DEC-041).
4. Set headers, `200`, flush; for each chunk in index order: fetch from a
   random effective replica, asking for at most size + 1 bytes; verify size
   and SHA-256; on failure classify (DEC-040: `CORRUPT` for bad data, missing,
   or unreadable header; skip for unreachable) and try the next replica; write
   and flush.
5. Before writing the last chunk, compare the whole-stream SHA-256 with the
   object checksum (DEC-042).
6. If a chunk after the first has no valid copy, abort the connection: the
   client gets a verified prefix shorter than `Content-Length`.

Order comes from metadata (`idx`), never from arrival. One chunk in memory
per download; at most `MAX_CONCURRENT_DOWNLOADS` downloads (DEC-043).

| Situation | Behaviour (all tested) |
| --- | --- |
| Holder stopped, still `ALIVE` in the registry | Read fails over to the other copy; nothing marked |
| Holder `DEAD` | Not tried at all |
| One copy appended / flipped / truncated / deleted / header destroyed | Correct bytes from the other copy; bad copy `CORRUPT`, never read again |
| Both copies of chunk 0 bad | Clean `503`; both `CORRUPT`; later requests refused at pre-flight |
| Both copies of a later chunk bad | `200` + verified prefix, then connection closed |
| Both holders of a chunk `DEAD` | `503` at pre-flight; copies stay `OK`; readable again once a holder returns |
| Stored object checksum wrong (assembly bug) | Last chunk withheld, connection closed |

Measured (Compose, 3.5 MB): download identical to upload; with a holder
stopped, identical; after appending bytes to one copy, 6 of 6 downloads
identical and one `corruption_detected` log line.

## 10. Metadata, list, and delete (implemented, Phase 5)

`internal/controller/api/objects.go`, backed by `meta`:

- **GET metadata:** object row + chunk map, then a status re-check (DEC-050);
  `ETag: "v<metadata_version>"`. No worker is contacted.
- **PATCH metadata:** `custommeta.ParsePatch` (strings or null) →
  `meta.UpdateMetadata` with `custommeta.Merge` inside one transaction
  (DEC-046); optional `If-Match` → 409; no-op merges write nothing. Chunks
  and replicas are never touched (asserted by comparing the chunk map before
  and after).
- **List:** keyset pagination over `READY` objects (DEC-047).
- **DELETE:** `MarkDeleting(READY → DELETING)` + replicas → pending deletes in
  one transaction, 204, janitor kick (DEC-048). The janitor deletes on `ALIVE`
  workers, retries the rest when they return, and purges the rows when
  nothing is left.

| Race | Behaviour (tested) |
| --- | --- |
| Concurrent PATCHes on one object | Serialised; none lost |
| PATCH vs DELETE | First to commit wins; the other gets 404 |
| DELETE during a streaming download | Download returns a correct prefix, then is cut |
| Listing while uploads commit | No duplicates; pre-existing objects all listed once |
| DELETE with a holder down | 204 at once; live holders cleaned; the down holder cleaned on return; rows purged then |

**Client:** `objctl` (DEC-049) and the Compose `client` service (`tools`
profile).

## 11. Degraded operation and resync (implemented, Phase 6)

**Inventory reconciliation** (`meta.ReconcileInventory`, wrapped by
`internal/controller/resync`) is the `InventoryReconciler` of every
registration. It runs while the worker is `SYNCING`; rules and mechanics in
DEC-051:

| Situation on registration | Result |
| --- | --- |
| File present, `OK` row, size matches | Nothing |
| `OK`/`CORRUPT` row, file missing (wiped volume, deleted file) | Row removed; chunk under-replicated |
| `OK` row, file size differs (bytes appended or truncated while down) | Row `CORRUPT` |
| `CORRUPT` row, file present | Nothing (repaired in Phase 7–8) |
| `PENDING` row | Nothing (owned by an operation in progress) |
| File present, no row (deleted object, aborted upload, unknown object) | Orphan → pending delete → janitor deletes it |

**Degraded operation.** Placement uses only `ALIVE` workers (§8), reads use
only effective replicas (§9), and the `SYNCING` state keeps a returning
worker out of both until reconciliation has finished (tested with
reconciliation held open: no GET and no PUT reached the worker).

**Observability.** `/cluster` has a `storage` section (DEC-052):
fully/under/over-replicated and zero-effective chunk counts, objects by
status, replicas by state, pending deletes. `GET /admin/audit` reports the
invariants (DEC-053).

**Measured** (Compose, default timings, one 5.5 MB object, worker2 holding 4
of 12 replicas):

| Step | `/cluster` chunks | Audit |
| --- | --- | --- |
| Healthy | 6 fully | ok, steady |
| worker2 stopped (DEAD) | 2 fully, 4 under-replicated; download identical | ok, not steady (I5: 4) |
| worker2 started (within grace) | 6 fully, no pending deletes; reconciliation `matched 4` | ok, steady |
| 7 bytes appended to one worker2 chunk, worker2 restarted | 5 fully, 1 under; reconciliation `size_mismatch_corrupt 1` | ok, not steady (I5: 1) |
| Volume wiped, worker restarted (e2e test) | missing rows removed; under-replicated = lost copies | ok, not steady |

## 12. Failure handling (summary)

See the design doc §5.2 (failure model) and §7 (failure scenario matrix).
Implemented so far: worker stop/start and controller restart for membership
(§7 above; tests `TestClusterMembership`,
`TestControllerRestartTriggersReregistration`,
`TestComposeClusterMembership`); worker crash mid-write leaves no partial
chunk (`TestWorkerKilledMidWriteLeavesNoPartialChunk`, e2e). Data-path
scenarios are implemented in Phases 4–9 and will be listed here with their
tests.

## 13. Trade-offs and limitations

Inherited from the design doc §8: single controller (SPOF); controller in the
data path (throughput ceiling); synchronous 2-way replication (write latency;
needs ≥ 2 workers); RF = 2 (one failure at a time); immutable objects; repair
grace period (longer vulnerability window); single-file metadata DB.

## 14. Deviations and clarifications vs. the design doc

| Topic | Design doc | Implementation | Decision |
| --- | --- | --- | --- |
| Worker `SYNCING` state | §4.5 uses it; §3.3 diagram omits it; §2.2 says it derives from heartbeat age | Registration-driven state with epochs; excluded from placement, reads, and repair; workers gate writes until registered | DEC-006 |
| Physical cleanup | Failed claims removed; orphans "scheduled for deletion" | Durable `pending_deletes` table, mutually exclusive with claims | DEC-007 |
| Repair target | "No replica of this chunk" but "prefer the CORRUPT holder" | No `OK`/`PENDING` replica and no pending delete; `CORRUPT → PENDING` in place | DEC-008 |
| Orphan sweep | Inventories only at registration | Janitor also pulls inventories periodically | DEC-009 |
| `GET /internal/workers/{id}/chunks` | Host unspecified | Served by the worker | DEC-010 |
| Stale-upload abort vs. live handler | Not addressed | Claims and commit conditional on status | DEC-011 |
| New parameters | — | `JANITOR_INTERVAL`, `INVENTORY_SWEEP_INTERVAL` | DEC-009 |
| Chunk size | 1 MiB = 1,048,576 bytes (§3.1, D3) | 1 MB = 1,000,000 bytes, as the specification says (owner decision) | DEC-005 |
| Download header timing | Headers right after pre-flight (§4.2 step 4) | Chunk 0 fetched and verified first; flush per chunk | DEC-041 |
| Object checksum on read | Client-side, optional (§3.6 layer 4) | Also checked by the controller before the last chunk is sent | DEC-042 |
| Missing chunk file on read | "Unreachable → try other replica" | Treated as `CORRUPT` (repairable in place) | DEC-040 |
| 409 | "If adopted" (§9.4) | Adopted: optional `If-Match` on PATCH | DEC-014 |
| Janitor for client deletes | Phase 5 scope | Built in Phase 3 (needed for aborts), reused | DEC-037, DEC-048 |
| Audit result | Pass/fail on I1–I5 | `ok` = safety invariants (I1, I2, I3, I7, I8); I5 reported as `steady_state` | DEC-053 |
| Missing file on registration vs. on read | §4.5: remove the row | Registration removes the row; a read marks it `CORRUPT` (both non-effective, both repaired in Phase 7) | DEC-040, DEC-051 |
