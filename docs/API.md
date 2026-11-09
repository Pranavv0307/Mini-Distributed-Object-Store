# API Reference

> **Status (end of Phase 6):** **every endpoint is IMPLEMENTED** and tested:
> all client endpoints of the specification (see the conformance checklist at
> the end of the client API section), all worker endpoints, the controller's
> membership endpoints (with real inventory reconciliation), `/healthz`,
> `/cluster` (workers and storage totals), and `/admin/audit` (v1). Repair
> counters (Phase 7) and audit I4 (Phase 9) are still to come. Each
> endpoint lists the phase that implements it; when that phase completes, its
> section is marked **IMPLEMENTED**. Until then, details may be refined, and
> any change is recorded in [`DECISIONS.md`](DECISIONS.md). Verified `curl`
> examples are in [`RUN_AND_TEST.md`](RUN_AND_TEST.md).

- Client API: served by the **controller**, published on host port `8080`.
- Internal API: controller endpoints under `/internal/workers/...` (called by
  workers) and worker endpoints under `/internal/chunks/...` (called by the
  controller). Internal endpoints are reachable only on the Compose network and
  are unauthenticated (architecture §5.10).

## Conventions

| Topic | Rule |
| --- | --- |
| Object ID | Canonical lowercase UUIDv4, e.g. `3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b`. A malformed ID returns 400 `INVALID_OBJECT_ID`; a well-formed unknown ID returns 404 (DEC-015). |
| Chunk ID | `{object_id}_{idx}`, `idx` zero-padded to 6 digits (`..._000000`). Regex: `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}_[0-9]{6}$`. Anything else, including `/` or `..`, is rejected with 400. Order is **never** inferred from the name; metadata `index` is authoritative. |
| Chunk size | 1,000,000 bytes (1 MB, DEC-005). The last chunk may be smaller. An empty object has 0 chunks. |
| Timestamps | RFC 3339, UTC, nanosecond precision (DEC-021). |
| Checksums | SHA-256, 64 lowercase hex characters (DEC-021). |
| Visibility | Only `READY` objects exist for clients. `UPLOADING` and `DELETING` objects behave exactly like unknown objects (404) and are never listed. |
| JSON | Request and response bodies with structured data are `application/json; charset=utf-8`. |

### Error format (all endpoints)

```json
{ "error": { "code": "OBJECT_NOT_FOUND", "message": "object 3f2b8c1e-... not found" } }
```

| HTTP | Code | Meaning |
| --- | --- | --- |
| 400 | `INVALID_REQUEST` | Malformed request (bad multipart, bad JSON, wrong content type) |
| 400 | `INVALID_OBJECT_ID` | Object ID is not a canonical UUIDv4 |
| 400 | `INVALID_CHUNK_ID` | Chunk ID fails the strict format (worker) |
| 400 | `MISSING_FILE` | Upload has no `file` part, or more than one |
| 400 | `INVALID_METADATA` | Custom metadata is not a flat object of string values (DEC-013) |
| 400 | `METADATA_LIMIT_EXCEEDED` | Too many keys, key or value too long |
| 400 | `OBJECT_TOO_LARGE` | Upload exceeds `MAX_OBJECT_SIZE` (DEC-016) |
| 400 | `INVALID_PAGINATION` | Bad `limit` or `cursor` |
| 400 | `CHECKSUM_MISMATCH` | Chunk bytes do not match the expected SHA-256 (worker) |
| 400 | `SIZE_MISMATCH` | Chunk body length differs from `Content-Length` (worker) |
| 400 | `CHUNK_TOO_LARGE` | Chunk body exceeds `CHUNK_SIZE` (worker) |
| 404 | `OBJECT_NOT_FOUND` | Unknown or non-`READY` object |
| 404 | `CHUNK_NOT_FOUND` | Worker does not hold the chunk |
| 404 | `WORKER_NOT_FOUND` | Inventory requested for a different worker ID (DEC-010) |
| 409 | `VERSION_CONFLICT` | `If-Match` does not match current `metadata_version` (DEC-014) |
| 500 | `INTERNAL` | Unexpected error (details only in logs) |
| 500 | `CHUNK_CORRUPT` | Worker cannot parse the chunk file header (DEC-027) |
| 503 | `INSUFFICIENT_WORKERS` | Fewer than `REPLICATION_FACTOR` ALIVE workers at upload start |
| 503 | `UPLOAD_FAILED` | Upload could not place a chunk on enough distinct workers |
| 503 | `STORAGE_UNAVAILABLE` | Download: some chunk has no effective (OK + ALIVE) replica |
| 503 | `NOT_REGISTERED` | Worker refuses writes before it is registered (DEC-006) |
| 409 | `REGISTRATION_SUPERSEDED` | A newer registration of the same worker started, or it was declared DEAD, during reconciliation; register again |
| 503 | `INSUFFICIENT_STORAGE` | Worker disk is full |
| 503 | `SERVER_BUSY` | `MAX_CONCURRENT_UPLOADS` uploads (or `MAX_CONCURRENT_DOWNLOADS` downloads) already in progress; retry later |

---

## Client API (controller)

### `POST /api/v1/objects` — upload an object · IMPLEMENTED (Phase 3)

**Request:** `multipart/form-data`, streamed (never buffered whole).

| Part | Required | Content |
| --- | --- | --- |
| `file` | yes, exactly once | Object bytes. Its `filename` becomes the object `name` (≤ 1024 bytes; may be empty; names are not unique). Its `Content-Type` becomes `content_type` (default `application/octet-stream`, must be a valid media type ≤ 256 bytes, DEC-017). |
| `metadata` | no, at most once | JSON object of string → string custom metadata (DEC-013, provisional): ≤ 50 keys, non-empty keys ≤ 128 bytes, values ≤ 1 KiB, document ≤ 64 KiB. An empty part means no metadata. May appear **before or after** `file`. |
| anything else | — | Ignored. |

Example (verified):

```bash
curl -s -F 'metadata={"owner":"alice"}' -F 'file=@big.bin;type=application/octet-stream'   localhost:8080/api/v1/objects
```

**Response 201** with `Location: /api/v1/objects/{object_id}` (verified,
5,500,000-byte file):

```json
{"object_id":"fce67c66-1d5d-4617-badb-adca7c0e1ba8","name":"big.bin","size":5500000,
 "content_type":"application/octet-stream",
 "checksum_sha256":"822817db53a46bde7be4f910ef106432460b5aa67336291a5bfbf686644772a3",
 "num_chunks":6,"created_at":"2026-11-09T11:06:28.872237206Z",
 "modified_at":"2026-11-09T11:06:29.032769513Z","metadata_version":1,
 "custom_metadata":{"owner":"alice"}}
```

`checksum_sha256` is the SHA-256 of the whole object (compare with
`sha256sum`). `num_chunks` = ⌈size / 1,000,000⌉; an empty file has 0 chunks.

**Errors:**

| Status | Code | When |
| --- | --- | --- |
| 400 | `INVALID_REQUEST` | Not `multipart/form-data`; malformed multipart; duplicate `metadata` part; bad file name or Content-Type; request body unreadable or truncated (client disconnected) |
| 400 | `MISSING_FILE` | No `file` part, or more than one |
| 400 | `INVALID_METADATA` | Metadata not a flat JSON object of strings |
| 400 | `METADATA_LIMIT_EXCEEDED` | Metadata over the limits above |
| 400 | `OBJECT_TOO_LARGE` | File larger than `MAX_OBJECT_SIZE` (default 5 GiB) |
| 503 | `INSUFFICIENT_WORKERS` | Fewer than `REPLICATION_FACTOR` (2) eligible `ALIVE` workers when the request arrives (nothing is stored) |
| 503 | `UPLOAD_FAILED` | A chunk could not be placed on 2 distinct workers because workers failed during the upload |
| 503 | `SERVER_BUSY` | `MAX_CONCURRENT_UPLOADS` (16) uploads in progress |
| 500 | `INTERNAL` | Unexpected error (details in the controller log) |

**Guarantees.**
- 201 is returned only after every chunk has `REPLICATION_FACTOR` OK replicas
  on distinct workers (each acknowledged after the worker verified the
  checksum and made the write durable) and the object was committed
  `UPLOADING → READY` in one transaction that re-verifies chunk indexes,
  sizes, and replication (DEC-034). After 201 the object exists for every
  later request.
- On any error, including client disconnect, **no object becomes visible**:
  the upload is aborted (`UPLOADING → DELETING`, all replica rows become
  pending deletes) and the janitor deletes every stored chunk file (DEC-035).
- A worker that fails or times out during an upload is not used again for
  that upload; its claim becomes a pending delete (a timeout does not prove
  the write failed) and the replica is written to another worker.
- Memory per upload is bounded by `UPLOAD_WINDOW` × 1 MB; when the window is
  full the controller stops reading the body (backpressure). A body read
  that stalls longer than `CLIENT_READ_TIMEOUT` (60 s) aborts the upload
  (DEC-036).

### `GET /api/v1/objects/{object_id}` — download · IMPLEMENTED (Phase 4)

Also answers `HEAD` (headers only; no chunk is fetched).

**Response 200**, body = the exact original bytes, with headers (verified):

```
Content-Type: application/pdf
Content-Length: 3500000
Content-Disposition: attachment; filename=report.pdf
Etag: "d2e2d9547eba9f87cf274718e746fb541e7156618e22b44b9a2336ffc123560f"
X-Object-Checksum-Sha256: d2e2d9547eba9f87cf274718e746fb541e7156618e22b44b9a2336ffc123560f
```

`Content-Disposition` carries the upload's file name with path components,
control characters, and quotes removed (non-ASCII names use RFC 2231
`filename*=`); it is a plain `attachment` if nothing is left.

**Errors:**

| Status | Code | When |
| --- | --- | --- |
| 400 | `INVALID_OBJECT_ID` | Not a canonical UUIDv4 |
| 404 | `OBJECT_NOT_FOUND` | Unknown, still `UPLOADING`, or `DELETING` |
| 503 | `STORAGE_UNAVAILABLE` | Some chunk has no effective replica (`OK` on an `ALIVE` worker), or chunk 0 has no valid copy. Nothing of the object is sent. |
| 503 | `SERVER_BUSY` | `MAX_CONCURRENT_DOWNLOADS` (32) in progress |
| 500 | `INTERNAL` | Unexpected error |

Error messages never name workers (clients do not learn chunk locations);
the controller log has the details.

**Guarantees (DEC-040 to DEC-044).**
- Every chunk is verified (size and SHA-256 against the controller's
  metadata) **before any of its bytes are sent**. Before the last chunk is
  sent, the SHA-256 of the whole stream is compared with the object's
  checksum.
- Copies are read in random order; on failure the next copy is tried. A copy
  that fails verification, is missing (`404`), or has an unreadable header is
  marked `CORRUPT` and is not read again. A copy that is unreachable (timeout,
  connection refused, 5xx) is skipped but not marked.
- Chunk 0 is fetched and verified before the response starts, so a missing
  or corrupt first chunk is a clean `503`. If a later chunk has no valid copy,
  the 200 response has started: the connection is closed, and the client
  receives a verified prefix shorter than `Content-Length`, **never wrong
  bytes**.
- Chunks are fetched one at a time; each download holds at most one chunk
  (1 MB) in memory.
- A delete racing with a streaming download (Phase 5) may cut the stream
  (architecture §5.6).

### `GET /api/v1/objects/{object_id}/metadata` — metadata · IMPLEMENTED (Phase 5)

**Response 200** with `ETag: "v<metadata_version>"`: every field of the
upload response plus the chunk map (verified example, abbreviated):

```json
{"object_id":"7f129833-eb06-4ffe-9cc1-87b7e53f9778","name":"obj.bin","size":2500000,
 "content_type":"application/octet-stream","checksum_sha256":"be435f21…","num_chunks":3,
 "created_at":"2026-11-09T11:42:31.380947647Z","modified_at":"2026-11-09T11:42:31.420749821Z",
 "metadata_version":1,"custom_metadata":{"owner":"alice"},
 "chunks":[{"index":0,"chunk_id":"7f129833-…_000000","size":1000000,"checksum_sha256":"fa7990ff…",
            "replicas":[{"worker_id":"worker1","state":"OK"},{"worker_id":"worker2","state":"OK"}]}, …]}
```

`chunks` is ordered by `index`; `replicas` by `worker_id`, with `state`
`PENDING`, `OK`, or `CORRUPT`. Showing replica locations is required by the
specification (§3 "chunk information and storage locations"; the replication
demonstration). Only worker IDs are shown, never addresses.

**Errors:** 400 `INVALID_OBJECT_ID`; 404 `OBJECT_NOT_FOUND` (unknown,
`UPLOADING`, or `DELETING`, including an object deleted while the chunk map
was being read, DEC-050).

### `PATCH /api/v1/objects/{object_id}/metadata` — update custom metadata · IMPLEMENTED (Phase 5)

**Request:** a JSON object. A string value adds or updates that key; `null`
removes it; keys not mentioned are unchanged (JSON merge semantics on a flat
map, DEC-014). The body is parsed as JSON whatever `Content-Type` says, so
plain `curl -X PATCH -d '{...}'` works (DEC-045). Optional
`If-Match: "v<n>"` (or `*`).

```bash
curl -s -X PATCH -d '{"owner":"bob","project":"demo"}' localhost:8080/api/v1/objects/$ID/metadata
curl -s -X PATCH -d '{"project":null}' localhost:8080/api/v1/objects/$ID/metadata
curl -s -X PATCH -H 'If-Match: "v3"' -d '{"x":"y"}' localhost:8080/api/v1/objects/$ID/metadata
```

**Response 200:** the same body as GET metadata, with the new
`metadata_version` (also as `ETag`) and `modified_at`.

**Errors:**

| Status | Code | When |
| --- | --- | --- |
| 400 | `INVALID_OBJECT_ID` | Malformed ID |
| 400 | `INVALID_REQUEST` | Malformed `If-Match` (must be `"v<n>"` or `*`) |
| 400 | `INVALID_METADATA` | Body not a JSON object, or a value that is not a string or null, or an empty key |
| 400 | `METADATA_LIMIT_EXCEEDED` | The **merged** result exceeds the limits (50 keys, key ≤ 128 B, value ≤ 1 KiB), or the body > 64 KiB |
| 404 | `OBJECT_NOT_FOUND` | Unknown, `UPLOADING`, or `DELETING` |
| 409 | `VERSION_CONFLICT` | `If-Match` version ≠ current version, e.g. `"metadata version conflict: current version 3, expected 1"` |

**Guarantees (DEC-046).** One transaction reads, merges, validates, and
writes, so concurrent PATCHes are serialised and none is lost (20 concurrent
PATCHes → 20 keys, version 21). A PATCH whose result equals the current
metadata changes nothing (same version, same `modified_at`), so retries are
idempotent. Chunks, replicas, size, and checksum are never touched. PATCH
racing DELETE: whichever commits first wins; the loser gets 404.

### `DELETE /api/v1/objects/{object_id}` — delete · IMPLEMENTED (Phase 5)

**Response 204**, empty body. **Errors:** 400 `INVALID_OBJECT_ID`; 404
`OBJECT_NOT_FOUND` (unknown, still uploading, or already deleted).

**Guarantees (D18, DEC-048).** Before 204 is returned, one transaction moves
the object `READY → DELETING` and turns every replica into a pending delete:
from that moment GET, HEAD, metadata, PATCH, list, and a second DELETE all
see 404. The janitor then deletes the chunk files (immediately, then every
`JANITOR_INTERVAL`), retrying with backoff, and removes the object's rows when
every copy is confirmed gone (log `object_purged`). Files on a worker that is
down are removed when it is `ALIVE` again. A download already streaming may be
cut off (§5.6): it returns a correct prefix, never wrong bytes.

### `GET /api/v1/objects` — list · IMPLEMENTED (Phase 5)

**Query:** `limit` (1–1000, default 100); `cursor` (opaque, from the
previous page's `next_cursor`). Other parameters are ignored.

**Response 200** (verified):

```json
{"objects":[{"object_id":"7f129833-…","name":"obj.bin","size":2500000,
             "content_type":"application/octet-stream","checksum_sha256":"be435f21…",
             "num_chunks":3,"created_at":"…","modified_at":"…"}, …],
 "next_cursor":"eyJ0IjoxNzk0MjI0NTUzMDgzNjA3NTQ0LCJpZCI6ImY4ZjIwMzZkLTJhNjItNGZhMC05MjFjLTBhZTYxZDYxYmI3OSJ9"}
```

`next_cursor` is `null` on the last page. Only `READY` objects are listed
(I7). Ordering: `created_at`, then `object_id`.

**Errors:** 400 `INVALID_PAGINATION` (`limit` not an integer in [1, 1000],
or a cursor that is not one this server issued).

**Guarantees (DEC-047).** Keyset pagination: no object is listed twice and
no object that exists for the whole listing is skipped, even while uploads
and deletes happen between pages. `created_at` is when the upload *started*,
so an object committed during a listing with a `created_at` before the
cursor is not included in that pass (it appears in the next listing).

### Conformance checklist (spec §5 and §5.7)

Every row is asserted by `TestSpecConformance` (status, JSON content type,
error code, non-empty message); all pass.

| Request | Expected | Code |
| --- | --- | --- |
| `POST /api/v1/objects` (file + metadata) | 201 | — |
| `POST` without a `file` part | 400 | `MISSING_FILE` |
| `POST` not multipart | 400 | `INVALID_REQUEST` |
| `POST` with fewer than 2 `ALIVE` workers | 503 | `INSUFFICIENT_WORKERS` |
| `GET /api/v1/objects/{id}` | 200 | — |
| `GET` unknown ID | 404 | `OBJECT_NOT_FOUND` |
| `GET` malformed ID | 400 | `INVALID_OBJECT_ID` |
| `GET` when a chunk has no valid copy | 503 | `STORAGE_UNAVAILABLE` |
| `GET /api/v1/objects/{id}/metadata` | 200 | — |
| `GET .../metadata` unknown ID | 404 | `OBJECT_NOT_FOUND` |
| `PATCH .../metadata` | 200 | — |
| `PATCH` unknown ID | 404 | `OBJECT_NOT_FOUND` |
| `PATCH` invalid value | 400 | `INVALID_METADATA` |
| `PATCH` stale `If-Match` | 409 | `VERSION_CONFLICT` |
| `DELETE /api/v1/objects/{id}` | 204 | — |
| `DELETE` again | 404 | `OBJECT_NOT_FOUND` |
| `GET` after delete | 404 | `OBJECT_NOT_FOUND` |
| `GET /api/v1/objects` | 200 | — |
| `GET /api/v1/objects?limit=0` | 400 | `INVALID_PAGINATION` |

500 `INTERNAL` has no deterministic trigger (unit tests cover the mapping);
503 `SERVER_BUSY` is covered by `TestServerBusy`.

### `GET /healthz` — liveness (controller) · IMPLEMENTED (Phase 2)

200 `{"status":"ok"}` while the controller is serving. (From Phase 3 it also
requires the metadata database to be open.) Used by the Compose healthcheck.

### `GET /cluster` — observability · IMPLEMENTED (Phase 2: workers; Phase 6: storage); repair counters in Phase 7

200, JSON snapshot (verified example, worker2 stopped):

```json
{
  "controller": {"version": "dev", "started_at": "2026-11-09T10:31:13.9Z"},
  "config": {"heartbeat_interval_ms": 3000, "dead_after_ms": 10000, "repair_grace_ms": 30000},
  "summary": {"total": 4, "alive": 3, "syncing": 0, "dead": 1},
  "workers": [
    {"worker_id": "worker2", "address": "http://worker2:9000", "state": "DEAD",
     "epoch": 1794220273940473530, "last_heartbeat_age_ms": 12489,
     "registered_at": "2026-11-09T10:31:14.322965428Z",
     "dead_since": "2026-11-09T10:31:24.943565474Z", "repair_eligible": false,
     "free_bytes": 1020651261952, "used_bytes": 0, "chunk_count": 0}
  ]
}
```

| Field | Meaning |
| --- | --- |
| `state` | `SYNCING` (registered, inventory being reconciled; not used for anything), `ALIVE`, `DEAD` |
| `epoch` | Registration epoch (DEC-029); changes on every (re-)registration |
| `last_heartbeat_age_ms` | Controller-clock time since the last heartbeat or registration (D11) |
| `dead_since` | `null` unless `DEAD` |
| `repair_eligible` | `DEAD` for longer than `REPAIR_GRACE` (D12); repair itself starts in Phase 7 |
| `free_bytes` | Free space on the worker's volume as reported by the worker; `-1` = unknown |
| `used_bytes`, `chunk_count` | Payload bytes and chunk files on the worker's volume |

Workers are sorted by ID. The registry is in memory only, so `workers` is
empty right after a controller restart until workers re-register (seconds).

**Storage section (Phase 6).** `config.replication_factor` and a `storage`
object (verified, worker2 stopped, one 5.5 MB object):

```json
"storage":{"chunks":{"total":6,"fully_replicated":2,"under_replicated":4,"zero_effective":0,"over_replicated":0},
           "objects_by_status":{"READY":1},"replicas_by_state":{"OK":12},"pending_deletes":0}
```

| Field | Meaning |
| --- | --- |
| `chunks.total` | Chunks of `READY` objects |
| `fully_replicated` | Exactly RF effective replicas (`OK` on an `ALIVE` worker) |
| `under_replicated` | 1 to RF−1 effective replicas: readable, at risk |
| `zero_effective` | No effective replica: unreadable (503) until a holder returns |
| `over_replicated` | More than RF effective replicas (trimmed from Phase 7) |
| `objects_by_status` | Object rows per status (`UPLOADING`, `READY`, `DELETING`) |
| `replicas_by_state` | Replica rows per state (`PENDING`, `OK`, `CORRUPT`) |
| `pending_deletes` | Chunk files queued for deletion (DEC-007) |

Computed on each request from the metadata and the current `ALIVE` set.

### `GET /admin/audit` — invariant audit · IMPLEMENTED (Phase 6, v1); I4 in Phase 9

Checks the correctness invariants against live metadata (architecture
§3.4, §5.9). Always 200 (500 only if the database fails). Verified example
(worker2 stopped):

```json
{"checked_at":"2026-11-09T…","ok":true,"steady_state":false,
 "checks":[{"id":"I1","description":"a READY object has chunk rows for every index 0..num_chunks-1, without gaps or duplicates","kind":"safety","violations":0,"examples":[]},
           …,
           {"id":"I5","description":"every chunk of every READY object has exactly 2 effective replicas (OK on an ALIVE worker)","kind":"steady_state","violations":4,"examples":["fd97…_000000: 1 effective", …]}]}
```

| Check | Kind | Invariant |
| --- | --- | --- |
| I1 | safety | A READY object has chunk rows for every index 0..n−1, no gaps or duplicates |
| I2 | safety | The sizes of a READY object's chunks sum to its size |
| I3 | safety | No two replica rows of one chunk on one worker |
| I7 | safety | The listing contains exactly the READY objects |
| I8 | safety | No (chunk, worker) has both a replica row and a pending delete |
| I5 | steady_state | Every chunk of every READY object has exactly RF effective replicas |

`ok` is true when every **safety** invariant holds; these must hold at all
times, and a false `ok` is a bug (also logged as `audit_violation`).
`steady_state` reports I5, which the design only promises "in steady state":
it is expected to be false while a worker is down or until repair (Phase 7)
has restored the lost copies. `examples` lists up to 10 violations per check.
Not in the audit: I4 (needs worker inventories, Phase 9) and I6 (checked by
every download). The checks are independent reads, so a check racing a
concurrent operation can report a transient I5 change; the safety checks only
look at READY objects, whose chunk rows never change.

---

## Internal API — controller endpoints (called by workers) · IMPLEMENTED (Phase 2)

### `POST /internal/workers/register`

Sent by a worker at every start and whenever a heartbeat reply says
`reregister`. The worker closes and drains its write gate before taking the
inventory snapshot, and keeps it closed until this call succeeds (DEC-006,
DEC-030), so the inventory is exact.

```json
{
  "worker_id": "worker2",
  "address": "http://worker2:9000",
  "free_bytes": 1000000000, "used_bytes": 12000000,
  "inventory": [ { "chunk_id": "3f2b...a6b_000000", "size": 1000000 } ]
}
```

**Controller behaviour:**
1. Validate (worker ID format; `address` is `http(s)://host[:port]` with no
   path or query; every chunk ID valid; sizes ≥ 0). Body limit 64 MiB.
2. Mark the worker `SYNCING` under a **new epoch** (DEC-029). A `SYNCING`
   worker is never used for placement, reads, or repair.
3. Reconcile the inventory against metadata (§4.5 table, implemented in
   Phase 6, DEC-051): matching files are left alone; rows whose file is
   missing are removed; `OK` rows whose file size differs become `CORRUPT`;
   `PENDING` rows are left to their operation; files no row claims are queued
   as orphan pending deletes. Logged as `inventory_reconciled` with counts.
4. Mark it `ALIVE` **only if that epoch is still current and the worker is
   still `SYNCING`.**

**Response 200** (only after step 4): `{"epoch": 1794220273940473530, "heartbeat_interval_ms": 3000}`.
The controller's `HEARTBEAT_INTERVAL` is the single source of the heartbeat
period.

**Errors:** 400 `INVALID_REQUEST`; 409 `REGISTRATION_SUPERSEDED` (a newer
registration of the same worker began, or the worker was declared `DEAD`,
while this one was reconciling); 503 `INTERNAL` (reconciliation failed; the
worker stays `SYNCING`). The worker retries every failure with exponential
backoff and jitter (200 ms → 5 s).

**Idempotency:** registering again is always safe; it supersedes the previous
registration.

### `POST /internal/workers/heartbeat`

Sent every `heartbeat_interval_ms` after a successful registration.

```json
{ "worker_id": "worker2", "epoch": 1794220273940473530, "free_bytes": 1000000000,
  "used_bytes": 12000000, "chunk_count": 12, "suspect_chunks": [] }
```

**Response 200** `{"action":"ok"}`, or `{"action":"reregister"}` when the
worker is unknown (e.g. the controller restarted), marked `DEAD`, or the epoch
is not its current one (DEC-020). A heartbeat never revives a `DEAD` worker;
only a new registration does. The controller records its own receive time
(D11). `suspect_chunks` is accepted and ignored until Phase 8.

**Errors:** 400 `INVALID_REQUEST`. Body limit 1 MiB.

**Worker behaviour on failure:** a failed heartbeat (controller down or
unreachable) is retried on the next tick; the worker keeps serving with its
write gate open. If the outage exceeds `DEAD_AFTER`, the controller marks it
`DEAD` and its next successful heartbeat gets `reregister`.

---

## Internal API — worker endpoints (called by the controller) · IMPLEMENTED (Phase 1)

Workers listen on port 9000 inside the Compose network. **Write gate
(DEC-006):** PUT and DELETE return 503 `NOT_REGISTERED` while the worker is
not accepting writes. With `CONTROLLER_URL` set (always, under Compose) the
worker accepts writes only while registered: the gate is closed and drained
before every registration and opened after it succeeds (DEC-030). Without
`CONTROLLER_URL` the worker runs standalone and accepts writes from startup
(DEC-026). GET, inventory, and healthz are always available.

### `PUT /internal/chunks/{chunk_id}`

**Request:** raw bytes. Headers: `Content-Length` (required; chunked
transfer encoding is rejected), `X-Chunk-SHA256` (required; 64 hex characters,
either case).

**Behaviour:** streams the body to `tmp/` while hashing; rejects it on size
or checksum mismatch; then fsync, rename into `chunks/`, fsync the directory
(when `FSYNC_ON_WRITE=true`). **Always atomically replaces** any existing file
(DEC-024): a retry rewrites identical bytes (idempotent), and re-sending the
correct bytes repairs a corrupt replica in place (DEC-008). A rejected PUT
never changes the existing file.

**Response 201** (no file existed) or **200** (an existing file was replaced):

```json
{"chunk_id":"3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000","size":1000000,
 "checksum_sha256":"934e1109...","created":true}
```

**Errors:** 400 `INVALID_CHUNK_ID` (including `..%2F` traversal attempts),
`INVALID_REQUEST` (missing/malformed checksum header, missing Content-Length),
`CHUNK_TOO_LARGE` (> `CHUNK_SIZE`), `CHECKSUM_MISMATCH`, `SIZE_MISMATCH`;
503 `NOT_REGISTERED`, `INSUFFICIENT_STORAGE` (disk full); 500 `INTERNAL`.

**Concurrency:** concurrent PUTs, GETs, and DELETEs of the same chunk are
serialised at the final rename/unlink/read by per-chunk locks. A reader
always sees either the old complete file or the new complete file.

### `GET /internal/chunks/{chunk_id}`

**Response 200:** body = every byte stored after the chunk header,
`Content-Type: application/octet-stream`, `Content-Length`, and
`X-Chunk-SHA256` = the checksum recorded in the header **when the chunk was
written**. The worker does **not** verify the payload: the **controller**
verifies size and SHA-256 against metadata (authority rule, architecture
§3.6). If the file was corrupted by appending or truncating bytes, the body
length differs from the original size. The body is capped at `CHUNK_SIZE + 1`
bytes so an oversized file stays detectable without unbounded memory (DEC-025).

**Errors:** 400 `INVALID_CHUNK_ID`; 404 `CHUNK_NOT_FOUND`; 500
`CHUNK_CORRUPT` (the header itself is unreadable: bad magic, unknown version,
or the file is shorter than the header); 500 `INTERNAL`.

**Resource bound:** at most `MAX_CONCURRENT_READS` GETs hold a chunk in
memory at once; others wait (and give up if the client disconnects).

### `DELETE /internal/chunks/{chunk_id}`

**Response 204** whether or not the chunk existed (idempotent, §5.5).
**Errors:** 400 `INVALID_CHUNK_ID`; 503 `NOT_REGISTERED`; 500 `INTERNAL`.

### `GET /internal/workers/{worker_id}/chunks`

**Response 200:**

```json
{"worker_id":"worker1","chunks":[{"chunk_id":"3f2b8c1e-..._000000","size":1000000}]}
```

Lists committed chunk files sorted by chunk ID; `chunks` is `[]` (never
`null`) when empty. Temp files are excluded. `size` is the **payload size on
disk** (file size − 48-byte header), taken from file metadata only, so
appended or truncated bytes show up as a size mismatch against metadata.
Files whose names are not valid chunk IDs are ignored (and logged).
**Errors:** 404 `WORKER_NOT_FOUND` if `worker_id` is not this worker (DEC-010).

### `GET /healthz` (worker)

200 `{"status":"ok","worker_id":"worker1","registered":false,"accepting_writes":true}`.
`registered` is true while the worker holds a successful registration;
`accepting_writes` is the write gate's state (both false between a
`reregister` reply and the next successful registration).
