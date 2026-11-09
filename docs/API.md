# API Reference

> **Status (end of Phase 4):** **IMPLEMENTED** and tested: all worker
> endpoints (Phase 1); the controller's membership endpoints, `/healthz`, and
> `/cluster` (Phase 2); `POST /api/v1/objects` (Phase 3);
> **`GET`/`HEAD /api/v1/objects/{object_id}`** (Phase 4). Everything else is
> the **API contract** for later phases. Each
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

### `GET /api/v1/objects/{object_id}/metadata` — metadata · *Phase 5*

**Response 200** with `ETag: "v<metadata_version>"`: the upload response fields
plus the chunk map:

```json
{
  "object_id": "...", "name": "...", "size": 5767168, "content_type": "...",
  "checksum_sha256": "...", "num_chunks": 6,
  "created_at": "...", "modified_at": "...", "metadata_version": 1,
  "custom_metadata": { "owner": "alice" },
  "chunks": [
    { "index": 0, "chunk_id": "3f2b...a6b_000000", "size": 1000000,
      "checksum_sha256": "...",
      "replicas": [ { "worker_id": "worker1", "state": "OK" },
                    { "worker_id": "worker3", "state": "OK" } ] }
  ]
}
```

Replica `state` is one of `PENDING`, `OK`, `CORRUPT`. Worker liveness is not
included here (see `/cluster`). **Errors:** 400 `INVALID_OBJECT_ID`; 404.

### `PATCH /api/v1/objects/{object_id}/metadata` — update custom metadata · *Phase 5*

**Request:** `application/json` object. A string value adds or updates a key;
`null` removes a key; keys not mentioned are unchanged (DEC-014). Optional
header `If-Match: "v<metadata_version>"`.

```json
{ "owner": "bob", "obsolete-key": null }
```

**Response 200:** same body as GET metadata (new `metadata_version`,
`modified_at` updated).

**Errors:** 400 `INVALID_REQUEST` (not a JSON object) / `INVALID_METADATA`
(non-string, non-null value) / `METADATA_LIMIT_EXCEEDED` (limits checked on the
merged result); 404; 409 `VERSION_CONFLICT`.

**Guarantees.** Single DB transaction; never touches chunks, replicas, or
workers. PATCH racing DELETE: whichever commits first wins; the loser gets 404.

### `DELETE /api/v1/objects/{object_id}` — delete · *Phase 5*

**Response 204**, empty body. **Errors:** 400 `INVALID_OBJECT_ID`; 404.

**Guarantees.** The object disappears for all clients atomically (status
`READY → DELETING`) before 204 is returned. Physical removal of chunk files
happens afterwards and is retried until it completes, including on workers
that are down at delete time (D18, DEC-007). A download already streaming may be
aborted mid-stream (architecture §5.6).

### `GET /api/v1/objects` — list · *Phase 5*

**Query:** `limit` (1–1000, default 100); `cursor` (opaque string from a
previous response).

**Response 200:**

```json
{
  "objects": [
    { "object_id": "...", "name": "...", "size": 123, "content_type": "...",
      "checksum_sha256": "...", "created_at": "...", "modified_at": "..." }
  ],
  "next_cursor": "eyJ0IjoiMjAyNi0..."
}
```

`next_cursor` is `null` on the last page. Ordering: `created_at`, then
`object_id` (keyset pagination; stable under concurrent inserts).
**Errors:** 400 `INVALID_PAGINATION`.

### `GET /healthz` — liveness (controller) · IMPLEMENTED (Phase 2)

200 `{"status":"ok"}` while the controller is serving. (From Phase 3 it also
requires the metadata database to be open.) Used by the Compose healthcheck.

### `GET /cluster` — observability · IMPLEMENTED (Phase 2: workers); chunk and repair counters in Phases 6–8

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

### `GET /admin/audit` — invariant audit · *Phase 6 (v1), Phase 9 (v2)*

Evaluates invariants I1–I7 and returns violations. Schema fixed in Phase 6.

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
3. Reconcile the inventory against metadata (§4.5 table). *Phase 2: no
   metadata yet, so this only logs the inventory size; implemented in Phase 6.*
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
