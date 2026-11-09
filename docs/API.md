# API Reference

> **Status (Phase 0):** this document is the **API contract**. No endpoint is
> implemented yet. Each endpoint lists the phase that implements it; when that
> phase completes, its section is marked **IMPLEMENTED** and verified examples
> are added. Until then, details may be refined, and any change is recorded in
> [`DECISIONS.md`](DECISIONS.md).

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
| Chunk size | 1,048,576 bytes (PROVISIONAL, DEC-005). The last chunk may be smaller. An empty object has 0 chunks. |
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
| 404 | `OBJECT_NOT_FOUND` | Unknown or non-`READY` object |
| 404 | `CHUNK_NOT_FOUND` | Worker does not hold the chunk |
| 404 | `WORKER_NOT_FOUND` | Inventory requested for a different worker ID (DEC-010) |
| 409 | `VERSION_CONFLICT` | `If-Match` does not match current `metadata_version` (DEC-014) |
| 500 | `INTERNAL` | Unexpected error (details only in logs) |
| 503 | `INSUFFICIENT_WORKERS` | Fewer than `REPLICATION_FACTOR` ALIVE workers at upload start |
| 503 | `UPLOAD_FAILED` | Upload could not place a chunk on enough distinct workers |
| 503 | `STORAGE_UNAVAILABLE` | Download: some chunk has no effective (OK + ALIVE) replica |
| 503 | `NOT_REGISTERED` | Worker refuses writes before it is registered (DEC-006) |
| 503 | `INSUFFICIENT_STORAGE` | Worker disk is full |

---

## Client API (controller)

### `POST /api/v1/objects` — upload an object · *Phase 3*

**Request:** `multipart/form-data`, streamed (never buffered whole).

| Part | Required | Content |
| --- | --- | --- |
| `file` | yes, exactly once | Object bytes. `filename` → object `name`; part `Content-Type` → object `content_type` (default `application/octet-stream`, DEC-017). |
| `metadata` | no | JSON object of string → string custom metadata. May appear before or after `file`. |

**Response 201** with `Location: /api/v1/objects/{object_id}`:

```json
{
  "object_id": "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b",
  "name": "photo.jpg",
  "size": 5767168,
  "content_type": "image/jpeg",
  "checksum_sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "num_chunks": 6,
  "created_at": "2026-11-09T14:10:00.123456789Z",
  "modified_at": "2026-11-09T14:10:00.123456789Z",
  "metadata_version": 1,
  "custom_metadata": { "owner": "alice" }
}
```

**Errors:** 400 `INVALID_REQUEST` / `MISSING_FILE` / `INVALID_METADATA` /
`METADATA_LIMIT_EXCEEDED` / `OBJECT_TOO_LARGE`; 503 `INSUFFICIENT_WORKERS`
(admission check) or `UPLOAD_FAILED`; 500.

**Guarantees.** 201 is returned only after every chunk has `REPLICATION_FACTOR`
acknowledged replicas on distinct workers and the object was committed
`UPLOADING → READY` in one transaction. On any failure or client disconnect, no
object becomes visible and stored chunks are cleaned up by the delete path.
After 201, every subsequent GET, metadata, or list request sees the object.

### `GET /api/v1/objects/{object_id}` — download · *Phase 4*

**Response 200**, body = exact original bytes, with headers:
`Content-Type`, `Content-Length`,
`Content-Disposition: attachment; filename="<sanitised name>"`,
`ETag: "<checksum_sha256>"`, `X-Object-Checksum-SHA256: <hex>`.

**Errors:** 400 `INVALID_OBJECT_ID`; 404 `OBJECT_NOT_FOUND`; 503
`STORAGE_UNAVAILABLE` (pre-flight found a chunk with no effective replica).

**Guarantees.** Each chunk is verified against its metadata SHA-256 before any
of its bytes are sent; a mismatching replica is marked `CORRUPT` and the other
replica is used. If a failure occurs **after** the 200 headers were sent, the
connection is aborted, so the client sees a short body (length mismatch),
never wrong bytes.

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
    { "index": 0, "chunk_id": "3f2b...a6b_000000", "size": 1048576,
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

### `GET /healthz` — liveness (controller) · *Phase 2*

200 `{"status":"ok"}` once the controller has opened its database and is serving.

### `GET /cluster` — observability · *Phase 2 (workers), Phase 6–8 (chunk and repair counters)*

JSON snapshot: workers (`worker_id`, `address`, `state`
`ALIVE|SYNCING|DEAD`, heartbeat age, `dead_since`, chunk count, bytes),
chunk totals (fully replicated / under-replicated / zero-effective),
repairs (in flight / completed / failed), objects by status. Exact schema is
fixed in Phase 2.

### `GET /admin/audit` — invariant audit · *Phase 6 (v1), Phase 9 (v2)*

Evaluates invariants I1–I7 and returns violations. Schema fixed in Phase 6.

---

## Internal API — controller endpoints (called by workers)

### `POST /internal/workers/register` · *Phase 2*

```json
{
  "worker_id": "worker2",
  "address": "http://worker2:9000",
  "free_bytes": 1000000000, "used_bytes": 12582912,
  "inventory": [ { "chunk_id": "3f2b...a6b_000000", "size": 1048576 } ]
}
```

**Response 200** `{"epoch": 7, "heartbeat_interval_ms": 3000}`.
The controller marks the worker `SYNCING`, reconciles the inventory against
metadata (architecture §4.5 table; DEC-006/007), then marks it `ALIVE` if the
epoch is still current. The reply is sent after reconciliation. **Errors:**
400 (bad ID, address, or chunk IDs).

Workers retry registration with backoff until it succeeds, and refuse chunk
writes until then (DEC-006).

### `POST /internal/workers/heartbeat` · *Phase 2 (suspects used in Phase 8)*

```json
{ "worker_id": "worker2", "epoch": 7, "free_bytes": 1000000000,
  "used_bytes": 12582912, "chunk_count": 12, "suspect_chunks": [] }
```

**Response 200** `{"action": "ok"}` or `{"action": "reregister"}` when the
worker is unknown (e.g. controller restarted), marked `DEAD`, or the epoch is
stale (DEC-020). Liveness is judged on the controller's receive time only (D11).

---

## Internal API — worker endpoints (called by the controller)

### `PUT /internal/chunks/{chunk_id}` · *Phase 1*

**Request:** raw bytes (`application/octet-stream`). Headers:
`Content-Length` (required), `X-Chunk-SHA256` (required, expected checksum).

**Behaviour:** stream to a temp file while hashing; reject on size or
checksum mismatch; fsync, rename, fsync directory (when `FSYNC_ON_WRITE`).
**Idempotent:** if a valid file with the same checksum already exists, the
result is a no-op success; if an existing file is invalid or differs, it is
atomically replaced (in-place corrupt repair, DEC-008).

**Responses:** 201 stored; 200 already present and identical. **Errors:** 400
`INVALID_CHUNK_ID` / `CHECKSUM_MISMATCH` / `SIZE_MISMATCH` / `INVALID_REQUEST`;
503 `NOT_REGISTERED` / `INSUFFICIENT_STORAGE`.

### `GET /internal/chunks/{chunk_id}` · *Phase 1*

**Response 200:** chunk payload bytes, `Content-Length`, and
`X-Chunk-SHA256` = checksum stored in the chunk header. The worker returns the
bytes as stored; the **controller** verifies them against metadata (authority
rule, architecture §3.6). **Errors:** 400 `INVALID_CHUNK_ID`; 404 `CHUNK_NOT_FOUND`.

### `DELETE /internal/chunks/{chunk_id}` · *Phase 1*

**Response 204** whether or not the chunk existed (idempotent, §5.5).
**Errors:** 400 `INVALID_CHUNK_ID`; 503 `NOT_REGISTERED`.

### `GET /internal/workers/{worker_id}/chunks` · *Phase 1*

**Response 200:** `{"worker_id": "worker2", "chunks": [ {"chunk_id": "...", "size": 1048576} ]}`
listing fully written chunks (temp files excluded). `size` is the payload
size from the chunk header. **Errors:** 404 `WORKER_NOT_FOUND` if
`worker_id` is not this worker (DEC-010).

### `GET /healthz` (worker) · *Phase 1*

200 `{"status":"ok","worker_id":"worker2","registered":true}`.
