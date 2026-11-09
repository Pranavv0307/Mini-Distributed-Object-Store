# Mini Distributed Object Store — Architecture Document

**Version:** 1.0 (living document) · **Written:** 8 Nov 2026
**Scope:** architecture only: components, data, flows, policies, deployment, milestones.
**Not in scope:** code structure, class design, library choices (that comes after this doc is agreed).
**Language-neutral:** nothing here depends on Go, Java, or Python.

---

## 0. How to read this document

The document goes from the big picture down to the details. Read it in order the first time. After that, use it as a reference.

| Layer | Part | Question it answers |
| --- | --- | --- |
| L1 | §1 Big picture | What are we building, why, and what are the principles? |
| L2 | §2 Components | Which processes exist and what is each one responsible for? |
| L3 | §3 Data architecture | What do we store, where, with which identifiers, and what states can things be in? |
| L4 | §4 Flows | What exactly happens, step by step, in every operation? |
| L5 | §5 Policies | Consistency, failure model, timing, retries, races, edge cases, errors |
| L6 | §6 Deployment | How does it run on a laptop with Docker Compose? |
| — | §7 Failure matrix | Every failure we promise to handle, and how it will be demonstrated |
| — | §8 Trade-offs | What we knowingly give up |
| — | §9 Open decisions | What *you* must decide before building |
| — | §10 Milestones | The order to build in, with a checkpoint for each |
| — | §11 Interview prep | Questions this design should let you answer |

**Changes from our earlier chat discussion** (so nothing surprises you):

1. **Claim before write.** The controller records a chunk's replica rows *before* sending bytes to workers. Earlier I said "after acks". Recording intent first makes crash cleanup exact (§4.1, D10).
2. **Abort is just delete.** An upload that fails is cleaned up by the same code path as a normal delete (§4.4).
3. **One repair path.** Under-replicated, corrupt, and missing chunks are all repaired by the same mechanism (§4.7, §4.9).

---

# L1 — THE BIG PICTURE

## 1.1 The problem, in plain words

One computer has two weaknesses: **limited disk** and **a single point of failure**. If it dies, your files die with it. The cure is to spread data over several machines. That creates new problems, and every part of this project exists to answer one of them:

| New problem | Our answer | Where |
| --- | --- | --- |
| A file may be bigger than is convenient to place on one machine | Cut it into fixed **1 MiB chunks** and spread the chunks | §3.1, §4.1 |
| A machine can die and take its chunks with it | Keep **2 copies** (replication factor RF = 2) on *different* machines | §4.1 |
| Someone has to remember where everything is | A **controller** holds the map (metadata) | §2.2, §3 |
| How do we notice a dead machine? | Machines send periodic **heartbeats**; silence means dead | §4.6 |
| After a death we're down to one copy | The controller **copies chunks to a healthy machine** (self-healing) | §4.7 |
| Disks can silently flip bits | **Checksums** (fingerprints) are checked on every read and in the background | §3.6, §4.9 |

This is the same set of ideas that sits behind Amazon S3, HDFS, and Google's GFS, just smaller.

## 1.2 Goals and non-goals

**Goals**

- Store and retrieve arbitrary binary objects through an S3-style REST API.
- Survive the failure of any single worker with no data loss and no downtime for reads.
- Detect and repair under-replication and corruption automatically.
- Be fully demonstrable on one laptop with Docker Compose.
- Be *explainable*: every behavior has a documented reason.

**Non-goals (deliberately left out; mention them as "future work" in interviews)**

- Authentication, encryption, multi-tenancy.
- Controller high availability (consensus/Raft). The controller is a known single point of failure (§8).
- Erasure coding, data rebalancing, object versioning, partial overwrites.
- Multi-datacenter operation.

## 1.3 Requirements traceability (spec → mechanism)

| Spec requirement | Mechanism in this design | See |
| --- | --- | --- |
| 1 controller, 4 workers, 1+ clients | Process topology | §1.4, §6 |
| 1 MB chunks, ordered, unique IDs, last chunk may be smaller | Chunker + chunk ID scheme + `idx` field | §3.1, §3.2 |
| Replication factor 2, copies on different workers | Synchronous 2-way write; "distinct workers" placement rule | §4.1, §5.3 |
| Chunks distributed across workers | Placement policy | §5.3 |
| Object metadata + custom key-value metadata | `objects` record + custom metadata map | §3.2 |
| Client REST API | Controller API front end | §2.2, §5.8 |
| Internal worker APIs (register, heartbeat, chunk PUT/GET/DELETE, list) | Worker API + membership API | §2.3, §2.4 |
| Detect worker unavailability | Heartbeats + failure detector | §4.6 |
| Keep serving when a copy remains | Replica fallback on reads | §4.2 |
| Detect lost replication and restore it | Level-triggered reconciler | §4.7 |
| Detect corruption, don't return it, repair it | Layered checksums, verify-on-read, scrubber, unified repair | §3.6, §4.9 |
| Persistent worker storage | Docker named volumes | §6 |
| Docker / Compose only, no Kubernetes | Compose topology | §6 |
| 503 when storage unavailable | Error mapping | §5.8 |

## 1.4 The system at a glance

```
                         ┌──────────────────────────────────────────────┐
                         │                 CONTROLLER                   │
   ┌────────┐  REST/JSON │  ┌────────────┐   ┌──────────────────────┐   │
   │ Client │───────────▶│  │ API front  │──▶│ Upload / Download    │   │
   │ (curl, │◀───────────│  │ end        │   │ coordinators         │   │
   │  CLI)  │  binary    │  └────────────┘   └─────────┬────────────┘   │
   └────────┘            │       ┌──────────────┐      │                │
                         │       │ Metadata DB  │◀─────┤ (source of     │
                         │       │ (persistent) │      │  truth)        │
                         │       └──────────────┘      │                │
                         │  Background loops:  failure detector ·      │
                         │  reconciler (self-healing) · janitor        │
                         └──────────────┬──────────────────────────────┘
                 chunk PUT/GET/DELETE   │   ▲  register / heartbeat
                 (controller → worker)  ▼   │  (worker → controller)
        ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
        │ worker1   │ │ worker2   │ │ worker3   │ │ worker4   │
        │ chunk     │ │ chunk     │ │ chunk     │ │ chunk     │
        │ files on  │ │ files on  │ │ files on  │ │ files on  │
        │ a volume  │ │ a volume  │ │ a volume  │ │ a volume  │
        └───────────┘ └───────────┘ └───────────┘ └───────────┘
              Workers never talk to each other. Only the controller decides.
```

## 1.5 Six design principles (they explain almost every decision below)

1. **One brain, many hands.** The controller decides everything: placement, liveness, repair. Workers are deliberately simple: store bytes, return bytes, report health. Fewer decision-makers means no split-brain and far simpler reasoning.
2. **Metadata is the truth.** The controller's database is the single source of truth about which objects exist, their chunk order, checksums, and which workers hold which copies. Whatever the workers' disks contain is only "evidence" to be compared against it.
3. **Immutable data.** Chunks and objects are never modified in place. An object is written once, and a "re-upload" is a new object. This removes whole classes of problems: stale replicas, partial overwrites, write conflicts.
4. **Intent first, action second, commit last.** Record what you are about to do, do it, then flip one atomic switch to make it visible. After any crash, leftovers are always detectable and cleanable.
5. **Desired state vs observed state, reconciled in a loop.** The desired state is "every chunk has RF healthy copies on distinct live workers". Background loops continuously compare reality to this and fix differences. Loops that re-check state are robust to missed events and crashes; "do something once when a worker dies" is not.
6. **Trust nothing you didn't verify.** Every transfer is verified by the receiver against a checksum the controller holds. Corruption is treated as a normal event, not an exception.

## 1.6 Decision log (summary)

Each row is explained in the section noted. If you can't recite the *why* for a row, re-read that section before building the corresponding milestone.

| ID | Decision | Main alternative(s) rejected | Why (short) | §|
| --- | --- | --- | --- | --- |
| D1 | Single controller + 4 workers | Peer-to-peer (Dynamo-style), Raft-replicated controller | Spec-mandated shape; simplest to reason about | 2 |
| D2 | Controller proxies data (data path goes through it) | Client talks to workers directly (HDFS-style) | Spec says clients must not know locations; one simple endpoint | 4.1 |
| D3 | Fixed 1 MiB chunks | Variable or larger chunks | Spec-mandated; bounded memory; good repair granularity | 3.1 |
| D4 | RF = 2, synchronous, all-or-nothing | Ack after 1 copy then copy async | Durability at commit; simple invariant | 4.1 |
| D5 | Chunk ID = `{object_id}_{zero-padded idx}` | Content hash; global counter | Globally unique, debuggable, easy per-object cleanup | 3.1 |
| D6 | Objects and chunks are immutable | In-place update | Eliminates stale-replica and conflict problems | 1.5 |
| D7 | Metadata in an embedded transactional DB (SQLite-class) on a persistent volume | Postgres; KV store; in-memory + snapshots | Atomic multi-row commits, SQL for reconciler queries, no extra container | 3.5 |
| D8 | Workers are "dumb"; metadata decides location, order, checksum | Workers manage their own indexes | Single source of truth | 1.5 |
| D9 | Object states: UPLOADING → READY → DELETING | Insert only at end | Exact crash cleanup; visibility control | 3.3 |
| D10 | Claim-before-write: replica rows recorded as PENDING before bytes are sent | Record after acks | No unclaimed chunk files except from a crash window the sweep covers | 4.1 |
| D11 | Liveness from heartbeats, judged on the controller's clock; DEAD after 10 s | Worker-reported timestamps; pings from controller | No cross-machine clock dependence | 4.6 |
| D12 | Repair starts only after a 30 s grace period past DEAD | Repair immediately | Avoids mass copying on quick restarts and flapping | 4.6 |
| D13 | Level-triggered reconciler loops | Event-triggered repair | Survives crashes and missed events | 4.7 |
| D14 | Repair is a controller-mediated copy, risk-prioritized, throttled | Worker-to-worker transfer; unthrottled | Reuses existing internal API; protects client traffic | 4.7 |
| D15 | SHA-256 per chunk + whole object; receiver verifies; controller's copy is authoritative | CRC32; worker-only checksums | End-to-end integrity | 3.6 |
| D16 | Corruption is repaired by the same path as under-replication | Special-case repair | One mechanism to build and test | 4.9 |
| D17 | Pluggable placement; random first, rendezvous hashing later | Fixed policy | Ship early, improve with measurements | 5.3 |
| D18 | Delete = logical first (DELETING), physical removal afterwards with retries | Fully synchronous delete | Correct even if a worker is down | 4.4 |
| D19 | Stream with a bounded number of in-flight chunks | Buffer whole object | Bounded memory; backpressure | 5.1 |
| D20 | Workers have stable IDs; re-register with an inventory on every start or return from DEAD | Controller trusts its memory | Handles restarts, wiped disks, orphans uniformly | 4.5 |
| D21 | Observability is a feature: `/cluster`, structured event logs, audit | Logs only | The demo *is* the deliverable | 5.9 |
| D22 | Explicit Compose services (not `--scale`), one published port | Scaled replicas | Named containers and per-worker volumes needed for `docker stop worker2` | 6 |

---

# L2 — COMPONENTS

## 2.1 Client

Any HTTP client: `curl`, a small CLI, or a scripted client container used for demos and benchmarks. Clients know only the controller's address. They never see worker addresses and never need them (spec §5.2).

## 2.2 Controller

One process, one persistent volume. It is internally split into modules. These are *responsibilities*, not code classes.

| Module | Responsibility | Runs as |
| --- | --- | --- |
| **API front end** | Parse and validate HTTP requests, map outcomes to status codes and JSON errors | Per-request |
| **Upload coordinator** | Chunk the incoming stream, place chunks, fan out to workers, commit | Per-request |
| **Download coordinator** | Resolve chunk map, fetch from replicas in order, verify, stream out | Per-request |
| **Metadata store** | All reads/writes of persistent metadata; enforces transactions | Shared |
| **Worker registry** | In-memory view of workers: address, last heartbeat, state | Shared (locked) |
| **Placement engine** | Choose which workers receive which chunk | Called by coordinators and reconciler |
| **Worker client** | The only code that talks to workers: timeouts, retries, checksum headers | Shared |
| **Failure detector** | Marks workers DEAD/ALIVE/SYNCING from heartbeat age | Background loop |
| **Reconciler** | Finds under-replicated/corrupt/missing chunks and repairs them | Background loop |
| **Janitor** | Removes dead uploads, finishes deletes, sweeps orphan chunks | Background loop |
| **Admin and observability** | `/healthz`, `/cluster`, audit, metrics, event log | Per-request + passive |

**Why the controller sits in the data path (D2).** Two designs exist:

- *Control/data split* (HDFS): the controller hands the client a list of workers and the client streams to them directly. This scales better because the controller isn't a bandwidth bottleneck.
- *Proxy* (ours): the client sends everything to the controller, which forwards to workers.

We choose the proxy because the spec requires that clients not know chunk locations and exposes a single upload endpoint. It also gives us one place to compute checksums, verify data, and handle retries and failures. **Cost accepted:** the controller's network and CPU cap throughput. We will measure this and discuss it in the design doc and interviews as a known limitation.

## 2.3 Worker

One process per worker, each with its own persistent volume. A worker has five responsibilities:

| Module | Responsibility |
| --- | --- |
| **Chunk API** | Accept, serve, delete chunks; list its inventory |
| **Chunk store** | On-disk layout; atomic write (temp file then rename); stores a checksum with each chunk; verifies data against the expected checksum on write |
| **Membership agent** | Register at startup (with inventory); send heartbeats forever |
| **Scrubber** | Slowly re-reads its own chunks and re-checks checksums; reports suspects |
| **Startup recovery** | Delete leftover temp files from a crash; build the inventory |

A worker makes **no decisions** about anything outside its own disk. It does not know what an object is.

## 2.4 Who talks to whom

| From → To | Purpose | Style |
| --- | --- | --- |
| Client → Controller | All user operations | REST, JSON for metadata, binary for objects |
| Controller → Worker | Store, fetch, delete chunks; copy for repair | REST (internal) |
| Worker → Controller | Register (with inventory), heartbeat | REST (internal) |
| Worker ↔ Worker | **Never** | (deliberate) |
| Operator → containers | Fault injection (`docker stop`, `docker exec` to corrupt a file) | Docker CLI |

**Why workers never talk to each other:** every interaction has a single decision-maker and a single place to log it. The cost is that repair traffic flows through the controller (D14).

---

# L3 — DATA ARCHITECTURE

## 3.1 Identifiers

| Thing | Format | Why |
| --- | --- | --- |
| **Object ID** | Random UUID (v4), or a time-sortable variant | Needs no coordination (any request can mint one); collision-proof for our purposes; not guessable. A counter would need a central lock and leaks object counts. |
| **Chunk ID** | `{object_id}_{idx}` with `idx` zero-padded (e.g. 6 digits) | Globally unique because `object_id` is. Prefix lets you find all chunks of one object (cleanup, debugging, `ls` on a worker volume). **Order is never inferred from the name**; the `idx` field in metadata is authoritative. |
| **Worker ID** | Stable human-readable string from config (`worker1`…) | Must survive restarts, because the persistent disk belongs to *that* identity. |

**Chunk size (D3).** Fixed at 1 MiB (1,048,576 bytes); the spec says "1 MB", so *pick one definition and use it everywhere, including docs and tests.* Fixed size gives predictable memory per in-flight chunk and a uniform repair unit. The final chunk may be smaller. An object whose size is an exact multiple of the chunk size has **no empty trailing chunk**. An empty (0-byte) object has **0 chunks**.

*Rejected:* content-hash chunk IDs (enables deduplication but needs reference counting before any delete can be safe; out of scope).

## 3.2 Conceptual entities

These are logical records, not table definitions.

**Object**

| Field | Notes |
| --- | --- |
| `object_id` | |
| `name` | Original filename; **not unique** |
| `size` | Total bytes |
| `content_type` | From upload; default `application/octet-stream` |
| `created_at`, `modified_at` | Controller's clock; `modified_at` changes on metadata PATCH |
| `checksum` | SHA-256 of the whole object, computed while ingesting |
| `num_chunks` | |
| `status` | UPLOADING, READY, DELETING (§3.3) |
| `last_activity` | Used by the janitor to spot dead uploads |
| `custom_metadata` | Key-value map (limits in §5.7) |

**Chunk** (one per `idx` per object): `chunk_id`, `object_id`, `idx`, `size`, `checksum` (SHA-256 of this chunk).

**Replica** (one per copy): `chunk_id`, `worker_id`, `state` (PENDING, OK, CORRUPT).

**Worker** (registry view): `worker_id`, `address`, `state` (ALIVE, DEAD, SYNCING), `last_heartbeat` (controller clock), `dead_since`, `free/used bytes`, `chunk_count`.

**Relationships:** Object 1—N Chunk; Chunk 1—N Replica (normally 2); Replica N—1 Worker.

**Effective replica** = a replica in state OK on a worker in state ALIVE. Reads use only effective replicas, and the "is this chunk safe?" question is answered by counting them.

**What the metadata endpoint returns.** System fields, custom metadata, and the chunk map (`idx`, `chunk_id`, `size`, `checksum`, and the replica list with worker and state). The chunk map is needed to *demonstrate* distribution and replication; a 1 GiB object yields about 1,024 entries (roughly 200 KB), which is acceptable.

## 3.3 State machines

**Object**

```
   (create)
      │
      ▼
  UPLOADING ──commit──▶ READY ──delete──▶ DELETING ──all chunks gone──▶ (removed)
      │                                      ▲
      └── failure / stale / controller ──────┘
          restart (abort = delete path)
```

- Only READY objects are visible to clients (GET, list, metadata, PATCH). UPLOADING and DELETING objects behave as **404**.
- The commit (UPLOADING → READY) is **one atomic update**. This is the instant an object "exists".

**Replica**

```
  PENDING ──worker acked──▶ OK ──checksum failure detected──▶ CORRUPT
     ▲                                                           │
     └────────────────── repair started ─────────────────────────┘
  (a PENDING replica whose write failed is removed; a MISSING replica is removed)
```

**Worker**

```
  (unknown) ──register──▶ ALIVE ──no heartbeat for DEAD_AFTER──▶ DEAD
                            ▲                                      │
                            └─────── heartbeat/register ───────────┘
                         (return from DEAD always triggers an inventory resync)
```

"Repair-eligible" is *derived*: a worker that has been DEAD longer than `REPAIR_GRACE` makes its replicas count as lost for repair purposes.

## 3.4 Invariants (the rules that must always hold)

These are your correctness contract. We will build an **audit** that checks them (§5.9) and use them as test assertions (§10, M6 onward).

| # | Invariant |
| --- | --- |
| I1 | A READY object has chunk rows for every `idx` from 0 to `num_chunks − 1`, with no gaps or duplicates |
| I2 | The sum of chunk sizes equals the object size |
| I3 | Two OK/PENDING replicas of the same chunk are never on the same worker |
| I4 | After the janitor has run, no chunk file on any worker is unclaimed by a replica row |
| I5 | In steady state (no failures in progress, repair complete), every chunk of every READY object has exactly RF effective replicas |
| I6 | The SHA-256 of the bytes returned by a download equals the object's stored checksum |
| I7 | A non-READY object is never returned or listed |

## 3.5 Where state lives

| State | Location | Survives controller restart? | Notes |
| --- | --- | --- | --- |
| Objects, chunks, replicas | Controller DB on a persistent volume | Yes | **D7:** embedded transactional DB. Atomic multi-row commit; SQL queries ("chunks with fewer than 2 effective replicas"); no extra container. Rejected: Postgres (more moving parts than the project needs), plain KV store (you would hand-build the queries), in-memory with snapshots (loses data on crash). |
| Worker registry (liveness) | Controller memory | No, **rebuilt** from heartbeats | Cheap to rebuild; stale liveness is worse than none |
| Chunk bytes + local checksum | Worker volume | Yes | |
| In-flight upload state (running hash, window, errors) | Handler memory | No | Dies with the request; the DB row says UPLOADING and recovery cleans it |

**DB concurrency policy.** Many requests and three background loops touch the DB. Rule: **short transactions, never hold a transaction open across a network call to a worker.** Do the network work first (or between transactions), and commit state changes in small atomic steps.

## 3.6 Integrity architecture: layered checksums (D15)

| Layer | What is checked | By whom | When |
| --- | --- | --- | --- |
| 1 | Chunk bytes against expected chunk checksum | **Worker** | On every PUT (rejects transfer corruption) |
| 2 | Chunk bytes against checksum stored with the chunk | **Worker scrubber** | Periodically, slowly |
| 3 | Chunk bytes against the controller's checksum | **Controller** | Every download read and every repair copy |
| 4 | Whole-object SHA-256 | **Client** (optional) | Compare with metadata after download |

**The authority rule:** the controller's metadata checksum is the truth. A worker-side checksum is a convenience so a worker can self-check without asking anyone. If a worker reports a mismatch, the controller **cross-checks** against its own record before declaring a replica CORRUPT.

**Why SHA-256:** collision-resistant, standard, resume-friendly, and fast enough here (disk and network will dominate). CRC32C would be faster but is only error-detecting, not tamper-evident. We will mention it as an option.

**Why both chunk-level and object-level:** chunk checksums localize damage so one bad chunk is repaired without touching the rest. The object checksum catches *assembly* bugs (missing, duplicated, or reordered chunks) that per-chunk checks cannot.

---

# L4 — FLOWS (step by step)

## 4.1 Upload

```
 Client            Controller                             Workers (A, B chosen per chunk)
   │ POST multipart    │                                         │
   │──────────────────▶│ 1 validate; need ≥2 ALIVE workers else 503
   │                   │ 2 create object row: UPLOADING
   │                   │ 3 read next ≤1 MiB from the request stream
   │                   │   (update whole-object hash; compute chunk hash)
   │                   │ 4 placement → workers A,B (distinct, ALIVE)
   │                   │ 5 WRITE-AHEAD: record chunk row + 2 replica rows = PENDING
   │                   │ 6 send chunk to A and B in parallel ───────▶│ verify checksum,
   │                   │                                             │ temp file → rename,
   │                   │◀──────────────────── ack from both ─────────│ fsync, ack
   │                   │ 7 mark both replicas OK
   │                   │   (repeat 3–7 with a bounded window of chunks in flight)
   │                   │ 8 end of stream: wait for all chunks
   │                   │ 9 COMMIT (one transaction): size, num_chunks,
   │                   │   object checksum, custom metadata, status = READY
   │◀── 201 + info ────│
```

**Step-by-step reasoning**

- **Step 1 — admission.** Refuse early. Accepting a 1 GiB upload we cannot replicate wastes everyone's time. Fewer than 2 ALIVE workers means **503**.
- **Step 2 — UPLOADING row first (D9).** The controller can now tell "chunk of an in-progress upload" from "orphan of a dead upload", and visibility is controlled by `status`.
- **Step 3 — streaming.** Never buffer the whole object. The amount in memory is bounded by (window size × chunk size) per upload (D19).
- **Step 5 — claim before write (D10).** Rows exist *before* the bytes. A crash can leave "claimed but file missing" (detected by inventory comparison) but never "file present with no claim" except in the microscopic window between a worker's rename and the controller crash. The janitor sweep covers that window. Rule of thumb: **claim, act, confirm**.
- **Step 6 — synchronous, all-or-nothing replication (D4).** A chunk counts as stored only when **both** replicas have acked. If worker A fails mid-write, the coordinator picks a different ALIVE worker not already used for this chunk, replaces that PENDING row, and retries (bounded; §5.5). If two distinct workers can't be reached, the upload fails.
  - *Rejected:* "ack after one copy, replicate in the background." Faster, but the system would acknowledge writes it cannot yet survive the loss of.
- **Step 9 — commit.** All visibility flips in one atomic update. Before it: invisible. After it: complete.
- **Failure at any point:** run the delete path (§4.4) on this object. Client disconnects, worker failures, and controller crashes all converge on it.

**Parallelism in uploads.** Within one chunk, the two replica writes are always parallel. Across chunks, the input is a sequential byte stream, so the coordinator reads chunk *i+1* while chunk *i* is still being sent, up to a bounded window (e.g. 4). When the window is full it stops reading from the client, which is **backpressure**. Build sequential first, add the window later, and benchmark the difference (§10).

## 4.2 Download

```
 Client            Controller                                Workers
   │ GET object        │ 1 look up object; must be READY else 404
   │──────────────────▶│ 2 load chunk map + replica states
   │                   │ 3 PRE-FLIGHT: every chunk has ≥1 effective replica? else 503
   │◀── 200 + headers ─│ 4 send headers (size, type, checksum)
   │                   │ 5 for chunk idx = 0..n-1 (prefetch window W):
   │                   │     pick a random effective replica ──────▶│ return bytes
   │                   │     verify size + SHA-256
   │                   │     mismatch → mark replica CORRUPT, try other replica
   │                   │     unreachable → try other replica
   │◀── bytes, in order│ 6 write chunk idx to the response
```

- **Ordering (the common doubt).** Chunks may be *fetched* in parallel but are *written* strictly by `idx`. If chunk 3 arrives before chunk 2, it waits in memory. The window bounds that waiting memory. Order comes from metadata, never from arrival time.
- **Pre-flight (step 3).** The cheap metadata check catches "no copy available anywhere" *before* we commit to a 200, so the client gets a proper 503 instead of a broken download.
- **If a failure happens mid-stream** (e.g. replica A dies, replica B corrupt): we have already sent 200 and Content-Length. We cannot change the status, so we **abort the connection**. The client sees a truncated transfer (length mismatch), never silently wrong bytes. Log it loudly.
- **Verify before sending** each chunk. Corrupted data must never reach the client when a valid copy exists.
- **Replica choice:** random among effective replicas spreads read load. Failure moves to the next candidate within the same request.
- **Sequential first, parallel prefetch later.** Both are correct; the window is an optimization to measure.

## 4.3 Metadata operations

- **GET metadata:** read-only. Returns system fields, custom metadata, and the chunk map.
- **PATCH metadata:** JSON object merged into custom metadata (add or update keys; setting a key to `null` removes it, if you adopt that convention; see §9). It updates `modified_at` and **never touches chunks or workers**.
- **List:** READY objects only, ordered by creation time then ID, with `limit` and an opaque `cursor` (keyset pagination: "continue after this item", which stays fast and stable under inserts).
- All three are single-transaction DB operations. No worker is contacted.

## 4.4 Delete (and abort)

```
 1 transaction: status ← DELETING          (object instantly disappears: 404 for everyone)
 2 respond 204   (the delete is now guaranteed to finish eventually)
 3 try to delete each replica's chunk file on its worker
 4 when every replica is confirmed gone → remove the rows
   (if any worker is unreachable, leave DELETING; the janitor retries until done)
```

**Why logical first (D18).** A worker may be dead when you delete. If delete required all workers to answer, one dead worker would block every delete. With a DELETING marker, the user-visible effect is immediate and correct, and the physical cleanup completes whenever it can. A worker that was down will later register with its inventory (§4.5) and the janitor removes any chunk that no live object claims.

**Abort = delete.** A failed or stale UPLOADING object is moved to DELETING and takes the same path. One cleanup mechanism, tested one way.

## 4.5 Worker join, heartbeat, and resync

**Startup (every time a worker process starts):**

1. Delete leftover temp files from any crash mid-write.
2. Build an **inventory**: list of (chunk ID, size) for chunks fully on disk.
3. Register with the controller (retry until it's reachable, so startup order doesn't matter) sending ID, address, and inventory.

**Controller on registration:**

1. Mark the worker in SYNCING state.
2. **Reconcile the inventory against the metadata** (D20):
3. After reconciliation is done, mark it ALIVE. (This is done to avoid the case where the worker is accessed before reconciliation is done.)

| Situation | Action |
| --- | --- |
| Chunk in inventory, matching OK replica row | Fine |
| Replica row says OK, but chunk missing from inventory (e.g. volume wiped) | Remove the row; the chunk is now under-replicated and the reconciler repairs it |
| Replica row OK but size differs from metadata | Mark CORRUPT |
| Chunk in inventory, no row claims it (object deleted, upload aborted, or repaired elsewhere and row dropped) | **Orphan**: schedule deletion |
| Chunk in inventory, but chunk already has RF effective replicas elsewhere | Over-replication: keep for now, trimmed later at low priority (safe but wasteful) |

**Heartbeats:** every `HEARTBEAT_INTERVAL`, the worker reports ID, free/used space, chunk count, and any **suspected-corrupt chunk IDs** from the scrubber. The controller replies "OK" or **"unknown/needs resync"**. If the controller restarted, or had marked this worker DEAD, it asks for re-registration so the inventory is re-synced. This is how the system self-heals its own bookkeeping.

## 4.6 Failure detection

- The controller records **its own receive time** of each heartbeat. If `now − last_heartbeat > DEAD_AFTER` (10 s ≈ three missed beats), the worker becomes **DEAD**. (D11)
- **Why the controller's clock only:** machines' clocks differ. If workers reported timestamps, a clock skew could kill a healthy worker or hide a dead one. Measuring "time since I last heard from you" on one clock is immune.
- **Effects of DEAD:** immediately excluded from placement and from read replica selection; its replicas stop being "effective".
- **Grace period (D12):** repair does **not** start until the worker has been DEAD for `REPAIR_GRACE` (30 s; shorten for demos). Reason: restarts and flaps are far more common than permanent loss. Copying gigabytes in response to a 5-second blip wastes bandwidth and can make things worse. Repair is a trade between *window of vulnerability* (time with only one copy) and *wasted work*.
- **Data-path failures** (a connection refused or timeout during an upload or download) only affect *that request*: it tries another replica or worker. **Only heartbeats change worker state.** One authority for liveness keeps behavior explainable.

## 4.7 Re-replication (self-healing)

The **reconciler** runs every `RECONCILER_INTERVAL` (5 s). It is **level-triggered** (D13): it does not react to "a worker died" events but compares the *current* state against the desired state each round.

```
 1 find chunks of READY objects whose effective-replica count < RF
   (replicas on DEAD-beyond-grace workers, CORRUPT, or removed-as-missing)
 2 sort by risk: chunks with exactly 1 effective replica first; 0 effective = cannot repair, alarm
 3 for each, up to MAX_CONCURRENT_REPAIRS at a time:
     a. source  = an effective replica (ALIVE, OK)
     b. target  = placement picks an ALIVE worker holding no replica of this chunk
                  (prefer the worker whose replica is CORRUPT, to repair in place)
     c. write-ahead: replica row on target = PENDING
     d. read chunk from source, verify SHA-256 against metadata,
        write to target (target verifies again)
     e. mark target replica OK; drop a superseded CORRUPT/lost row
 4 low priority: trim over-replication (>RF effective) once everything is safe
```

**Why each choice**

- **Level-triggered:** if the controller crashes mid-repair, or an event is missed, the next round simply finds the same gap and fixes it. No state to recover.
- **Risk priority:** a chunk with one copy is one failure from data loss; a chunk with two is fine. Fix the scary ones first.
- **Throttle:** unbounded repair would compete with client traffic and saturate the controller (it carries the copy, D14). Cap concurrency.
- **Controller-mediated copy (D14):** reuses the existing internal chunk GET/PUT, so no new worker-to-worker protocol, and the controller verifies the bytes end-to-end. Cost: repair bandwidth flows through the controller.
- **In-flight tracking:** the reconciler remembers which chunks it is currently repairing so two rounds never repair the same chunk twice.
- **Zero effective replicas:** nothing to copy from. If the holders are merely DEAD they may return, so reads return **503** (storage unavailable) and the event is logged as a data-at-risk alarm. We never fabricate data.

**Window of vulnerability (an important concept).** With RF = 2, losing *one* worker never loses data. But until repair finishes, the chunks it held have a single copy. A second failure in that window *can* lose data. Durability is therefore governed by **time to repair ≈ detection time + grace + (data on dead worker ÷ repair throughput)**. This is exactly the number your benchmark should measure.

## 4.8 Worker returns after failure

Case analysis, all handled by §4.5 reconciliation:

| Returns when | Result |
| --- | --- |
| Within `REPAIR_GRACE` | No repair had started; all its replicas become effective again. System is whole. |
| After repair completed | Its chunks are now over-replicated; excess trimmed later. Orphans (deleted objects) removed. |
| After repair partly done | Same, plus in-flight repairs finish harmlessly. |
| With an empty disk (same ID) | Rows for missing chunks are removed; the reconciler repairs them elsewhere. |

The spec's "synchronize required data when a failed node recovers" is satisfied by this inventory reconciliation plus repair.

## 4.9 Corruption detection and repair

**Detection paths**

1. **On read:** the controller's checksum check (layer 3) fails → mark that replica CORRUPT → use the other replica for this request.
2. **Scrubber:** the worker's background check (layer 2) fails → reported via heartbeat → the controller cross-checks and, if confirmed, marks CORRUPT.
3. **On inventory:** size mismatch on re-registration.

**Repair:** a CORRUPT replica counts as lost, so the reconciler repairs it through the **same path as §4.7**, with **no grace period** (corruption is certain, not transient). Placement prefers rewriting on the *same* worker: chunk writes are atomic temp-then-rename, so the new copy replaces the bad file. This avoids moving data and leaving garbage behind. (D16)

**If both replicas are corrupt:** nothing valid remains. Report the object as unavailable (503), log a data-loss event. Only an operator re-upload fixes it. This case should be demonstrable as an "honest failure" in your docs.

## 4.10 Controller restart and crash recovery

Because the controller's state is in the DB (durable) and registry (rebuilt), restart is a defined procedure:

1. Open the DB.
2. **Recovery pass:**
   - Every object in UPLOADING is dead (no handler survives a restart) → move to DELETING.
   - Finish every DELETING object.
   - Any PENDING replica rows (belonging to dead uploads or repairs) → removed; stray files are swept as orphans.
3. Start the API, failure detector, and janitor.
4. Workers' next heartbeat gets "unknown", so they re-register with inventories. The registry rebuilds.
5. **Reconciler warm-up:** start the reconciler only after `DEAD_AFTER` plus a margin. Otherwise, immediately after restart *every* worker looks absent and the reconciler would panic-repair everything. Workers that don't check in by then become DEAD with `dead_since = restart time`, then follow the normal grace period.

While the controller is down: clients get connection errors; workers keep running and keep retrying heartbeats. That is the SPOF, stated plainly.

---

# L5 — POLICIES AND CROSS-CUTTING CONCERNS

## 5.1 Consistency and concurrency

- **Metadata is strongly consistent:** one controller, one transactional DB. After a `201`, any `GET`, list, or metadata call sees the object.
- **Visibility is atomic:** objects appear/disappear in a single update.
- **Replicas never go stale** because data is immutable and writes are synchronous (D6, D4). There is no "which replica is newer?" question.
- **Concurrent uploads are independent:** distinct object IDs mean distinct chunk IDs, rows, and files. They share only the DB and the worker registry (both protected).
- **Background loops are idempotent.** Running any of them twice, or concurrently with requests, must be safe.
- **Bounded concurrency everywhere (D19):** chunks in flight per upload, prefetch per download, concurrent repairs. Memory is roughly (uploads × window + downloads × window) MiB. Make these configurable and document the math.

## 5.2 Failure model: what we handle and what we don't

| We handle | We do not handle (documented limits) |
| --- | --- |
| A worker crash-stop and restart | Controller high availability |
| One worker down at a time with no data loss | Two workers down simultaneously **before repair completes** may lose chunks held only by those two |
| Worker slow/unreachable (network partition): treated as DEAD, treated as recovery on heal | Malicious behavior (Byzantine faults), authentication |
| Silent disk corruption (bit rot) | Loss of the controller's DB volume (backup it: copying the DB file is the cheap mitigation) |
| Crashes at any point in upload or delete | Whole-host failure (everything is on one laptop) |
| Client disconnects | |

**Network partition note.** If a worker is alive but unreachable, the controller can't tell it from a dead one, and it doesn't need to. It treats it as DEAD and repairs if the grace period expires. On heal the worker re-registers like any returning worker. There is no split-brain risk *because workers never make decisions and data is immutable*.

## 5.3 Placement (D17)

**Hard rules** (all policies):

1. Only ALIVE workers.
2. The RF copies of one chunk go to **distinct** workers (I3).
3. Skip workers over a configured fullness threshold (if free space is reported).

**Policies**

| Policy | How | Pros | Cons |
| --- | --- | --- | --- |
| Random distinct | Shuffle ALIVE workers, take first RF | Trivial; fine for even load | Variance on small samples |
| Least-loaded | Pick workers with fewest chunks/bytes (known from metadata) | Even by construction | Needs tracking; hot workers get all new writes after a failure |
| **Rendezvous (HRW) hashing** | For each ALIVE worker compute hash(chunk_id, worker_id); take top RF | Deterministic; even; when the worker set changes, only the affected chunks move; great interview topic | Slightly more to explain |

**Plan:** make placement a replaceable component. Ship **random** in M3, add **rendezvous** after measuring spread, and show a "chunks per worker" table. Repair uses the same engine, constrained to "workers not already holding this chunk". Note: reads never depend on placement being reproducible; they use the recorded replica rows.

## 5.4 Timing parameters (all configurable, with defaults)

| Parameter | Default | Meaning / reasoning |
| --- | --- | --- |
| `CHUNK_SIZE` | 1 MiB | Fixed by spec |
| `REPLICATION_FACTOR` | 2 | Fixed by spec |
| `HEARTBEAT_INTERVAL` | 3 s | Frequent enough to notice quickly, cheap enough to be free |
| `DEAD_AFTER` | 10 s | About three missed beats; tolerates one lost heartbeat without false positives |
| `REPAIR_GRACE` | 30 s (demo: 10–15 s) | Avoids repair storms from blips; shorten so demos don't wait |
| `RECONCILER_INTERVAL` | 5 s | Repair latency floor |
| `MAX_CONCURRENT_REPAIRS` | 4 | Protects client traffic |
| `UPLOAD_WINDOW` | 4 chunks | Bounds upload memory; tune with benchmark |
| `DOWNLOAD_PREFETCH` | 4 chunks | Bounds download memory |
| `WORKER_CONNECT_TIMEOUT` | 2 s | Fail fast on a stopped container |
| `WORKER_REQUEST_TIMEOUT` | 10 s per chunk operation | Hung worker must not hang the request forever |
| `UPLOAD_STALE_AFTER` | 10 min | Janitor aborts uploads with no activity |
| `SCRUB_INTERVAL` | 10 min per full pass (demo: 30 s) | Slow background verification, throttled |
| `FSYNC_ON_WRITE` | on | Durability vs speed (see §5.11); benchmark both |

## 5.5 Retries and idempotency

- **Idempotent operations are the foundation of safe retries.** Chunk PUT with the same ID and bytes is a no-op or harmless overwrite. Chunk DELETE of an already-missing chunk counts as success. Inventory reconciliation is repeatable.
- **Retry policy for worker calls:** a small bounded number of attempts per target, then move to a *different* target. Never retry forever; never retry on a worker already failed within this request.
- **A worker's checksum rejection** (data corrupted in transit) is retried as a normal failure.
- **Timeouts are mandatory** on every call. A call with no timeout is a latent outage.

## 5.6 Races (and their resolutions)

| Race | Resolution |
| --- | --- |
| Delete while a download is streaming | The download may fail mid-stream (connection aborted). Accepted and documented; I6 still holds because no wrong bytes are ever sent. |
| Delete while repair is copying that chunk | The repair finishes and leaves an orphan; the janitor sweep removes it. Repair also re-checks object status before marking OK. |
| Worker returns while its chunks are being repaired elsewhere | Over-replication, trimmed later. Harmless. |
| Two uploads concurrently | Independent. |
| Worker dies during an upload | Chunk is retried on another worker; the upload continues. |
| PATCH and DELETE at the same time | Serialized by the DB; whichever commits first wins; the loser sees 404 or applies to a DELETING object (404). |
| Worker flapping (dies/returns repeatedly) | Grace period prevents repair thrash; every return triggers a cheap inventory resync. |
| Reconciler round overlapping with the previous one | In-flight repair set prevents duplicate repair of the same chunk. |

## 5.7 Edge cases and limits to decide up front

- **Empty file:** allowed; 0 chunks; checksum of empty input; download returns an empty body.
- **Exactly N × 1 MiB:** N chunks, no empty last chunk.
- **Missing `file` field / malformed multipart / invalid metadata JSON:** 400.
- **Metadata part position:** clients may send `metadata` before or after `file`; since it's small, hold it in memory and apply at commit.
- **Custom metadata limits:** e.g. at most 50 keys, key ≤ 128 B, value ≤ 1 KiB, string values. Protects the DB.
- **Maximum object size:** configurable cap (e.g. a few GiB) to avoid filling disks.
- **Filename on download:** sanitize it before putting it in headers.
- **Chunk IDs on workers:** validated against a strict character set; reject anything with `/` or `..` (path traversal).
- **Disk full on a worker:** write fails; handled like any failed write (retry elsewhere).

## 5.8 Error mapping

| Situation | Status |
| --- | --- |
| Successful read/list/patch | 200 |
| Upload committed | 201 |
| Delete accepted | 204 |
| Bad request (missing file, bad JSON, bad pagination, over limits) | 400 |
| Unknown object, or object not READY | 404 |
| Concurrent-modification conflict (if adopted; see §9) | 409 |
| Unexpected internal error | 500 |
| Fewer than 2 ALIVE workers on upload; upload failed because workers became unavailable; download with a chunk that has no effective replica | 503 |

All errors return JSON with a short, meaningful message and a stable error code.

## 5.9 Observability (D21)

The demo and the interview are both about *showing* behavior, so the system must narrate itself.

- **Structured event log**, one line per important event: `worker_dead`, `worker_alive`, `chunk_underreplicated`, `repair_started`, `repair_completed`, `repair_failed`, `corruption_detected`, `corruption_repaired`, `upload_aborted`, `orphan_deleted`. Include IDs and durations.
- **`GET /cluster`:** workers (state, heartbeat age, chunk count, bytes), chunk totals (fully replicated / under-replicated / zero-effective), repairs (in flight, done, failed), objects by status.
- **Audit** (an admin check): evaluates invariants I1–I5 against live metadata and, optionally, worker inventories, and prints violations. This is your best correctness tool. Run it after every chaos scenario.
- **`GET /healthz`** for Compose healthchecks. Optional metrics endpoint.

## 5.10 Security posture

Out of scope for features, but state it in the doc: no auth; internal APIs are reachable by anyone on the Compose network; only the controller's port is published to the host. Path-traversal safety on workers is **in** scope because it is a correctness issue. A shared-secret header for internal calls is a cheap optional hardening.

## 5.11 Durability on a worker

A worker's ack means "this chunk is durable on my disk". To mean that after power loss, the worker must flush file data and the rename to stable storage before replying (the `FSYNC_ON_WRITE` setting). Container stops do not lose the OS cache, so you won't see a difference in demos, but you *will* see a cost in benchmarks. Report both. It's a good talking point on durability vs throughput.

---

# L6 — DEPLOYMENT ARCHITECTURE

## 6.1 Topology (Docker Compose only)

```
 host ──(published port 8080)──▶ controller ──┐
                                              │  user-defined bridge network
                                              ├──▶ worker1  (volume: worker1-data)
                                              ├──▶ worker2  (volume: worker2-data)
                                              ├──▶ worker3  (volume: worker3-data)
                                              └──▶ worker4  (volume: worker4-data)
 controller volume: controller-data  (metadata DB)
 optional "tools" profile: client container with demo and benchmark scripts
```

| Service | Container name | Published port | Volume | Notes |
| --- | --- | --- | --- | --- |
| controller | `controller` | 8080 → host | `controller-data` | Healthcheck on `/healthz` |
| worker1…4 | `worker1`…`worker4` | none | `workerN-data` | Fixed identity via config; advertise `workerN` DNS name |
| client (optional) | `client` | none | none | Profile-gated; runs demo/benchmark scripts |

**Decisions (D22)**

- **Four explicit worker services, not a scaled service:** each needs its own named volume and a stable name so that `docker stop worker2` and `docker start worker2` work exactly as the spec demonstration requires.
- **Workers are not published to the host.** Reinforces that clients don't know locations. Faults are injected with `docker stop` and `docker exec` (e.g. to alter a chunk file in the volume).
- **Startup order is not relied upon.** Workers retry registration until the controller is up; the controller tolerates workers arriving in any order.
- **Restart policy:** default `no` so a deliberately stopped worker stays stopped during demos. Consider `unless-stopped` for resilience; document the effect.
- **Volume lifecycle warning:** `docker compose down` keeps volumes (data survives). `docker compose down -v` **deletes them**. It is your factory reset and also a way to lose your demo data.
- **Resource sizing:** six small processes; a few hundred MB RAM total plus a few GiB of free disk (a 1 GiB test object at RF = 2 occupies 2 GiB across volumes).

## 6.2 Configuration

All timing/size parameters in §5.4 are environment variables with documented defaults. The README must list them. Worker config also includes its ID and the controller URL.

---

# VERIFICATION

## 7. Failure scenario matrix

Each row is a promise. Each should have a scripted demo and an audit pass afterwards.

| # | Scenario | Detected by | Immediate behavior | Recovery | How to demo |
| --- | --- | --- | --- | --- | --- |
| 1 | Worker stopped | Missed heartbeats (10 s) | Marked DEAD; reads fall back to the other replica; uploads use remaining workers | After grace, reconciler restores RF=2 on remaining workers | `docker stop worker2`; download; watch `/cluster` |
| 2 | Worker restarts before grace ends | Heartbeat/register | No repair ever started | Inventory resync; nothing to do | stop, start within ~15 s |
| 3 | Worker restarts after repair | Register | Its chunks are now over-replicated | Excess trimmed; orphans cleaned | stop, wait, start |
| 4 | Worker volume wiped | Register with empty inventory | Missing replicas dropped | Repair rebuilds on others | wipe a volume, restart |
| 5 | Two workers down together | Heartbeats | Some chunks may have zero effective replicas → 503 for those objects | Return of either worker restores access | stop two workers (demonstrates the RF=2 limit honestly) |
| 6 | Only one worker alive | Registry | Uploads return 503; reads of objects with a copy there still work | Workers return | stop three workers |
| 7 | One replica corrupted | Verify-on-read or scrubber | Download still returns correct bytes | Reconciler rewrites the bad replica | append bytes to a chunk file in a worker volume, download |
| 8 | Both replicas of a chunk corrupted | Verify-on-read | Download refuses (503), never returns bad data | None automatic; operator re-uploads | corrupt both copies |
| 9 | Chunk file deleted from a volume | Scrubber / read miss / inventory | Fallback read | Repair | delete a chunk file |
| 10 | Controller crash mid-upload | Restart recovery | Object never became visible | UPLOADING → DELETING → chunks removed | `docker kill controller` mid-upload, restart |
| 11 | Controller restart (idle) | — | Brief outage | Workers re-register; reconciler warm-up | `docker restart controller` |
| 12 | Worker crash mid-chunk write | Temp file on startup; write error | Chunk retried on another worker | Temp file removed | stop a worker during a large upload |
| 13 | Client disconnects mid-upload | Broken stream | Upload aborted | Delete path | Ctrl-C a `curl` upload |
| 14 | Network partition (optional) | Missed heartbeats | Same as scenario 1 | Same as scenario 2/3 | `docker network disconnect` |

**Mapping to the spec's required demonstrations**

| Spec demo | Covered by |
| --- | --- |
| Object storage; retrieved matches original | M3–M4, invariant I6 |
| Chunking and distribution | M3, `/cluster` + metadata chunk map |
| Replication, copies' locations | M3, metadata endpoint |
| Metadata upload/modify without re-upload | M5 |
| Worker failure and restart; RF maintained? | M6–M7, scenarios 1–4 |
| Data integrity | M8, scenarios 7–9 |

---

# TRADE-OFFS, OPEN DECISIONS, INTERVIEW PREP

## 8. Trade-offs we knowingly accept

| Choice | What we gain | What it costs | What a "next version" would do |
| --- | --- | --- | --- |
| Single controller | Simplicity, strong consistency | SPOF; total outage if down | Replicated controller with Raft |
| Controller in data path | One simple API; central verification | Throughput ceiling; repair bandwidth through controller | Direct client-to-worker transfer with signed placements |
| Synchronous 2-way replication | Writes are durable at ack | Higher write latency; need ≥2 workers | Quorum writes; tunable consistency |
| RF = 2 | Spec; minimal footprint | One failure at a time; window of vulnerability | RF = 3 or erasure coding |
| Fixed 1 MiB chunks | Predictable memory/repair units | More metadata for big objects | Larger or adaptive chunk sizes |
| Immutable objects | No conflicts or stale replicas | No partial update | Versioning |
| Grace period before repair | No repair storms | Longer vulnerability after a real failure | Adaptive: shorter when more risk |
| Metadata in embedded DB | Easy, transactional | Single file, single node | Distributed metadata store |

## 9. Open decisions (you must decide; my recommendation in parentheses)

1. **Language and HTTP framework** (Go if you can spend 2–3 days learning it; otherwise Java).
2. **Metadata DB specifics** (SQLite-class embedded DB; enable write-ahead logging and a busy timeout).
3. **Worker-side checksum storage:** sidecar file vs header inside the chunk file. A header gives a single atomic rename; a sidecar is easier to inspect by hand. (Recommend: header, and add a debug tool to print it.)
4. **Meaning of 409:** reserve for conflicting concurrent modifications (e.g., optional `If-Match` on PATCH), or leave unused. (Recommend: optional `If-Match`, low priority.)
5. **PATCH delete-key convention:** `null` removes a key, or keys can never be removed. (Recommend: `null` removes.)
6. **Include scrubber in required scope?** (Yes: small, and it gives you an independent corruption detection path.)
7. **Pipelined upload in v1 or sequential first?** (Sequential first, then window; measure.)
8. **Demo timing values** (`REPAIR_GRACE` 10–15 s for demos; keep 30 s as documented default.)
9. **Maximum object size cap** (a few GiB, configurable).

## 10 → see Milestones below. First, interview prep:

## 11. Questions this design lets you answer

Answer each in your own words before the interview; each links to a section.

1. Why does the controller, not the worker, decide that an object is complete? (§3.3, §4.1)
2. What happens if the controller crashes after the last chunk is stored but before commit? (§4.10)
3. How do you know a chunk file on a worker is an orphan? (D10, §4.5)
4. Why wait 30 seconds before repairing? What is the cost? (§4.6, §4.7)
5. How is download order guaranteed when chunks are fetched in parallel? (§4.2)
6. What is the window of vulnerability, and how would you shrink it? (§4.7)
7. Why use the controller's clock for liveness? (§4.6)
8. How do you avoid returning corrupted data, and what if both copies are corrupt? (§3.6, §4.9)
9. Why are objects immutable and what does that simplify? (§1.5, §5.1)
10. What are the single points of failure and how would you remove them? (§8)
11. Why level-triggered reconciliation instead of reacting to events? (§4.7)
12. How would this change if the client uploaded directly to workers? (§2.2, §8)
13. Why synchronous replication, and what would asynchronous give up? (§4.1)
14. How would you add a fifth worker? What would not happen automatically? (§4.5; rebalancing is a non-goal)

---

# 10. MILESTONES

**How to use the milestones.** For each: first **read the decisions it depends on**, then **answer the "must be able to explain" questions in writing**, then build, then pass the **checkpoint**. After each milestone, update this document if reality changed a decision. If you cannot explain *why*, stop and ask before continuing. That is the point of this document.

## 10.0 Overview and schedule

*The dates below assume today is 8 Nov 2026, about 12–15 focused hours per week, and that the SPE capstone (proposal around 11 Nov, pipeline around 25 Nov, demo 7–12 Dec) and your GNN project run in parallel. **Replace them with your actual submission date.** If it's earlier than mid-December, compress as noted.*

| Milestone | Theme | Depends on | Suggested window |
| --- | --- | --- | --- |
| M0 | Design lock-in | — | 9–10 Nov |
| M1 | One worker as a container | M0 | 10–12 Nov |
| M2 | Cluster membership and failure detection | M1 | 12–15 Nov |
| M3 | Write path (chunk, place, replicate, commit) | M2 | 15–19 Nov |
| M4 | Read path (verify, fall back, stream) | M3 | 19–21 Nov |
| M5 | Complete API surface | M4 | 21–24 Nov |
| M6 | Degraded operation and worker resync | M5 | 24–27 Nov |
| M7 | Self-healing re-replication | M6 | 27 Nov–1 Dec |
| M8 | Data integrity | M7 | 1–4 Dec |
| M9 | Crash consistency and janitor | M8 | 4–7 Dec |
| M10 | Evaluation: benchmarks, audit, chaos | M9 | 7–12 Dec (overlaps SPE demos; do a minimum) |
| M11 | Packaging: docs, demo script, recording, resume bullets | M10 | 12–15 Dec |
| S1–S5 | Stretch | M9 | Only if time allows |

**If you must compress:** merge M0+M1, merge M6 into M7, and move M9's janitor sweep and the audit to a minimal version. Do **not** cut M7, M8, or the failure demo; they are the project's value.

---

## M0 — Design lock-in

**Goal:** you can explain the whole system on a whiteboard in five minutes.

**Decisions to settle:** everything in §9 (language, DB, etc.). Confirm the timing parameters in §5.4.

**Work:**
- Read this document end to end once, then re-read L1 and L3 and redraw §1.4 and §3.3 from memory.
- Create the repository; add this document under `docs/`.
- Pick the language; do the language tour in parallel with M1.
- Draft the API contract as a table (client endpoints in the spec, internal endpoints in the spec, plus `/healthz` and `/cluster`).

**Must be able to explain:** why one brain, many hands (§1.5); what immutability buys; why there's an UPLOADING state.

**Checkpoint:** a written one-page "decisions made" note resolving each item in §9.

---

## M1 — One worker as a container

**Goal:** a standalone, durable chunk store reachable over HTTP, running in Docker.

**Why first:** it's the simplest component and everything depends on it. It also forces you to confront durability and atomicity early.

**Decisions it uses:** chunk ID format (§3.1), atomic write and local checksum (§2.3, §3.6), idempotent PUT/DELETE (§5.5), path-traversal safety (§5.7), worker config (§5.4, §6.2).

**Scope:**
- Chunk API: store, fetch, delete, list inventory, health.
- Temp-then-rename writes; verify expected checksum on write; store checksum with the chunk; startup cleanup of temp files; inventory build.
- Container image and a named volume.

**Must be able to explain:** why rename makes a write atomic; why PUT must be idempotent; what could go wrong if the chunk ID contained `../`.

**Checkpoint:**
- Store, fetch, and compare a file's bytes.
- Stop and start the container; the chunk is still there (persistence proven).
- Kill the process during a write; no half-chunk is visible after restart.
- A PUT with a wrong expected checksum is rejected.

---

## M2 — Cluster membership and failure detection

**Goal:** the controller always knows which workers are alive, and you can see it.

**Why now:** every later feature (placement, reads, repair) depends on trustworthy liveness.

**Decisions it uses:** D11, D20, §4.5, §4.6, controller-clock-only liveness, `HEARTBEAT_INTERVAL`, `DEAD_AFTER`.

**Scope:**
- Controller skeleton with a worker registry (in memory).
- Worker registration (with inventory) and periodic heartbeats; "unknown, please re-register" response.
- Failure detector loop (ALIVE ↔ DEAD).
- `/healthz` and `/cluster` (workers only for now).
- Compose file with one controller and four workers, named volumes, one published port.

**Must be able to explain:** why use controller time; why registration must retry; what happens to workers when the controller restarts.

**Checkpoint:**
- `/cluster` shows four ALIVE workers.
- `docker stop worker2` → DEAD within about 10 s; `docker start worker2` → ALIVE again.
- `docker restart controller` → workers re-register without being touched.

---

## M3 — Write path

**Goal:** upload any object; it's chunked, replicated twice on distinct workers, and committed atomically.

**Why now:** this is the core feature and the source of most architectural decisions.

**Decisions it uses:** D2–D6, D9, D10, D17 (random), §3.2, §4.1.

**Build in two steps:**
- **M3a (sequential):** chunks processed one at a time; replicas of each chunk written in parallel.
- **M3b (bounded window):** read ahead with a fixed number of chunks in flight and backpressure.

**Scope:**
- Metadata DB: objects, chunks, replicas (§3.2); object state UPLOADING → READY.
- Chunker, whole-object and per-chunk SHA-256, placement (random distinct ALIVE), write-ahead replica rows, retry on a different worker, commit.
- Failure of any chunk aborts the upload via the delete path (a minimal version is fine now).
- Upload response with object info.

**Must be able to explain:** why claim before write; why commit is a single update; what the client sees if a worker dies mid-upload; why the memory use is bounded.

**Checkpoint:**
- Upload a ~5.5 MiB file: 6 chunks, 12 replica rows, each chunk on two distinct workers.
- The files appear on worker volumes under the expected names.
- Stop one worker mid-upload: upload still succeeds on the others (≥ 2 alive).
- Stop workers until only one remains: upload → 503, and no object or chunks remain.
- (M3b) a benchmark shows pipelined ≥ sequential throughput for large objects.

---

## M4 — Read path

**Goal:** download returns exactly the original bytes, in order, even with a worker down.

**Decisions it uses:** §4.2, D15, §3.6 layer 3, replica choice, pre-flight, abort-on-midstream failure.

**Scope:**
- Download by object ID, headers, in-order streaming, per-chunk verification, replica fallback, pre-flight 503, 404 for unknown/non-READY.
- Mark a replica CORRUPT on a verification failure (repair comes in M7/M8).
- Start with sequential fetch; add prefetch window later (S2).

**Must be able to explain:** how order is preserved; why verify before sending; why the connection is aborted if failure happens mid-stream.

**Checkpoint:**
- Upload, download, compare checksums: identical (invariant I6).
- Stop a worker holding chunks; the download still matches.
- Truncate or alter a chunk on one worker by hand; the download still matches (bad copy skipped) and the replica is marked CORRUPT.

---

## M5 — Complete API surface

**Goal:** every endpoint in the spec works with correct statuses and JSON errors.

**Decisions it uses:** §4.3, §4.4, §5.7, §5.8, D18.

**Scope:**
- Metadata GET (with chunk map) and PATCH; list with pagination; delete (logical, then physical, with janitor retry).
- Validation and limits; error mapping table implemented; consistent JSON error bodies.
- A conformance checklist generated from the spec's tables.

**Must be able to explain:** why delete returns 204 before physical removal is complete; what 409 means in your system; how pagination stays stable.

**Checkpoint:**
- Run the checklist: every row of the spec's endpoint and status tables produces the expected result.
- PATCH never changes chunks (verify by comparing replica rows before and after).
- Delete: object vanishes instantly; chunk files vanish shortly after; delete with a worker down still returns 204 and completes when the worker returns.

---

## M6 — Degraded operation and worker resync

**Goal:** the system behaves correctly *during* a failure and when a worker comes back, even before repair exists.

**Decisions it uses:** §4.5 reconciliation table, §4.8, §5.2, D12 (grace concept), invariants.

**Scope:**
- Placement and read selection already use only ALIVE workers (verify thoroughly).
- Full inventory reconciliation on register and on return from DEAD (missing, size mismatch, orphans, over-replication).
- `/cluster` shows chunk totals and under-replicated counts.
- **Audit v1:** checks I1, I2, I3, I5, I7 against metadata.

**Must be able to explain:** the difference between DEAD and repair-eligible; what each row of the reconciliation table does and why.

**Checkpoint:**
- Stop a worker: uploads and downloads keep working; `/cluster` shows under-replicated chunks growing as expected.
- Restart it: counts return to normal with no data movement (inside grace).
- Wipe a worker's volume and restart it: missing replicas are detected and dropped.
- Audit passes after each scenario.

---

## M7 — Self-healing re-replication

**Goal:** after a worker is gone beyond the grace period, full RF is restored automatically.

**Why this is the centerpiece:** it's the behavior that distinguishes "backups" from a self-healing system and gives you your best resume number and interview story.

**Decisions it uses:** D13, D14, §4.7, window of vulnerability, risk priority, throttle, in-flight set.

**Scope:**
- Reconciler loop (level-triggered): find, prioritize, copy with verification, update rows.
- Throttling and in-flight tracking; over-replication trimming (low priority).
- Events in the log; counters in `/cluster`; a "zero effective replicas" alarm.

**Must be able to explain:** why level-triggered; why prioritize one-copy chunks; what bounds the vulnerability window; what happens if the controller dies mid-repair.

**Checkpoint (the main demo):**
1. Upload several large objects; audit shows RF=2 everywhere.
2. `docker stop worker2`; downloads still match.
3. After grace, `/cluster` shows under-replicated count falling to zero on the three remaining workers.
4. Audit passes. `docker start worker2` → resync and trimming. Audit passes.
5. Measure: detection time, time to full RF (record for M10).

---

## M8 — Data integrity

**Goal:** corruption is detected on every path, never returned, and automatically repaired.

**Decisions it uses:** D15, D16, §3.6, §4.9, the cross-check authority rule.

**Scope:**
- Worker scrubber (throttled) reporting suspects via heartbeat; controller cross-check.
- Verify-on-read marks CORRUPT (from M4); inventory size mismatch marks CORRUPT.
- Repair of CORRUPT through the M7 path, no grace, preferring in-place rewrite.
- The "both replicas corrupt" behavior (503, loud log).

**Must be able to explain:** why the controller's checksum is the authority; why the same repair path is used; what is and isn't detectable.

**Checkpoint:**
- Append bytes to a chunk file in a worker volume: a download still returns correct bytes; the replica is marked CORRUPT and then repaired; the file on disk is valid again.
- Delete a chunk file: detected and repaired.
- Corrupt both replicas: the download is refused, nothing wrong is returned, the log says why.
- Scrubber finds corruption without any client read.

---

## M9 — Crash consistency and janitor

**Goal:** the system is clean after a crash at any point.

**Decisions it uses:** D9, D10, §4.4, §4.10, §5.6 races.

**Scope:**
- Janitor loop: stale UPLOADING objects, unfinished DELETING objects, orphan sweep from worker inventories.
- Controller startup recovery pass and reconciler warm-up.
- **Audit v2:** adds I4 (orphans) via inventories.

**Must be able to explain:** every crash point in upload and delete, and what each leaves behind and who cleans it.

**Checkpoint:**
- `docker kill controller` mid-upload, restart: no visible object, no leftover chunk files after the janitor runs.
- Kill a worker mid-chunk-write: no half-chunk survives its restart.
- Ctrl-C a client upload: cleaned up.
- Delete an object while a worker is down, bring the worker back: its stale chunks are removed.
- Audit (v2) passes.

---

## M10 — Evaluation

**Goal:** turn behavior into numbers and prove correctness with systematic tests.

**Scope:**
- **Benchmarks** (≥ 10 runs per data point; report medians and p99):
  - Upload and download throughput for 1 MiB, 100 MiB, 1 GiB; 1, 4, 16 concurrent clients.
  - Sequential vs windowed upload; `FSYNC_ON_WRITE` on vs off.
  - Read latency with a worker down vs healthy.
  - Detection time, time to full RF vs data size on the dead worker.
  - Scrubber detection time.
- **Chaos script:** stop/start random workers during load; audit afterward.
- **Invariant-based tests** (I1–I7) plus the scenarios in §7.

**Checkpoint:** a results table and the numbers for your resume bullets, all backed by the scripts that produced them.

---

## M11 — Packaging

**Goal:** a stranger can build, run, and be impressed in ten minutes.

**Scope:**
- README: prerequisites, build, start, curl examples, configuration table, the full demo script, how to reset.
- Architecture diagram and a trimmed version of this document, with limitations and trade-offs.
- One-command demo covering every row of the spec's required demonstrations.
- Screen recording; an optional small dashboard page over `/cluster`.
- Resume bullets using only measured numbers.

**Checkpoint:** on a clean machine (or fresh clone plus `docker compose down -v`), the README alone gets you through the full demo.

---

## Stretch milestones (in order of value per effort)

| ID | Item | Why | Rough effort |
| --- | --- | --- | --- |
| S1 | Rendezvous-hash placement (D17) | Deterministic, minimal movement; strong interview topic | Small |
| S2 | Parallel download with prefetch window | Throughput; ordering story | Small–medium |
| S3 | HTTP Range reads | Chunk arithmetic; practical feature | Small |
| S4 | Metrics endpoint + one-page dashboard | Demoability for non-technical reviewers | Small–medium |
| S5 | Zone labels on workers; replicas in different zones | Real-world placement thinking | Medium |

Out of scope even as stretch unless you finish everything with time to spare: a replicated controller (Raft), erasure coding, automatic rebalancing.

---

# APPENDIX — Glossary

| Term | Meaning |
| --- | --- |
| **Object** | A stored file (any bytes) plus its metadata |
| **Chunk** | A fixed-size (1 MiB) piece of an object |
| **Replica** | One stored copy of a chunk on one worker |
| **RF** | Replication factor: the number of copies of each chunk (2) |
| **Controller** | The coordinator holding the metadata and making all decisions |
| **Worker** | A storage node holding chunk files |
| **Heartbeat** | A periodic "I'm alive" message from a worker |
| **Effective replica** | An OK replica on an ALIVE worker |
| **Under-replicated** | A chunk with fewer than RF effective replicas |
| **Reconciler** | Background loop that restores desired state |
| **Level-triggered** | Acts by comparing current state to desired state, not by reacting to events |
| **Write-ahead intent** | Recording what you're about to do before doing it |
| **Orphan** | A chunk file on a worker that no metadata claims |
| **Scrubber** | Background process that re-verifies stored data |
| **Backpressure** | Slowing the input when downstream work can't keep up |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **Window of vulnerability** | Time during which a chunk has fewer copies than desired |
| **SPOF** | Single point of failure |
| **Audit** | A check that the system's state satisfies its invariants |
