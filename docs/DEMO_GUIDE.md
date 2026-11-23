# Demo Guide: presenting the project to someone new

A script for showing the Mini Distributed Object Store to someone who has
never seen it: what to prepare, what to run, what to say, what to point at,
and how to answer the usual questions. The core demonstration is automated
(`scripts/demo.sh`), so nothing depends on typing long commands live.

| Format | Time | Parts |
| --- | --- | --- |
| Short | 5–7 min | 1 (pitch, 1 min) → 2 (automated demo) |
| Full | 20–25 min | 1 → 2 → 3 (by hand) → 4 (evidence) → 5 (questions) |

## 0. Prepare (10 minutes, before the audience arrives)

1. **Docker Desktop is running** (`docker version` shows a Server section).
   On Windows, use **Git Bash** for every command below.
2. **Get the code and build the image** so the demo does not wait for a build:

   ```bash
   git clone <this repository> mini-object-store && cd mini-object-store
   docker compose build
   ```

3. **Avoid name and port clashes.** If `docker ps -a` shows containers called
   `controller` or `worker1`…`worker4` from another project, or port 8080 is
   in use, set (in every terminal you will use):

   ```bash
   export OBJSTORE_CONTAINER_PREFIX=objstore- CONTROLLER_PORT=18080
   ```

   Container names then become `objstore-worker2` etc. and the API is on
   port 18080. Adjust the commands in part 3 accordingly.
4. **Start from a clean state** (deletes this project's stored data only):

   ```bash
   docker compose down -v
   ```

5. **Dry run once:** `./scripts/demo.sh` (about 2 minutes). It must end with
   `Demo complete: every required demonstration passed.` Then
   `docker compose down -v` again.
6. Arrange the screen: one large terminal (font ≥ 16 pt), and this guide or
   the README's diagram on a second screen or tab.
7. Open `http://127.0.0.1:${CONTROLLER_PORT:-8080}/dashboard/` in a browser.
   Keep it visible beside the terminal: it refreshes every two seconds and
   makes the worker states, physical chunk files, controller replica map,
   repair counters, and audit state visible while each failure is injected.

## 1. The pitch (1–2 minutes, no commands)

Open the README's **How it works** diagram and say, in your own words:

- *The problem:* one disk is too small and one machine can die. Spreading
  data over several machines solves both but raises new questions: where is
  each piece, what happens when a machine dies, how do we notice data that
  silently went bad?
- *The idea, as in S3, HDFS, GFS:* files are cut into **1 MB chunks**; every
  chunk is stored on **two different workers**. One **controller** keeps the
  map of which chunk is where (in a small SQLite database), watches the
  workers through **heartbeats**, and **repairs** anything that goes missing
  or bad. Clients only ever talk to the controller.
- *What runs here:* 1 controller and 4 workers, each worker with its own
  Docker volume (its "disk"), plus a client container, all with
  `docker compose`.
- *What you are about to see:* the six demonstrations the specification
  asks for, end to end, against the real containers.

## 2. The automated walkthrough (3–5 minutes)

Run it with pauses so you can talk at each step:

```bash
DEMO_PAUSE=1 ./scripts/demo.sh
```

Each step prints a heading, then every command it runs (`$ …` in cyan), the
output, and a green ✔ line with the result. Press Enter to start each step.
Below: what to say and what to point at.

**Step 0: start the cluster.** "Compose starts the controller and four
workers. Each worker registers with the controller and then sends a
heartbeat every second." Point at `{"total":4,"alive":4,…}`. Mention the
timings on the ✔ line: the demo shortens them (dead after 5 s, repair after
15 s) so the failure demo is quick; the real defaults are 10 s and 30 s.

**Step 1: object storage.** A 5.5 MB file is uploaded with two metadata
fields, downloaded again, and compared byte by byte. Point at
`"num_chunks":6` and `"checksum_sha256"` in the response. "The controller
checks every chunk's SHA-256 before it sends a byte, and the whole object's
checksum too."

**Step 2: chunking and distribution.** The `objctl chunks` table comes from
the client container. "Six chunks: five of exactly 1,000,000 bytes and one of
500,000. Each row lists the two workers holding that chunk." Point at the
per-worker file counts: the chunks are spread over all four workers.

**Step 3: replication.** The two `ls -l` lines show **the same chunk as a
real file on two different workers' volumes**. "1,000,048 bytes: the 1 MB of
data plus a 48-byte header holding its size and checksum, so a worker can
check its own files." The ✔ line: 12 files for 6 chunks, so exactly 2 copies
each.

**Step 4: metadata.** "The metadata lives only in the controller's
database." Version 1 shows `owner: alice, project: demo`; after the PATCH,
version 2 shows `owner: bob, reviewed: yes` and `project` is gone. "No data
was re-uploaded; the chunk placement is unchanged." Optional: "PATCH can
carry `If-Match: v2` so two people editing at once cannot overwrite each
other (409 Conflict)."

**Step 5: worker failure** (the highlight; let it breathe).

- *Before:* `"ok":true,"steady_state":true` and how many copies worker2
  holds. "The audit checks the system's invariants live."
- *`docker stop worker2`* (exactly the specification's command). After a few
  seconds worker2 is **DEAD**: point at `"under_replicated":N`. "Those
  chunks have only one copy now. The audit is still `ok` (nothing is wrong
  or lost) but not `steady_state` (the replication factor is not met)."
- *Still served:* the ✔ line says the object still downloads byte-identical.
  "Reads simply use the surviving copy."
- *Self-healing:* "The controller waits a grace period first, because most
  outages are restarts and copying data for nothing is wasteful. Then it
  copies each endangered chunk from its surviving copy to another worker."
  Point at `"fully_replicated":6` and `"under_replicated":0`, **while worker2
  is still down**.
- *`docker start worker2`*: it re-registers, the controller compares its
  files with the database, and deletes its now-redundant copies. Point at
  `"over_replicated":0` and `"steady_state":true`: "exactly two copies per
  chunk again."

**Step 6a: corruption caught on read.** "I damage one copy directly on the
worker's disk," and the four downloads are all correct. "Every chunk is
verified against the controller's checksum before it is sent; a bad copy is
skipped, marked CORRUPT, and rewritten from the good copy." Point at
`"detected_by":"read"`. (If the reads happened to use only the good copy,
the log says `"detected_by":"scrubber"` instead: the background check found
it. Both are correct behaviours; say so.)

**Step 6b: silent corruption, no download.** "Now damage that keeps the file
size the same, so only a checksum can tell, and nobody reads the file." The
`worker inspect` tool shows `status: CORRUPT`. "Each worker runs a slow
background scrubber that re-reads its files. It reports the suspect in its
heartbeat; the controller re-reads the copy and confirms it against **its
own** checksum before acting." Point at `"detected_by":"scrubber"` and
`after repair: status: VALID`.

**Step 6c: both copies corrupt.** "With no valid copy left, the honest answer
is an error, not wrong data." Point at `HTTP 503 STORAGE_UNAVAILABLE`.

**Wrap-up.** The audit is `ok` and steady; the object is deleted (204, then
404) and the janitor removes all its files from the volumes.

> If anything fails, the script prints a red ✘ with what it was waiting for
> and stops; the cluster keeps running so you can look at
> `docker compose logs controller`. Rerunning the script is safe.

## 3. By hand (optional, 5–8 minutes, default timings)

This shows the same behaviour without the script, at the real default
timings. Restart the cluster with defaults first (the demo's shortened
timings are still set otherwise):

```bash
unset HEARTBEAT_INTERVAL DEAD_AFTER REPAIR_GRACE RECONCILER_INTERVAL JANITOR_INTERVAL SCRUB_INTERVAL INVENTORY_SWEEP_INTERVAL
docker compose up -d
URL=http://localhost:${CONTROLLER_PORT:-8080}
head -c 5500000 /dev/urandom > demo.bin
curl -s -F 'metadata={"course":"distributed systems"}' -F file=@demo.bin $URL/api/v1/objects
ID=<paste the object_id>
docker compose --profile tools run --rm client chunks $ID
```

Then, in a second terminal, keep the cluster view visible:

```bash
while true; do clear; curl -s $URL/cluster | grep -oE '"(worker_id|state)":"[^"]*"|"(alive|dead|under_replicated|fully_replicated|completed)":[0-9]+' | paste -sd' '; sleep 1; done
```

In the first terminal:

```bash
docker stop worker2          # watch: DEAD after ~10 s, under_replicated > 0
curl -s -o copy.bin $URL/api/v1/objects/$ID && cmp demo.bin copy.bin && echo identical
                             # watch: ~30 s later, completed rises, under_replicated back to 0
docker start worker2         # watch: ALIVE again
curl -s $URL/admin/audit | grep -o '"ok":[a-z]*,"steady_state":[a-z]*'
```

Questions people like to try (all safe):

- *"Upload with most workers down?"* `docker compose stop worker2 worker3 worker4`,
  upload → `503 INSUFFICIENT_WORKERS` (two copies on two different workers
  are impossible, so it refuses instead of storing one copy).
  `docker compose start worker2 worker3 worker4` afterwards.
- *"Restart the controller?"* `docker compose restart controller`: workers
  re-register by themselves and all objects are still there (its database
  is on its own volume).
- *"Kill the controller during an upload?"* Upload a big file
  (`head -c 300000000 /dev/urandom > big.bin`; `curl … -F file=@big.bin … &`),
  then `docker compose kill -s SIGKILL controller` and
  `docker compose start controller`: the half-finished upload never appears
  in the list, and its chunk files are cleaned up (`startup_recovery` in
  `docker compose logs controller`).

On Windows Git Bash, prefix `docker exec … ls /data/chunks` style commands
with `MSYS_NO_PATHCONV=1` so the path is not rewritten.

## 4. Evidence: tests and measurements (2–3 minutes)

- `go test ./...` (≈ 1 minute): unit tests plus in-process clusters with
  fault injection, including `TestInvariantsUnderChurn` (uploads, edits and
  deletes while a worker dies and comes back; afterwards everything is
  consistent and every object downloads identical).
- `go test -tags e2e ./test/e2e/...` (≈ 4 minutes): the same scenarios
  against real Docker Compose clusters, including `docker kill` of the
  controller mid-upload.
- [`EVALUATION.md`](EVALUATION.md): measured throughput (100 MB uploads ≈ 90
  MB/s, downloads ≈ 165 MB/s per client on the evaluation laptop), failure
  detection (8.5 s) and full re-replication (41 s) with default timings,
  scrubber detection (3 s with a 2 s interval).
- [`VIVA_GUIDE.md`](VIVA_GUIDE.md): every feature and failure scenario mapped
  to the test that proves it.

## 5. Questions you will probably get

**What if the controller dies?** It is the one single point of failure, a
deliberate simplification. Nothing is lost: metadata is in SQLite on its own
volume, written durably. While it is down, clients get no answers; when it
restarts, workers re-register automatically, unfinished uploads are cleaned
up, and nothing is re-copied needlessly (it waits for workers to report back
before judging any as dead). The fix in a real system is several controllers
with consensus (Raft).

**Why two copies and not three?** The specification asks for two. Two
survive one failure at a time; the repair loop restores the second copy
quickly, so the risky window is short. The replication factor is a setting.

**Why wait before repairing?** Most outages are restarts. Copying a whole
worker's data because it rebooted for 20 seconds wastes disk and network,
and then the copies have to be deleted again. The grace period (30 s
default) trades a short window of single copies for that.

**How do you know data is not corrupt?** Checksums in four places: the
worker checks every chunk it receives; the worker's scrubber re-checks its
files in the background; the controller checks every chunk it reads, for
downloads and for repairs, against its own record, which is the authority;
and the whole-object checksum is checked before the last chunk is sent (and
by `objctl` after a download).

**What if both copies are bad?** The object is refused with 503 and a
`data_unrecoverable` event is logged. The system never returns wrong data;
only a re-upload fixes it.

**What happens to a half-finished upload?** Nothing is visible until every
chunk has two confirmed copies and a single database transaction marks the
object ready. A failed or abandoned upload is turned into a durable list of
files to delete, which the janitor works through even across restarts.

**Can two people edit metadata at once?** Updates are serialised in the
database, so none is lost; with `If-Match` a client can insist on the
version it read and get 409 Conflict otherwise.

**Why go through the controller for data?** Clients need one address and
every byte is verified in one place. The cost is a throughput ceiling; real
systems let clients talk to storage nodes directly with signed URLs.

**How do you know the whole thing is correct?** The invariants (complete
objects, sizes add up, two copies on distinct workers, no stray files, the
listing shows exactly the finished objects) are checked live by
`/admin/audit`, and the tests check them after every failure scenario.

## 6. Reset afterwards

```bash
docker compose down -v      # stops the cluster and deletes this project's data volumes
rm -f demo.bin copy.bin big.bin
```
