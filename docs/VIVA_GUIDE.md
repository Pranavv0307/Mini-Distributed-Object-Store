# Viva Guide

Every feature and failure scenario mapped to the test that proves it and a
command that shows it. For a narrated presentation to someone new, use
[`DEMO_GUIDE.md`](DEMO_GUIDE.md). Use a prefixed Compose setup if the host
already has containers named `controller` or `worker1`–`worker4`:

```bash
export OBJSTORE_CONTAINER_PREFIX=objstore-
export CONTROLLER_PORT=18080
docker compose down -v 2>/dev/null || true
./scripts/demo.sh
```

The demo runs the specification's six required demonstrations against the
real stack in about two minutes: storage and byte comparison, chunking,
replication (files on two volumes), metadata PATCH, worker failure with
re-replication and return, and corruption (read path, scrubber, both copies).
It does not delete volumes; `docker compose down -v` does.

## Feature-to-demonstration map

| Feature | Ready-made test | Viva command / observation |
| --- | --- | --- |
| Four workers register | `TestComposeClusterMembership` | `curl $URL/cluster`; show 4 `ALIVE`, epochs, and capacity |
| Durable worker storage | `TestWorkerContainer` | Upload a chunk, `docker compose down`, `up`; fetch it again |
| Chunking and RF=2 | `TestComposeUpload` | Upload 5.5 MB; inspect `/metadata`; show 6 chunks and two worker IDs per chunk |
| Verified download | `TestComposeDownload` | Download and run `cmp original downloaded` |
| Metadata PATCH / list | `TestComposeObjectAPI` | `GET metadata`, `PATCH`, `GET objects?limit=10`; show `ETag` changes |
| Delete and janitor | `TestComposeObjectAPI` | `DELETE`; show 204, then 404 and `object_purged` in logs |
| `objctl` client | `TestObjctl` / `TestComposeObjectAPI` | `docker compose run --rm client list`, `stat`, `download`, `delete` |
| Worker failure / fallback | `TestComposeDownload` | Stop a chunk holder; download remains byte-identical |
| Degraded cluster | `TestComposeResync` | Stop one worker; `/cluster` shows under-replication but audit safety remains true |
| Self-healing | `TestComposeSelfHealing` | Stop worker beyond grace; show repair counters return to full RF |
| Corrupt one copy (read path) | `TestComposeDownload` | Append bytes to one holder; download remains correct; show `CORRUPT`, repair, and valid file |
| Corrupt one copy (scrubber, no read) | `TestComposeIntegrity` | Overwrite bytes in place; within `SCRUB_INTERVAL` the controller logs `"detected_by":"scrubber"`; repaired once |
| Chunk file deleted from a volume | `TestComposeIntegrity` | `rm` one chunk file; the inventory sweep logs `"detected_by":"inventory_sweep"`; the file is rewritten |
| Both copies corrupt | `TestComposeDownload` | Corrupt both holders of chunk 0; expect clean `503 STORAGE_UNAVAILABLE` |
| Wiped worker volume | `TestComposeResync` | Empty `/data/chunks`; restart worker; show missing rows, then repair |
| Upload with insufficient workers | `TestComposeUpload` | Stop 3 workers; upload returns `503 INSUFFICIENT_WORKERS` |
| Controller restart | `TestComposeClusterMembership` | `docker compose restart controller`; workers re-register with new epochs |
| Crash-mid-write atomicity | `TestWorkerKilledMidWriteLeavesNoPartialChunk` | `go test -run TestWorkerKilledMidWriteLeavesNoPartialChunk ./test/integration`; no committed partial file |
| Controller crash recovery | `TestComposeControllerKilledMidUpload` | Kill controller during upload, restart; `startup_recovery` logged; nothing listed, no files left |
| Invariants under mixed load | `TestInvariantsUnderChurn` | `go test -run TestInvariantsUnderChurn -v ./test/integration` |

## Failure demonstrations

Start the stack with short intervals so background detection is visible
during a viva (exported in the shell, so no `.env` file is created or
overwritten):

```bash
export SCRUB_INTERVAL=5s INVENTORY_SWEEP_INTERVAL=5s HEARTBEAT_INTERVAL=1s \
       DEAD_AFTER=4s REPAIR_GRACE=6s RECONCILER_INTERVAL=1s JANITOR_INTERVAL=1s
docker compose up -d --build
URL=http://127.0.0.1:${CONTROLLER_PORT:-8080}
head -c 5500000 /dev/urandom > /tmp/demo.bin
ID=$(curl -s -F file=@/tmp/demo.bin "$URL/api/v1/objects" | sed -n 's/.*"object_id":"\([^"]*\)".*/\1/p')
curl -s "$URL/api/v1/objects/$ID/metadata"
```

Pick a holder of chunk 0 from the metadata (below: `worker3`; with
`OBJSTORE_CONTAINER_PREFIX` set, prefix the container name).

**Scrubber, no client read.** Overwrite 4 bytes in place (same size: only a
checksum can tell), then watch, without downloading:

```bash
C0=${ID}_000000
docker exec worker3 sh -c "printf ZZZZ | dd of=/data/chunks/$C0 bs=1 seek=1000 conv=notrunc 2>/dev/null"
docker compose logs -f controller | grep -E 'corruption_detected|repair_completed'
```

Expected within about one scrub interval: `corruption_detected` with
`"detected_by":"scrubber"`, then one `repair_completed`; `/metadata` shows the
copy `OK` again (rewritten in place, no `REPAIR_GRACE` wait). It is repaired
once: later heartbeats do not report it again.

**Corruption on the read path.** Append bytes to a copy and download:

```bash
docker exec worker3 sh -c "printf CORRUPTION >> /data/chunks/$C0"
curl -s "$URL/api/v1/objects/$ID" -o /tmp/recovered.bin && cmp /tmp/demo.bin /tmp/recovered.bin && echo IDENTICAL
```

**Deleted chunk file.** `docker exec worker3 rm /data/chunks/$C0`; the next
sweep logs `"detected_by":"inventory_sweep"` and repair rewrites the file.

**Both copies corrupt.** Append bytes to chunk 0 on every worker that has it:

```bash
for w in worker1 worker2 worker3 worker4; do
  docker exec "$w" sh -c "test -f /data/chunks/$C0 && printf BAD >> /data/chunks/$C0 || true"
done
curl -s -w '\nHTTP %{http_code}\n' "$URL/api/v1/objects/$ID"
```

Expected: HTTP 503 `STORAGE_UNAVAILABLE`, no incorrect bytes, and a
`data_unrecoverable` controller log. Only a new upload fixes the object.

**Insufficient workers.**

```bash
docker compose stop worker2 worker3 worker4
curl -s -w '\nHTTP %{http_code}\n' -F file=@/tmp/demo.bin "$URL/api/v1/objects"
docker compose start worker2 worker3 worker4
```

Expected: HTTP 503 `INSUFFICIENT_WORKERS`; no READY object is created.

**Controller crash mid-upload.** Start a large upload in the background,
kill the controller while it streams, restart it:

```bash
head -c 300000000 /dev/urandom > /tmp/large.bin
curl -s -F file=@/tmp/large.bin "$URL/api/v1/objects" & sleep 2
docker compose kill -s SIGKILL controller
docker compose start controller
docker compose logs controller | grep -E 'startup_recovery|object_purged'
curl -s "$URL/cluster"
```

Expected: `startup_recovery` in the log; within a few janitor rounds
`objects_by_status` is empty, `pending_deletes` is 0, and no chunk file of
the upload remains on any volume. The interrupted upload is never listed.

## Useful viva evidence

```bash
curl -s "$URL/cluster" | python3 -m json.tool
curl -s "$URL/admin/audit" | python3 -m json.tool
docker compose logs --no-log-prefix controller | grep -E 'worker_dead|inventory_reconciled|repair_completed|object_purged|data_unrecoverable'
docker compose exec worker1 sh -c 'ls -lh /data/chunks | head'
```

Explain the safety story in one sentence: the controller verifies every chunk
against metadata before sending it, refuses a missing valid replica with 503,
and repairs a known-bad copy only after a verified source is available.

## Cleanup

```bash
docker compose down -v      # deletes this project's volumes
unset SCRUB_INTERVAL INVENTORY_SWEEP_INTERVAL HEARTBEAT_INTERVAL DEAD_AFTER REPAIR_GRACE RECONCILER_INTERVAL JANITOR_INTERVAL
rm -f /tmp/demo.bin /tmp/large.bin /tmp/recovered.bin
```
