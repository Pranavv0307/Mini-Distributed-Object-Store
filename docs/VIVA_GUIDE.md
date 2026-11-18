# Viva Guide

This is the shortest route from a fresh checkout to a convincing
demonstration. Use a prefixed Compose setup if the host already has containers
named `controller` or `worker1`–`worker4`:

```bash
export OBJSTORE_CONTAINER_PREFIX=objstore-
export CONTROLLER_PORT=18080
docker compose down -v 2>/dev/null || true
./scripts/demo.sh
```

The demo builds the image, starts the cluster, uploads a 5.5 MB object, shows
metadata and replicas, patches metadata, lists, downloads and compares bytes,
stops a worker and downloads through a surviving replica, restarts the worker,
audits invariants, and deletes the object. It does not delete volumes unless
you explicitly run `docker compose down -v`.

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
| Corrupt one copy | `TestComposeDownload` | Append bytes to one holder; download remains correct; show `CORRUPT`, repair, and valid file |
| Both copies corrupt | `TestComposeDownload` | Corrupt both holders of chunk 0; expect clean `503 STORAGE_UNAVAILABLE` |
| Wiped worker volume | `TestComposeResync` | Empty `/data/chunks`; restart worker; show missing rows, then repair |
| Upload with insufficient workers | `TestComposeUpload` | Stop 3 workers; upload returns `503 INSUFFICIENT_WORKERS` |
| Controller restart | `TestComposeClusterMembership` | `docker compose restart controller`; workers re-register with new epochs |
| Crash-mid-write atomicity | `TestWorkerKilledMidWriteLeavesNoPartialChunk` | `go test -run TestWorkerKilledMidWriteLeavesNoPartialChunk ./test/integration`; no committed partial file |
| Controller crash recovery | Manual procedure below | Kill controller during upload, restart; janitor removes invisible upload state |

## Failure demonstrations

Set a short scrubber interval before starting the stack so corruption is
visible during a viva:

```bash
cat > .env <<'EOF'
SCRUB_INTERVAL=5s
HEARTBEAT_INTERVAL=1s
DEAD_AFTER=4s
REPAIR_GRACE=6s
RECONCILER_INTERVAL=1s
EOF
docker compose up -d --build
```

First upload an object and save its ID as `ID`. Show its replica map:

```bash
URL=http://127.0.0.1:${CONTROLLER_PORT:-8080}
curl -s "$URL/api/v1/objects/$ID/metadata"
```

Find a holder of chunk 0 from that response, then append bytes to the actual
chunk file:

```bash
C0=${ID}_000000
docker compose exec worker2 sh -c "test -f /data/chunks/$C0 && printf CORRUPTION >> /data/chunks/$C0"
sleep 7
curl -s "$URL/api/v1/objects/$ID" -o /tmp/recovered.bin
docker compose logs controller | grep -E 'corruption_detected|repair_completed|scrub'
curl -s "$URL/api/v1/objects/$ID/metadata"
```

If worker2 is not a holder, repeat with the worker named in the metadata.
Expected: the download is correct, the suspect is reported by heartbeat, the
matching replica becomes `CORRUPT`, and repair rewrites it in place without
waiting for `REPAIR_GRACE`.

For both copies corrupt, append bytes to chunk 0 on every worker that has it:

```bash
for w in worker1 worker2 worker3 worker4; do
  docker compose exec "$w" sh -c "test -f /data/chunks/$C0 && printf BAD >> /data/chunks/$C0 || true"
done
curl -s -w '\nHTTP %{http_code}\n' "$URL/api/v1/objects/$ID"
```

Expected: HTTP 503 with `STORAGE_UNAVAILABLE`, no incorrect bytes, and a
`data_unrecoverable` controller log. Restore with a new upload or
`docker compose down -v && docker compose up -d --build`.

For insufficient placement:

```bash
docker compose stop worker2 worker3 worker4
curl -s -w '\nHTTP %{http_code}\n' -F 'file=@big.bin' "$URL/api/v1/objects"
docker compose start worker2 worker3 worker4
```

Expected: HTTP 503 `INSUFFICIENT_WORKERS`; no READY object is created.

For controller crash recovery, use a large upload and two terminals. Start an
upload in terminal 1, kill the controller in terminal 2 while it is running,
restart it, and inspect the cleanup:

```bash
curl -s -F 'file=@large.bin' "$URL/api/v1/objects" &
docker compose kill -s SIGKILL controller
docker compose start controller
sleep 12
curl -s "$URL/api/v1/objects?limit=100"
docker compose exec controller sh -c 'ls -la /data'
docker compose logs controller | grep -E 'startup_recovery|object_purged'
```

Expected: the interrupted upload is not listed, and its chunk files are
removed after startup recovery and the janitor pass. The deterministic worker
crash case is covered by the ready-made test above.

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
docker compose down -v
rm -f .env
```
