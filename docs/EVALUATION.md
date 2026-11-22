# Phase 10 — Evaluation

Measured behaviour of the system and the commands that reproduce it. All
numbers below are from one host (listed under each table) and change with
the machine; rerun the scripts for your own.

## How to reproduce

Start the stack from this repository (prefix and port only needed if the
default container names or port 8080 are taken), then run the scripts:

```bash
export OBJSTORE_CONTAINER_PREFIX=objstore- CONTROLLER_PORT=18080
docker compose up -d --build
./scripts/phase10-bench-compose.sh     # throughput and latency (≈ 5 min)
./scripts/phase10-chaos.sh             # worker loss: detection, time to full RF (≈ 1 min)
docker compose down -v                 # deletes this project's volumes
```

For fsync off, restart the stack with `FSYNC_ON_WRITE=false docker compose up -d`
and run the benchmark again. For 1 GB objects:
`BENCH_SIZES=1000000000 BENCH_REQUESTS=3 BENCH_CLIENTS=1 BENCH_WORKER= ./scripts/phase10-bench-compose.sh`.
The in-process upload-window benchmark: `./scripts/phase10-benchmark.sh`.

`phase10-bench-compose.sh` deletes the objects it uploads. Its degraded step
and `phase10-chaos.sh` stop and restart one of this project's workers
(`worker2` by default); neither deletes volumes.

## Host

Windows 11 (Git Bash, `MINGW64_NT-10.0-26200`), 14 logical CPUs, Docker
29.9.2 (Docker Desktop, Linux VM), Compose 5.6.1; all containers and the
client on the same machine. Run on 2026-11-22. Default configuration unless
stated (1 MB chunks, RF 2, `UPLOAD_WINDOW` 4, `FSYNC_ON_WRITE=true`).

## Throughput and latency (Compose)

Median and p99 are over the requests of each row; "MB/s per request" is
object size / median time. With 4 clients the requests run concurrently, so
the aggregate throughput is roughly 4 × that column.

| Scenario | Object | Clients | Requests | Median (s) | p99 (s) | MB/s per request |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| upload | 1 MB | 1 | 10 | 0.026 | 0.056 | 38.0 |
| upload | 1 MB | 4 | 12 | 0.058 | 0.076 | 17.3 |
| download | 1 MB | 1 | 10 | 0.011 | 0.030 | 87.4 |
| download | 1 MB | 4 | 12 | 0.016 | 0.020 | 64.1 |
| upload | 10 MB | 1 | 10 | 0.143 | 0.167 | 69.9 |
| upload | 10 MB | 4 | 12 | 0.300 | 0.668 | 33.3 |
| download | 10 MB | 1 | 10 | 0.064 | 0.096 | 156.0 |
| download | 10 MB | 4 | 12 | 0.181 | 0.191 | 55.3 |
| upload | 100 MB | 1 | 10 | 1.092 | 1.248 | 91.6 |
| upload | 100 MB | 4 | 12 | 3.433 | 5.392 | 29.1 |
| download | 100 MB | 1 | 10 | 0.601 | 0.660 | 166.3 |
| download | 100 MB | 4 | 12 | 1.876 | 1.981 | 53.3 |
| upload | 1 GB | 1 | 3 | 12.843 | 13.531 | 77.9 |
| download | 1 GB | 1 | 3 | 7.071 | 7.416 | 141.4 |

Observations:

- Uploads write every byte twice (RF 2) and fsync each chunk, so they are
  slower than downloads, which read one copy.
- One client is limited by the controller's per-request pipeline (window of
  4 chunks); four clients raise the aggregate (100 MB uploads: ≈ 116 MB/s
  aggregate vs. 92 MB/s for one) until the shared disk and the controller
  saturate. Downloads aggregate at ≈ 210–260 MB/s with 4 clients.
- 1 GB behaves like 100 MB per byte: memory is bounded by the window, not by
  the object size.

### Degraded reads (worker2 stopped, before and after it is declared DEAD)

| Object | Requests | Median (s) | p99 (s) | MB/s per request |
| --- | ---: | ---: | ---: | ---: |
| 1 MB | 10 | 0.025 | 2.028 | 40.1 |
| 10 MB | 10 | 0.072 | 2.084 | 138.7 |
| 100 MB | 10 | 0.876 | 1.088 | 114.2 |

Until the failure detector marks the stopped worker DEAD, a read that picks
it as a replica waits for `WORKER_CONNECT_TIMEOUT` (2 s) and then falls back
to the other copy: that is the p99. Once DEAD, it is no longer chosen. Every
degraded download was verified byte-identical by the chaos script.

### fsync on vs. off

Same benchmark, stack restarted with `FSYNC_ON_WRITE=false`:

| Scenario | Object | Clients | fsync on: median (s) | fsync off: median (s) | Change |
| --- | --- | ---: | ---: | ---: | ---: |
| upload | 1 MB | 1 | 0.026 | 0.023 | −12% |
| upload | 1 MB | 4 | 0.058 | 0.036 | −38% |
| upload | 10 MB | 1 | 0.143 | 0.124 | −13% |
| upload | 10 MB | 4 | 0.300 | 0.290 | −3% |
| upload | 100 MB | 1 | 1.092 | 0.951 | −13% |
| upload | 100 MB | 4 | 3.433 | 3.247 | −5% |

fsync costs 3–13% of upload time here except for many small concurrent
writes (−38% without it). The Docker Desktop VM disk makes fsync cheaper
than on many physical disks, so expect a larger difference elsewhere.
Downloads do not fsync (differences there were noise: ±20% run to run).
Durability requires fsync (DEC-012: temp → fsync → rename → directory fsync); `false` exists only for this
measurement.

## Failure detection and self-healing (Compose, defaults)

`./scripts/phase10-chaos.sh` with the default timings (`HEARTBEAT_INTERVAL`
3 s, `DEAD_AFTER` 10 s, `REPAIR_GRACE` 30 s, `RECONCILER_INTERVAL` 5 s), one
5.5 MB object, `worker2` stopped:

| Measurement | Result |
| --- | ---: |
| Failure detection (stop → DEAD) | 8.5 s |
| Full RF restored (stop → audit steady) | 41.3 s |
| Repairs | 1 copy (1 MB), 0 failed |
| Download while degraded, and after the worker returned | byte-identical |
| Audit after the return | ok, steady |

Detection is `DEAD_AFTER` minus the time since the last heartbeat (7–10 s
with these settings). Time to full RF ≈ detection + `REPAIR_GRACE` + up to
one reconciler round + copy time. Earlier runs with more data: 10 s / 41 s for
6 copies of 5.5 MB objects (Phase 7, `ARCHITECTURE.md` §12).

## Scrubber and inventory sweep

From `TestComposeIntegrity` (Compose; `SCRUB_INTERVAL` 2 s,
`INVENTORY_SWEEP_INTERVAL` 2 s, heartbeat 1 s, reconciler 1 s):

| Measurement | Result |
| --- | ---: |
| Same-size corruption → detected by the scrubber and rewritten in place (no client read) | 3 s |
| Repairs for that one corruption, over two further scrub passes | exactly 1 |
| Deleted chunk file → found by the sweep and rewritten | < 5 s |

With the defaults (`SCRUB_INTERVAL` 10 min, throttled to 20 MB/s;
`INVENTORY_SWEEP_INTERVAL` 5 min), detection takes up to one interval plus
one heartbeat and one reconciler round.

## In-process upload window benchmark

`./scripts/phase10-benchmark.sh` (8 MB object, 4 in-process workers, 5 ms
simulated latency per chunk PUT, fsync off). Phase 3 verified medians
(`-count 3 -benchtime 10x`): window 1: 70.4, 2: 112.7, 4: 122.1, 8: 112.2
MB/s, i.e. windowed ≈ 1.7× sequential (DEC-036).

## Invariant tests

Correctness under failures is checked by tests rather than measured:
`TestInvariantsUnderChurn` runs concurrent uploads, PATCHes, and deletes
while a worker dies, stays dead past the grace period (repairs run), and
returns; the safety invariants hold throughout, the cluster then converges
to steady state (I4 and I5), and every surviving object downloads
byte-identical. Each scenario of the design's failure matrix is mapped to its
test in [`VIVA_GUIDE.md`](VIVA_GUIDE.md).
