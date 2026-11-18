# Phase 10 — Evaluation

The evaluation commands are intentionally small and reproducible. They report
raw `go test` benchmark output rather than inventing hardware-independent
throughput numbers.

## Local benchmark

```bash
./scripts/phase10-benchmark.sh
```

This runs the existing in-process upload benchmark for windows 1, 2, 4, and 8
with an 8 MB object and injected per-chunk latency. Use `-count` or
`-benchtime` in `BENCH_ARGS` for repeated measurements:

```bash
BENCH_ARGS='-count=5 -benchtime=3x' ./scripts/phase10-benchmark.sh
```

## Compose chaos smoke test

With Docker running, the following exercises upload, worker loss, repair, and
the audit endpoint. It uses the caller's Compose project and leaves the stack
running for inspection; clean it up with `docker compose down -v`.

```bash
./scripts/phase10-chaos.sh
```

The script prints the upload response, the audit before/after repair, and the
final `/cluster` view. Detection and repair times are visible in the JSON
timestamps and controller logs. For a formal report, capture the output and
record host, Go, Docker, Compose, configuration, median, and p99 values in
the results table below.

## Results table

| Scenario | Configuration | Median | p99 | Date / host |
| --- | --- | ---: | ---: | --- |
| Upload, window 1 | 8 MB, 4 workers | — | — | — |
| Upload, window 4 | 8 MB, 4 workers | — | — | — |
| Worker loss detection | Compose defaults | — | — | — |
| Full-RF repair | Compose defaults | — | — | — |
| Scrubber detection | `SCRUB_INTERVAL` configured | — | — | — |
| Fsync on vs. off | same workload | — | — | — |

Numbers depend on the host and are filled by the operator after running the
commands.
