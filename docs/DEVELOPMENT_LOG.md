# Development Log

Chronological record of each phase: what was built, why it looks the way it
does, what went wrong, and what is left. Newest entries at the bottom.

---

## Phase 0 — Setup and design lock-in (2026-11-09)

### What was done

- Read the project specification (PDF) and the architecture document in full.
  Extracted requirements, components, milestones, and open questions; agreed
  the phase roadmap with the owner (Phases 0–11 = milestones M0–M11).
- Installed Go 1.27.3 via `winget install GoLang.Go` (Go was not previously
  installed on the development machine).
- Initialised a git repository on branch `main`. **No commits** (the owner
  commits after each phase review).
- Created the Go module `objstore` (`go 1.27.0` minimum) with stub binaries
  `cmd/controller`, `cmd/worker`, `cmd/objctl` (each prints "not implemented"
  and exits 2) and `internal/version` with a unit test as a toolchain smoke
  test.
- Pinned `staticcheck` as a `go.mod` tool dependency (`go tool staticcheck`).
- Added `.gitignore`, `.gitattributes` (LF line endings), `.editorconfig`,
  `.env.example` (the configuration contract from design doc §5.4 plus two new
  parameters).
- Wrote the documentation set: `README.md`, `docs/ARCHITECTURE.md`,
  `docs/IMPLEMENTATION_PLAN.md`, `docs/DECISIONS.md` (DEC-001…DEC-022),
  `docs/RUN_AND_TEST.md`, `docs/API.md` (the API contract, an M0 deliverable),
  this log, and copies of the source documents in `docs/reference/`.

### Important decisions

- **Worker `SYNCING` state (DEC-006).** Evaluated whether a worker used during
  inventory reconciliation causes races. It does: a placement after the
  inventory snapshot leads to false "missing" row removal and needless orphan
  deletion; a quick restart lets the controller write to the worker between
  the snapshot and registration; stale `OK` rows make the download pre-flight
  lie; repair accounting is overstated. Adopted a registration-driven
  `SYNCING` state with epochs plus worker-side write gating. Approved by the
  owner.
- **`pending_deletes` table (DEC-007).** Found an orphan-deletion race that
  `SYNCING` does not cover (a scheduled orphan delete destroying a chunk that a
  later repair re-claimed on the same worker), and the "timeout is not proof of
  failure" gap in upload retries. Deletion intent becomes a durable row,
  mutually exclusive with claims inside SQLite transactions. Approved by the
  owner.
- Resolved the remaining design-doc ambiguities: repair target rule (DEC-008),
  periodic inventory sweep (DEC-009), inventory endpoint location (DEC-010),
  conditional claims and commit (DEC-011), and every §9 open decision
  (DEC-012…DEC-017, DEC-020, DEC-021).
- Tooling: pure-Go SQLite driver (DEC-003) and the race detector in a Linux
  container (DEC-018), both because Windows has no C toolchain here.

### Problems encountered

- **Go missing.** Installed with winget; the MSI install took several minutes.
  Shells started before the install don't see Go on `PATH`; open a new
  terminal or use `"C:\Program Files\Go\bin\go.exe"` directly.
- **`go mod init` wrote `go 1.27.3`.** Lowered to `go 1.27.0` so any 1.27
  patch release builds the module.
- **Docker Desktop was not running.** Started it to run the race detector in
  a container.
- **The Read tool cannot render the PDF** (no poppler). Extracted it with
  `pdftotext -layout`; the text version is kept in `docs/reference/`. The
  PDF's configuration table is garbled by extraction; the intended values are
  1 controller, 4 workers, 1+ clients, 1 MB chunks, RF 2, Docker, persistent
  worker storage (consistent with spec §9).

### Tests performed

See the Phase 0 section of [`RUN_AND_TEST.md`](RUN_AND_TEST.md) and the
completion report. `go build`, `go vet`, `gofmt -l`, `go tool staticcheck`,
and `go test` passed natively on Windows (Go 1.27.3). `go test -race -count=1
./...` and `go vet ./...` passed inside `golang:1.27.3` (linux/amd64), from
both PowerShell and Git Bash. `go build -o bin/ ./cmd/...` produced the three
stub binaries, which exit with code 2 as intended.

### Known limitations / remaining work

- DEC-005 (chunk size) and DEC-013 (string-only metadata) are PROVISIONAL
  pending owner confirmation.
- No Dockerfile or Compose file yet (Phases 1–2).
- The module path `objstore` should be renamed if the repository is published
  (DEC-001).

---

## Phase 1 — Worker as a container (2026-11-09)

### What was implemented

- **`internal/ids`**: object-ID generation (UUIDv4 via `crypto/rand`), strict
  validation of object, chunk, and worker IDs, chunk-ID construction/parsing.
- **`internal/config`**: worker configuration from environment variables,
  reporting all invalid variables at once.
- **`internal/wire`**: shared JSON types, the `X-Chunk-SHA256` header name,
  stable error codes.
- **`internal/httpx`**: JSON responses and error bodies; middleware that logs
  one structured line per request and turns panics into 500s.
- **`internal/worker/store`**: the chunk store: 48-byte header format,
  temp → fsync → rename → dir-fsync writes, always-replace PUT, striped
  per-chunk locks, capped reads, idempotent delete, metadata-only inventory,
  startup temp cleanup, and an `Inspect` debug function. Platform files for
  directory fsync and disk-full detection (Linux vs. Windows).
- **`internal/worker/server`**: the five worker endpoints, write gate, bounded
  concurrent reads.
- **`cmd/worker`**: configuration, graceful shutdown on SIGINT/SIGTERM,
  `worker inspect FILE...` and `worker version` subcommands.
- **`Dockerfile`/`.dockerignore`**: one multi-stage image with all binaries
  (`golang:1.27.3-alpine` → `alpine:3.24`, non-root uid 10001), 27.6 MB.
- **Tests**: unit tests (ids, config, httpx, store, server), an integration
  test that kills the real worker process mid-write, and a Docker e2e test.

### Important decisions

DEC-023 (one Alpine image, non-root), DEC-024 (PUT always replaces),
DEC-025 (GET loads under a striped lock with bounded concurrency; read cap
`CHUNK_SIZE + 1`), DEC-026 (standalone write gate open in Phase 1),
DEC-027 (unreadable header → `CHUNK_CORRUPT`), DEC-028 (`internal/ids`).
DEC-012 now documents the exact on-disk layout.

Design points worth knowing:

- The worker **never judges payload validity** on read. It returns every byte
  after the header, so appended/truncated/flipped bytes reach the controller's
  checksum (the authority) instead of being silently trimmed.
- The inventory's `size` is file size − header, from `stat` only, so it is
  cheap and still exposes length corruption.
- Locks are held only around local file operations (rename, unlink, read into
  memory), never across network I/O.

### Problems encountered and resolutions

1. **Crash test timed out on Windows.** The test waited for the partial temp
   file to grow using `DirEntry.Info()`, whose size comes from the directory
   entry; Windows does not update it while the file is open for writing.
   Switched to `os.Stat(path)`.
2. **`gofmt` / `staticcheck` findings** in test code (map alignment, an unused
   field, an unused helper). Fixed.
3. **Git Bash path conversion.** `docker exec … ls /data/chunks` was rewritten
   to `C:/Program Files/Git/data/chunks`. Documented
   `export MSYS_NO_PATHCONV=1` in `RUN_AND_TEST.md`.
4. **Incident: collision with another Docker project (my error).** The first
   manual walkthrough used the generic container name `worker1`. A stopped
   container with that name already existed from the owner's separate Compose
   project `dos`. The `docker run` failed, but the script kept going: it
   started the `dos` worker1 container, wrote a 7-byte stray file
   `chunks/3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b_000000` into its volume
   `dos_worker1-data`, then removed the container with `docker rm -f`. The
   volume `dos_worker1-data` itself was not removed and was otherwise
   unchanged. The `worker1-data` volume that was removed was the one the
   script had just created. Remediation awaits owner approval. Prevention:
   all documented commands and tests now use `objstore-` prefixed or
   uniquely suffixed names, scripts abort if names exist, and
   `RUN_AND_TEST.md` carries a warning. The spec-mandated Compose names
   (`controller`, `worker1`…`worker4`) will collide with the `dos` project
   in Phase 2; this is raised with the owner.

### Tests performed and results

All on 2026-11-09:

- `go build`, `go vet` (default and `-tags e2e`), `gofmt -l` (empty),
  `go tool staticcheck` (default and `-tags e2e`): pass.
- `go test -count=3 ./...` natively on Windows: pass (integration crash test
  included).
- `go test -race -count=3 ./...` in `golang:1.27.3` (Linux): pass.
- `go test -tags e2e -count=1 -v ./test/e2e/...`: `TestWorkerContainer` and
  all 5 subtests pass.
- Manual `curl` walkthrough of `RUN_AND_TEST.md` §3–5 in Git Bash against a
  container: every documented output observed (201/200 PUT, matching hashes,
  400 `CHECKSUM_MISMATCH`, 400 `INVALID_CHUNK_ID` for `..%2Fescape`,
  inventory, inspect VALID → CORRUPT after appending, persistence across
  stop/start, 204/204/404 delete sequence).
- Native run of `bin/worker`: starts, serves `/healthz`, creates its
  directories.

### Known limitations

- Standalone only: no registration, heartbeats, or free-space reporting yet
  (Phase 2).
- No worker-side scrubber yet (Phase 8).
- `FSYNC_ON_WRITE` durability across power loss is implemented but cannot be
  demonstrated with container stops (architecture §5.11); the on/off cost is
  measured in Phase 10.
- The worker trusts the controller completely (no authentication, §5.10).

### Work remaining

Phase 2: controller skeleton, registry with `SYNCING`/epochs, registration
and heartbeats (the worker's write gate closes until registered), failure
detector, `/healthz`, `/cluster`, `docker-compose.yml`.
