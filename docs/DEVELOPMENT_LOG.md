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
