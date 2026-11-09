# Run and Test Guide

Practical, step-by-step instructions to set up, run, and verify the project.

> **Rule for this document:** every command here was run and its result
> observed at the phase noted. Sections for later phases are marked
> *not yet available* until those phases are implemented.

**Current state (end of Phase 0):** only the toolchain and stub binaries
exist. There is no running system yet; the sections "Running the system",
"Functional testing", and "Failure testing" become available in Phases 1–9.

---

## 1. Environment setup

### Required software

| Software | Version verified | Needed for |
| --- | --- | --- |
| Go | 1.27.3 (any 1.27.x works; `go.mod` requires ≥ 1.27.0) | Building and testing |
| Docker Engine | 29.9.2 (Docker Desktop on Windows) | Race detector on Windows (now); running the cluster (Phase 1+) |
| Docker Compose | v5.6.1 (`docker compose`, v2 plugin syntax) | Running the cluster (Phase 2+) |
| Git | 2.55 | Source control |

### Installing Go

- **Windows:** `winget install GoLang.Go` (installs to `C:\Program Files\Go`).
  **Open a new terminal afterwards**: shells started before the install don't
  have Go on `PATH`. Check with `go version`.
- **macOS:** `brew install go`. **Linux:** see <https://go.dev/doc/install>.

### Docker

Start Docker Desktop and wait until `docker info` prints server information
without errors. If it prints
`failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine`,
the engine is not running yet (see Troubleshooting).

### Dependencies

From the repository root:

```bash
go mod download
```

All dependencies are pinned in `go.mod`/`go.sum`. At Phase 0 the only
module requirements are those of the `staticcheck` tool (DEC-019).

---

## 2. Building

```bash
go build ./...
```

Expected: no output, exit code 0.

To produce binaries in `bin/` (ignored by git):

```bash
go build -o bin/ ./cmd/...
```

Expected: `bin/controller`, `bin/worker`, `bin/objctl` (`.exe` on Windows).
At Phase 0 each binary prints `<name> dev: not implemented yet (see
docs/IMPLEMENTATION_PLAN.md)` and exits with code 2.

---

## 3. Running the system

*Not yet available.* Phase 1 adds the worker container; Phase 2 adds
`docker-compose.yml` with the controller and four workers.

---

## 4. Functional testing (APIs)

*Not yet available.* Each endpoint in [`API.md`](API.md) gets verified
example requests here when its phase is complete.

---

## 5. Failure testing

*Not yet available.* Worker stop/start, controller restart, corruption, and
crash scenarios are added in Phases 2 and 6–9.

---

## 6. Automated testing and static analysis

Run from the repository root. All commands verified at Phase 0.

### Unit tests

```bash
go test ./...
```

Expected (Phase 0):

```
?   	objstore/cmd/controller	[no test files]
?   	objstore/cmd/objctl	[no test files]
?   	objstore/cmd/worker	[no test files]
ok  	objstore/internal/version	...
```

Add `-count=1` to bypass the test cache and `-v` for verbose output.

### Integration and end-to-end tests

*Not yet available* (Phase 2+). Integration tests will live in
`test/integration` and run with `go test ./...`; end-to-end tests against
Docker Compose will need `go test -tags e2e ./test/e2e/...`.

### Race detector

On Linux/macOS with a C toolchain:

```bash
go test -race ./...
```

On **Windows**, `-race` needs cgo and a C compiler, which is not
installed. Run it in the official Go container instead (DEC-018). The two
named volumes cache modules and build output between runs; the source is
mounted read-only.

PowerShell:

```powershell
docker run --rm -v "${PWD}:/src:ro" -v objstore-gomod:/go/pkg/mod -v objstore-gocache:/root/.cache/go-build -w /src golang:1.27.3 go test -race -count=1 ./...
```

Git Bash (`MSYS_NO_PATHCONV=1` stops Git Bash rewriting the `/src` paths;
`pwd -W` gives a Windows path Docker understands):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W):/src:ro" -v objstore-gomod:/go/pkg/mod -v objstore-gocache:/root/.cache/go-build -w /src golang:1.27.3 go test -race -count=1 ./...
```

Expected: the same `ok` lines as the unit tests. The first run also
downloads the `golang:1.27.3` image, which takes a few minutes.

### Static analysis

```bash
go vet ./...
gofmt -l .
go tool staticcheck ./...
```

Expected: no output from any of the three (exit code 0). `gofmt -l` lists
files that need formatting; fix them with `gofmt -w .`.

### Full pre-review check (all of the above)

Git Bash / Linux / macOS:

```bash
go build ./... && go vet ./... && test -z "$(gofmt -l .)" && go tool staticcheck ./... && go test -count=1 ./... && echo ALL-OK
```

Expected last line: `ALL-OK`. Then run the race detector as above.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `go: command not found` / `'go' is not recognized` | Go not installed, or the terminal was opened before installing | Install Go; open a new terminal; or call `"C:\Program Files\Go\bin\go.exe"` directly |
| `go: go.mod requires go >= 1.27.0` | Older Go toolchain | Install Go 1.27.x |
| `failed to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine` | Docker Desktop not running or still starting | Start Docker Desktop; wait until `docker info` succeeds. If it exits immediately, open it from the Start menu and check for prompts (updates, terms) |
| `-race requires cgo` / `cgo: C compiler "gcc" not found` on Windows | Native race detector needs a C toolchain | Use the Docker race command in §6 |
| Docker volume path errors like `/src` becoming `C:/Program Files/Git/src` in Git Bash | MSYS path conversion | Prefix the command with `MSYS_NO_PATHCONV=1` |
| Shell scripts fail in containers with `\r: command not found` | CRLF line endings | `.gitattributes` enforces LF for files checked out by git; for a file created locally with CRLF, convert it with `dos2unix <file>` or re-save it with LF in your editor |
