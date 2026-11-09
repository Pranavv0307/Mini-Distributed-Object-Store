// Package store is a worker's on-disk chunk store.
//
// Layout under the data directory:
//
//	chunks/<chunk_id>   committed chunk files (header + payload, see format.go)
//	tmp/<chunk_id>.*.tmp  in-progress writes; deleted at startup
//
// Atomicity: a chunk is written to tmp/, verified against the expected size
// and SHA-256, fsynced, and only then renamed into chunks/ (followed by a
// directory fsync). A crash at any point leaves either the previous state of
// chunks/<chunk_id> or the complete new file, never a partial one; leftovers
// in tmp/ are removed by Open. Both directories live on the same volume, so
// the rename is atomic.
//
// The store makes no decisions about whether a chunk "should" exist; it only
// stores, returns, and deletes bytes. It never validates data on read: the
// controller's checksum is the authority (architecture §3.6).
package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"objstore/internal/ids"
)

// Errors returned (possibly wrapped) by Store methods.
var (
	ErrInvalidID           = errors.New("invalid chunk id")
	ErrNotFound            = errors.New("chunk not found")
	ErrChecksumMismatch    = errors.New("checksum mismatch")
	ErrSizeMismatch        = errors.New("size mismatch")
	ErrTooLarge            = errors.New("chunk too large")
	ErrCorrupt             = errors.New("chunk file corrupt")
	ErrInsufficientStorage = errors.New("insufficient storage")
)

// lockStripes is the number of striped locks. Operations on different chunks
// rarely contend; sharing a stripe between two chunks is harmless.
const lockStripes = 256

const copyBufferSize = 64 << 10

// Options configures a Store.
type Options struct {
	// Fsync makes writes and deletes durable across power loss
	// (FSYNC_ON_WRITE). Container restarts do not need it; benchmarks do.
	Fsync bool
	// MaxPayloadSize is the largest chunk payload accepted (CHUNK_SIZE). It
	// also bounds how much a read loads into memory.
	MaxPayloadSize int64
	Logger         *slog.Logger
}

// Store is safe for concurrent use.
type Store struct {
	chunksDir  string
	tmpDir     string
	fsync      bool
	maxPayload int64
	log        *slog.Logger

	// locks serialise the final rename, unlink, and read of the same chunk.
	// Readers take the read lock only while loading the file into memory,
	// never while sending it over the network, so a slow client cannot block
	// writers. (On Windows an open file cannot be replaced or deleted, so this
	// also keeps tests portable.)
	locks [lockStripes]sync.RWMutex
}

// Open prepares the data directory and removes temp files left by a crash.
func Open(dataDir string, opts Options) (*Store, error) {
	if opts.MaxPayloadSize <= 0 {
		return nil, fmt.Errorf("store: MaxPayloadSize must be positive, got %d", opts.MaxPayloadSize)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Store{
		chunksDir:  filepath.Join(dataDir, "chunks"),
		tmpDir:     filepath.Join(dataDir, "tmp"),
		fsync:      opts.Fsync,
		maxPayload: opts.MaxPayloadSize,
		log:        logger,
	}
	for _, d := range []string{s.chunksDir, s.tmpDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("store: create %s: %w", d, err)
		}
	}
	removed, err := s.cleanTemp()
	if err != nil {
		return nil, err
	}
	logger.Info("startup_temp_cleanup", "removed_temp_files", removed, "data_dir", dataDir)
	return s, nil
}

// cleanTemp deletes everything in tmp/. It runs only from Open, before the
// store serves requests, so no in-progress write can be affected.
func (s *Store) cleanTemp() (int, error) {
	entries, err := os.ReadDir(s.tmpDir)
	if err != nil {
		return 0, fmt.Errorf("store: list temp dir: %w", err)
	}
	removed := 0
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.tmpDir, e.Name())); err != nil {
			return removed, fmt.Errorf("store: remove temp file %s: %w", e.Name(), err)
		}
		removed++
	}
	return removed, nil
}

func (s *Store) lockFor(id string) *sync.RWMutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id)) // hash.Hash.Write never returns an error
	return &s.locks[h.Sum32()%lockStripes]
}

func (s *Store) path(id string) string { return filepath.Join(s.chunksDir, id) }

// Put stores exactly size bytes from r as chunk id, provided they hash to
// sum. It always writes a fresh file and atomically replaces any existing one,
// which makes it idempotent for retries and lets the controller repair a
// corrupt replica in place by re-sending the correct bytes (DEC-008).
//
// created reports whether no file for id existed before the call. On any
// error, the previously stored file (if any) is unchanged.
func (s *Store) Put(id string, size int64, sum [sha256.Size]byte, r io.Reader) (created bool, err error) {
	if !ids.ValidChunkID(id) {
		return false, fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	if size < 0 {
		return false, fmt.Errorf("%w: negative size %d", ErrSizeMismatch, size)
	}
	if size > s.maxPayload {
		return false, fmt.Errorf("%w: %d bytes exceeds the %d-byte limit", ErrTooLarge, size, s.maxPayload)
	}

	tmp, err := os.CreateTemp(s.tmpDir, id+".*.tmp")
	if err != nil {
		return false, s.classifyWriteErr("create temp file", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close() // may already be closed; the error is irrelevant
			if rmErr := os.Remove(tmp.Name()); rmErr != nil && !errors.Is(rmErr, fs.ErrNotExist) {
				s.log.Warn("temp_file_cleanup_failed", "path", tmp.Name(), "error", rmErr)
			}
		}
	}()

	// The header records the *expected* size and checksum; the file is only
	// committed if the received bytes match them, so the header is then true.
	if _, err := tmp.Write(Header{Version: formatVersion, PayloadSize: size, SHA256: sum}.marshal()); err != nil {
		return false, s.classifyWriteErr("write header", err)
	}
	h := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(tmp, h), io.LimitReader(r, size), make([]byte, copyBufferSize))
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return false, fmt.Errorf("%w: body ended after %d of %d bytes", ErrSizeMismatch, n, size)
		}
		return false, s.classifyWriteErr("write payload", err)
	}
	if n < size {
		return false, fmt.Errorf("%w: body ended after %d of %d bytes", ErrSizeMismatch, n, size)
	}
	var extra [1]byte
	if k, err := io.ReadFull(r, extra[:]); k > 0 {
		return false, fmt.Errorf("%w: body is longer than %d bytes", ErrSizeMismatch, size)
	} else if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, fmt.Errorf("store: read body: %w", err)
	}
	var got [sha256.Size]byte
	h.Sum(got[:0])
	if got != sum {
		return false, fmt.Errorf("%w: expected %x, received %x", ErrChecksumMismatch, sum, got)
	}
	if s.fsync {
		if err := tmp.Sync(); err != nil {
			return false, s.classifyWriteErr("fsync temp file", err)
		}
	}
	if err := tmp.Close(); err != nil {
		return false, s.classifyWriteErr("close temp file", err)
	}

	final := s.path(id)
	mu := s.lockFor(id)
	mu.Lock()
	_, statErr := os.Lstat(final)
	existed := statErr == nil
	renameErr := os.Rename(tmp.Name(), final)
	mu.Unlock()
	if renameErr != nil {
		return false, s.classifyWriteErr("rename into place", renameErr)
	}
	committed = true
	if s.fsync {
		if err := syncDir(s.chunksDir); err != nil {
			// The file is in place and readable; only power-loss durability of
			// the directory entry is uncertain. Report failure so the caller does
			// not ack a write it cannot vouch for; a retry is harmless.
			return false, fmt.Errorf("store: fsync chunks dir: %w", err)
		}
	}
	return !existed, nil
}

func (s *Store) classifyWriteErr(op string, err error) error {
	if isDiskFull(err) {
		return fmt.Errorf("%w: %s: %v", ErrInsufficientStorage, op, err)
	}
	return fmt.Errorf("store: %s: %w", op, err)
}

// Chunk is a chunk as read from disk.
type Chunk struct {
	// Header is the header as stored. Its checksum is the worker's own record,
	// not proof that Payload is intact.
	Header Header
	// Payload is every byte after the header, exactly as stored. If the file
	// was truncated or extended (corruption), its length differs from
	// Header.PayloadSize; it is capped at MaxPayloadSize+1 bytes so a grossly
	// oversized file cannot exhaust memory while still being detectable.
	Payload []byte
}

// Get reads chunk id. It returns ErrNotFound if absent and ErrCorrupt if the
// header is unreadable. It does not verify the payload checksum.
func (s *Store) Get(id string) (Chunk, error) {
	if !ids.ValidChunkID(id) {
		return Chunk{}, fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	mu := s.lockFor(id)
	mu.RLock()
	data, err := readAtMost(s.path(id), HeaderSize+s.maxPayload+1)
	mu.RUnlock()
	if errors.Is(err, fs.ErrNotExist) {
		return Chunk{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Chunk{}, fmt.Errorf("store: read %s: %w", id, err)
	}
	hdr, err := parseHeader(data)
	if err != nil {
		return Chunk{}, fmt.Errorf("chunk %s: %w", id, err)
	}
	return Chunk{Header: hdr, Payload: data[HeaderSize:]}, nil
}

func readAtMost(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit))
}

// Delete removes chunk id. Deleting an absent chunk succeeds (idempotent);
// existed reports whether a file was removed.
func (s *Store) Delete(id string) (existed bool, err error) {
	if !ids.ValidChunkID(id) {
		return false, fmt.Errorf("%w: %q", ErrInvalidID, id)
	}
	mu := s.lockFor(id)
	mu.Lock()
	err = os.Remove(s.path(id))
	mu.Unlock()
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: delete %s: %w", id, err)
	}
	if s.fsync {
		if err := syncDir(s.chunksDir); err != nil {
			return true, fmt.Errorf("store: fsync chunks dir: %w", err)
		}
	}
	return true, nil
}

// Entry is one committed chunk file in the inventory.
type Entry struct {
	ChunkID string
	// Size is the payload size on disk: file size minus the header, or 0 if
	// the file is shorter than a header. It comes from file metadata only, so
	// listing is cheap; appended or truncated bytes show up as a size that
	// differs from the controller's metadata.
	Size int64
}

// Inventory lists committed chunk files, sorted by chunk ID. Temp files are
// never included. Files in chunks/ whose names are not valid chunk IDs are
// ignored (and logged), since no chunk ID can refer to them.
func (s *Store) Inventory() ([]Entry, error) {
	dirEntries, err := os.ReadDir(s.chunksDir)
	if err != nil {
		return nil, fmt.Errorf("store: list chunks: %w", err)
	}
	out := make([]Entry, 0, len(dirEntries))
	for _, de := range dirEntries {
		name := de.Name()
		if !ids.ValidChunkID(name) {
			s.log.Warn("inventory_unexpected_file", "name", name)
			continue
		}
		info, err := de.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue // deleted concurrently
		}
		if err != nil {
			return nil, fmt.Errorf("store: stat %s: %w", name, err)
		}
		if !info.Mode().IsRegular() {
			s.log.Warn("inventory_unexpected_file", "name", name, "mode", info.Mode().String())
			continue
		}
		out = append(out, Entry{ChunkID: name, Size: max(info.Size()-HeaderSize, 0)})
	}
	return out, nil
}

// Report is the result of inspecting a chunk file (debug tool, DEC-012).
type Report struct {
	Path          string
	FileSize      int64
	Header        Header
	HeaderErr     error
	PayloadSize   int64
	PayloadSHA256 [sha256.Size]byte
	Valid         bool
	Problem       string
}

// Inspect reads a chunk file and checks its payload against its own header.
// It is a debugging aid; it reads the whole file regardless of size limits.
func Inspect(path string) (Report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Report{}, err
	}
	r := Report{Path: path, FileSize: int64(len(data))}
	r.Header, r.HeaderErr = parseHeader(data)
	if r.HeaderErr != nil {
		r.Problem = r.HeaderErr.Error()
		return r, nil
	}
	payload := data[HeaderSize:]
	r.PayloadSize = int64(len(payload))
	r.PayloadSHA256 = sha256.Sum256(payload)
	var problems []string
	if r.PayloadSize != r.Header.PayloadSize {
		problems = append(problems, fmt.Sprintf("payload is %d bytes, header says %d", r.PayloadSize, r.Header.PayloadSize))
	}
	if r.PayloadSHA256 != r.Header.SHA256 {
		problems = append(problems, "payload SHA-256 does not match header")
	}
	r.Valid = len(problems) == 0
	r.Problem = strings.Join(problems, "; ")
	return r, nil
}

// Stats summarises the committed chunks.
type Stats struct {
	ChunkCount int64
	UsedBytes  int64 // sum of payload sizes on disk
}

// Stats scans the inventory (file metadata only). It is called once per
// heartbeat; its cost is one directory listing.
func (s *Store) Stats() (Stats, error) {
	inv, err := s.Inventory()
	if err != nil {
		return Stats{}, err
	}
	st := Stats{ChunkCount: int64(len(inv))}
	for _, e := range inv {
		st.UsedBytes += e.Size
	}
	return st, nil
}

// FreeBytes reports the space available on the data volume, or -1 if it
// cannot be determined (always on Windows, see fs_windows.go).
func (s *Store) FreeBytes() int64 {
	n, err := freeBytes(s.chunksDir)
	if err != nil {
		s.log.Debug("free_bytes_unavailable", "error", err)
		return -1
	}
	return n
}
