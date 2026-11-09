// Package upload is the controller's upload coordinator (architecture §4.1).
//
// For each object:
//
//	admission (≥ RF eligible workers, else 503)
//	→ object row UPLOADING (when the file part starts)
//	→ for each chunk, read ≤ CHUNK_SIZE bytes from the multipart stream:
//	    hash it, place it on RF distinct ALIVE workers,
//	    CLAIM (chunk + PENDING replica rows, one transaction),
//	    PUT to all targets in parallel, mark each replica OK on ack;
//	    a failed or timed-out PUT moves that claim to pending_deletes and
//	    re-claims on a different worker (one transaction), never reusing a
//	    worker that already failed in this upload
//	→ COMMIT: UPLOADING → READY in one conditional transaction.
//
// At most Window chunks are in flight; when the window is full the
// coordinator stops reading the request body (backpressure), so memory per
// upload is bounded by Window × ChunkSize. Window = 1 is the sequential
// variant (M3a).
//
// Any failure (worker shortage, client disconnect, invalid metadata,
// size limit, internal error) aborts through the delete path: the object
// becomes DELETING and all its replica rows become pending deletes, which the
// janitor removes (architecture §4.4 "abort = delete").
package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"objstore/internal/controller/custommeta"
	"objstore/internal/controller/meta"
	"objstore/internal/controller/placement"
	"objstore/internal/controller/registry"
	"objstore/internal/ids"
	"objstore/internal/wire"
)

// Limits on fields that end up in the metadata database or response headers.
const (
	maxNameBytes        = 1024
	maxContentTypeBytes = 256
)

// PutClient stores chunks on workers (workerclient.Client).
type PutClient interface {
	PutChunk(ctx context.Context, baseURL, chunkID string, data []byte, sum [sha256.Size]byte) error
}

// Config configures a Coordinator.
type Config struct {
	ChunkSize         int64 // CHUNK_SIZE (1,000,000)
	ReplicationFactor int   // REPLICATION_FACTOR (2)
	Window            int   // UPLOAD_WINDOW: chunks in flight per upload
	MaxObjectSize     int64 // MAX_OBJECT_SIZE
	MinFreeBytes      int64 // PLACEMENT_MIN_FREE_BYTES
	// ClientReadTimeout bounds how long one read of the request body may
	// stall (CLIENT_READ_TIMEOUT); a stalled client cannot pin resources.
	ClientReadTimeout time.Duration
	Logger            *slog.Logger
}

// Coordinator runs uploads. It is safe for concurrent use.
type Coordinator struct {
	meta    *meta.Store
	reg     *registry.Registry
	client  PutClient
	policy  placement.Policy
	cfg     Config
	log     *slog.Logger
	bufs    sync.Pool
	onAbort func()
}

// New creates a Coordinator. onAbort (may be nil) is called after an upload
// is aborted, e.g. to kick the janitor.
func New(m *meta.Store, reg *registry.Registry, client PutClient, policy placement.Policy, cfg Config, onAbort func()) *Coordinator {
	if cfg.Window < 1 {
		cfg.Window = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c := &Coordinator{meta: m, reg: reg, client: client, policy: policy, cfg: cfg, log: logger, onAbort: onAbort}
	c.bufs.New = func() any { b := make([]byte, cfg.ChunkSize); return &b }
	return c
}

// Error is an upload failure with its HTTP mapping (architecture §5.8).
type Error struct {
	Status  int
	Code    string
	Message string
	// ClientGone: the request body could not be read (client disconnected or
	// stalled); the response will most likely not be delivered.
	ClientGone bool
	Err        error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func badRequest(code, msg string, err error) *Error {
	return &Error{Status: http.StatusBadRequest, Code: code, Message: msg, Err: err}
}

var errNoWorker = errors.New("no eligible worker left for a replica")

// Upload consumes a multipart/form-data body (parts "file", required once,
// and "metadata", optional, in any order) and returns the committed object.
// setReadDeadline (may be nil) sets the connection's read deadline; it is
// called before each body read.
func (c *Coordinator) Upload(ctx context.Context, mr *multipart.Reader, setReadDeadline func(time.Time)) (meta.Object, error) {
	if n := len(placement.Eligible(c.reg.Alive(), nil, c.cfg.MinFreeBytes)); n < c.cfg.ReplicationFactor {
		return meta.Object{}, &Error{Status: http.StatusServiceUnavailable, Code: wire.CodeInsufficientWorkers,
			Message: fmt.Sprintf("%d eligible workers, need at least %d", n, c.cfg.ReplicationFactor)}
	}
	extend := func() {
		if setReadDeadline != nil {
			setReadDeadline(time.Now().Add(c.cfg.ClientReadTimeout))
		}
	}

	var (
		md       map[string]string
		mdSeen   bool
		obj      *meta.Object
		res      fileResult
		started  = time.Now()
		fail     = func(err error) (meta.Object, error) { return meta.Object{}, err }
		abortErr = func(err error) (meta.Object, error) {
			if obj != nil {
				c.abort(ctx, obj.ID, err)
			}
			return meta.Object{}, err
		}
	)
	for {
		extend()
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return abortErr(c.bodyError("malformed multipart body", err))
		}
		switch part.FormName() {
		case "metadata":
			if mdSeen {
				return abortErr(badRequest(wire.CodeInvalidRequest, "more than one metadata part", nil))
			}
			mdSeen = true
			raw, err := io.ReadAll(io.LimitReader(part, custommeta.MaxDocumentBytes+1))
			if err != nil {
				return abortErr(c.bodyError("reading metadata part", err))
			}
			if md, err = custommeta.ParseUpload(raw); err != nil {
				return abortErr(metadataError(err))
			}
		case "file":
			if obj != nil {
				return abortErr(badRequest(wire.CodeMissingFile, "more than one file part", nil))
			}
			o, err := c.createObject(ctx, part)
			if err != nil {
				return fail(err)
			}
			obj = &o
			res, err = c.storeFile(ctx, o.ID, part, extend)
			if err != nil {
				return abortErr(err)
			}
		default:
			c.log.Debug("upload_part_ignored", "form_name", part.FormName())
		}
	}
	if obj == nil {
		return fail(badRequest(wire.CodeMissingFile, `the multipart body has no "file" part`, nil))
	}

	committed, err := c.meta.CommitObject(ctx, meta.CommitInput{
		ObjectID:          obj.ID,
		Size:              res.size,
		NumChunks:         res.chunks,
		ChecksumSHA256:    res.checksum,
		CustomMetadata:    md,
		ReplicationFactor: c.cfg.ReplicationFactor,
	})
	if err != nil {
		return abortErr(c.internalOrAborted("commit", err))
	}
	elapsed := time.Since(started)
	c.log.Info("upload_committed", "object_id", committed.ID, "size", committed.Size,
		"chunks", committed.NumChunks, "duration", elapsed,
		"mb_per_s", float64(committed.Size)/1e6/max(elapsed.Seconds(), 1e-9))
	return committed, nil
}

func (c *Coordinator) createObject(ctx context.Context, part *multipart.Part) (meta.Object, error) {
	name := part.FileName()
	if len(name) > maxNameBytes {
		return meta.Object{}, badRequest(wire.CodeInvalidRequest, fmt.Sprintf("file name longer than %d bytes", maxNameBytes), nil)
	}
	ct := part.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	if _, _, err := mime.ParseMediaType(ct); err != nil || len(ct) > maxContentTypeBytes {
		return meta.Object{}, badRequest(wire.CodeInvalidRequest, fmt.Sprintf("invalid Content-Type %q on the file part", ct), err)
	}
	o, err := c.meta.CreateObject(ctx, ids.NewObjectID(), name, ct)
	if err != nil {
		return meta.Object{}, &Error{Status: http.StatusInternalServerError, Code: wire.CodeInternal, Message: "create object", Err: err}
	}
	c.log.Info("upload_started", "object_id", o.ID, "name", name, "content_type", ct)
	return o, nil
}

type fileResult struct {
	size     int64
	chunks   int
	checksum string
}

// storeFile streams the file part into chunks with a bounded window.
func (c *Coordinator) storeFile(ctx context.Context, objectID string, body io.Reader, extend func()) (fileResult, error) {
	uctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(uctx)
	window := make(chan struct{}, c.cfg.Window)
	failed := &failedSet{m: map[string]bool{}}
	objHash := sha256.New()
	var total int64
	idx := 0

	readErr := func() error {
		for {
			// Take a window slot before reading: when Window chunks are in
			// flight, this blocks and the client body is not read
			// (backpressure).
			select {
			case window <- struct{}{}:
			case <-gctx.Done():
				return nil // a chunk failed; g.Wait reports it
			}
			bp := c.bufs.Get().(*[]byte)
			release := func() { c.bufs.Put(bp); <-window }
			extend()
			n, err := readChunk(body, *bp)
			if n > 0 {
				total += int64(n)
				if total > c.cfg.MaxObjectSize {
					release()
					return badRequest(wire.CodeObjectTooLarge, fmt.Sprintf("object exceeds the %d-byte limit", c.cfg.MaxObjectSize), nil)
				}
				data := (*bp)[:n]
				objHash.Write(data)
				chunkID, idErr := ids.ChunkID(objectID, idx)
				if idErr != nil {
					release()
					return badRequest(wire.CodeObjectTooLarge, "too many chunks", idErr)
				}
				ch := meta.Chunk{ID: chunkID, ObjectID: objectID, Idx: idx, Size: int64(n)}
				sum := sha256.Sum256(data)
				ch.ChecksumSHA256 = hex.EncodeToString(sum[:])
				g.Go(func() error {
					defer release()
					return c.storeChunk(gctx, ch, data, sum, failed)
				})
				idx++
			} else {
				release()
			}
			switch {
			case err == nil:
				// Full chunk read; there may be more.
			case err == io.EOF:
				// Clean end of the file part: the multipart reader saw the
				// closing boundary (possibly after a short last chunk).
				return nil
			default:
				// Anything else, including io.ErrUnexpectedEOF from a body
				// truncated before the boundary, means the file is incomplete
				// and must never be committed.
				return c.bodyError("reading file data", err)
			}
		}
	}()
	if readErr != nil {
		cancel() // stop in-flight chunk writes
	}
	// Always wait: no goroutine may outlive this call or touch a pooled buffer.
	waitErr := g.Wait()
	// A read error is the root cause when present: the chunk writes then
	// failed only because cancel() stopped them.
	if readErr != nil {
		return fileResult{}, readErr
	}
	if waitErr != nil {
		return fileResult{}, waitErr
	}
	return fileResult{size: total, chunks: idx, checksum: hex.EncodeToString(objHash.Sum(nil))}, nil
}

// readChunk fills buf from r. It returns err == nil for a full buffer,
// io.EOF (unwrapped, exactly) when r ended cleanly, and any other error
// as-is. Unlike io.ReadFull it never converts errors, so a clean end of the
// part (io.EOF) stays distinguishable from a truncated body
// (io.ErrUnexpectedEOF from the multipart reader).
func readChunk(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// storeChunk claims and writes one chunk on ReplicationFactor workers.
func (c *Coordinator) storeChunk(ctx context.Context, ch meta.Chunk, data []byte, sum [sha256.Size]byte, failed *failedSet) error {
	rf := c.cfg.ReplicationFactor
	targets := c.policy.Choose(ch.ID, rf, placement.Eligible(c.reg.Alive(), failed.snapshot(), c.cfg.MinFreeBytes))
	if len(targets) < rf {
		return &Error{Status: http.StatusServiceUnavailable, Code: wire.CodeUploadFailed,
			Message: fmt.Sprintf("chunk %d: %d eligible workers, need %d", ch.Idx, len(targets), rf)}
	}
	if err := c.meta.ClaimChunk(ctx, ch, targets); err != nil {
		return c.internalOrAborted("claim chunk", err)
	}
	slots := &slotSet{current: map[string]bool{}}
	for _, t := range targets {
		slots.current[t] = true
	}
	g, gctx := errgroup.WithContext(ctx)
	for _, t := range targets {
		g.Go(func() error { return c.writeReplica(gctx, ch, data, sum, t, failed, slots) })
	}
	return g.Wait()
}

// writeReplica writes one replica, moving it to another worker on failure
// until it succeeds or no eligible worker is left.
func (c *Coordinator) writeReplica(ctx context.Context, ch meta.Chunk, data []byte, sum [sha256.Size]byte,
	target string, failed *failedSet, slots *slotSet) error {
	for {
		err := c.put(ctx, ch, data, sum, target)
		if err == nil {
			if err := c.meta.MarkReplicaOK(ctx, ch.ObjectID, ch.ID, target); err != nil {
				return c.internalOrAborted("mark replica OK", err)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// The write failed or its outcome is unknown (timeout). Never assume
		// the bytes did not land: the claim becomes a pending delete.
		failed.add(target)
		next, ok := slots.replace(target, func(exclude map[string]bool) []string {
			for w := range failed.snapshot() {
				exclude[w] = true
			}
			return c.policy.Choose(ch.ID, 1, placement.Eligible(c.reg.Alive(), exclude, c.cfg.MinFreeBytes))
		})
		c.log.Warn("replica_write_failed", "object_id", ch.ObjectID, "chunk_id", ch.ID, "worker_id", target,
			"retry_on", next, "error", err)
		if !ok {
			return &Error{Status: http.StatusServiceUnavailable, Code: wire.CodeUploadFailed,
				Message: fmt.Sprintf("chunk %d: not enough distinct workers left after worker failures", ch.Idx), Err: errNoWorker}
		}
		if err := c.meta.ReplaceReplica(ctx, ch.ObjectID, ch.ID, target, next, "replica_write_failed"); err != nil {
			return c.internalOrAborted("replace replica", err)
		}
		target = next
	}
}

func (c *Coordinator) put(ctx context.Context, ch meta.Chunk, data []byte, sum [sha256.Size]byte, workerID string) error {
	w, ok := c.reg.Get(workerID)
	if !ok || w.State != registry.Alive {
		return fmt.Errorf("worker %s is not ALIVE", workerID)
	}
	return c.client.PutChunk(ctx, w.Address, ch.ID, data, sum)
}

// abort moves the object to DELETING (queuing physical cleanup) using a
// context detached from the request, so it completes even if the client is
// gone. Repeating it is harmless.
func (c *Coordinator) abort(ctx context.Context, objectID string, cause error) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	moved, err := c.meta.MarkDeleting(actx, objectID, "upload_aborted", meta.StatusUploading)
	if err != nil {
		// The object stays UPLOADING; the Phase 9 stale-upload sweep and
		// startup recovery will abort it.
		c.log.Error("upload_abort_failed", "object_id", objectID, "error", err, "cause", cause)
		return
	}
	c.log.Warn("upload_aborted", "object_id", objectID, "transitioned", moved, "cause", cause)
	if c.onAbort != nil {
		c.onAbort()
	}
}

func (c *Coordinator) bodyError(msg string, err error) *Error {
	e := badRequest(wire.CodeInvalidRequest, msg, err)
	e.ClientGone = true
	return e
}

// internalOrAborted maps metadata errors: if the object is no longer
// UPLOADING (aborted concurrently) the upload failed for a server-side
// reason; anything else is an internal error.
func (c *Coordinator) internalOrAborted(op string, err error) error {
	var ue *Error
	if errors.As(err, &ue) {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, meta.ErrNotUploading) || errors.Is(err, meta.ErrClaimLost) {
		return &Error{Status: http.StatusServiceUnavailable, Code: wire.CodeUploadFailed, Message: op + ": upload was aborted", Err: err}
	}
	return &Error{Status: http.StatusInternalServerError, Code: wire.CodeInternal, Message: op, Err: err}
}

func metadataError(err error) *Error {
	code := wire.CodeInvalidMetadata
	if errors.Is(err, custommeta.ErrLimit) {
		code = wire.CodeMetadataLimitExceeded
	}
	return badRequest(code, err.Error(), nil)
}

// failedSet is the set of workers that failed during one upload; they are
// not used again for that upload (§5.5).
type failedSet struct {
	mu sync.Mutex
	m  map[string]bool
}

func (f *failedSet) add(w string) { f.mu.Lock(); f.m[w] = true; f.mu.Unlock() }

func (f *failedSet) snapshot() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.m))
	for k := range f.m {
		out[k] = true
	}
	return out
}

// slotSet tracks the workers currently holding a claim for one chunk, so
// concurrent replacements of the chunk's two replicas never pick the same
// worker.
type slotSet struct {
	mu      sync.Mutex
	current map[string]bool
}

// replace swaps old for a worker chosen by pick (given the workers to
// exclude) and reports whether one was found.
func (s *slotSet) replace(old string, pick func(exclude map[string]bool) []string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exclude := map[string]bool{}
	for w := range s.current {
		exclude[w] = true
	}
	got := pick(exclude)
	if len(got) == 0 {
		return "", false
	}
	delete(s.current, old)
	s.current[got[0]] = true
	return got[0], true
}
