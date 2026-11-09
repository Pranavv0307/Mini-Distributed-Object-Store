// Package download is the controller's download coordinator (architecture
// §4.2): it streams a READY object's chunks in index order, verifying every
// chunk against the controller's checksum before any of its bytes are sent.
//
//	1 look up the object; it must be READY, else 404
//	2 load the chunk map
//	3 pre-flight: every chunk has ≥ 1 effective replica (OK on an ALIVE
//	  worker), else 503 before any response is started
//	4 fetch and verify chunk 0 (a failure is still a clean 503)
//	5 send headers; then for each chunk in order: fetch from a random
//	  effective replica, verify size and SHA-256, fall back to the next
//	  replica on failure, write
//	6 before writing the last chunk, compare the running whole-object hash
//	  with the stored object checksum (catches assembly bugs)
//
// A replica that fails verification, is missing (404), or has an unreadable
// header is marked CORRUPT (conditional on OK). Network errors, timeouts and
// 5xx only skip that replica: only heartbeats change worker liveness (§4.6).
// If a failure happens after the headers were sent, the connection is
// aborted so the client sees a truncated body, never wrong bytes.
package download

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"math/rand/v2"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/workerclient"
	"objstore/internal/httpx"
	"objstore/internal/ids"
	"objstore/internal/wire"
)

// Getter fetches chunks from workers (workerclient.Client).
type Getter interface {
	GetChunk(ctx context.Context, baseURL, chunkID string, maxBytes int64) (workerclient.Chunk, error)
}

// Coordinator serves downloads. It is safe for concurrent use.
type Coordinator struct {
	meta   *meta.Store
	reg    *registry.Registry
	client Getter
	log    *slog.Logger
}

// New creates a Coordinator.
func New(m *meta.Store, reg *registry.Registry, client Getter, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Coordinator{meta: m, reg: reg, client: client, log: logger}
}

// ErrNoValidReplica: every effective replica of a chunk failed.
var ErrNoValidReplica = errors.New("no valid replica")

// ServeObject handles GET and HEAD /api/v1/objects/{object_id}.
func (c *Coordinator) ServeObject(w http.ResponseWriter, r *http.Request, objectID string) {
	if !ids.ValidObjectID(objectID) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidObjectID, fmt.Sprintf("invalid object id %q", objectID))
		return
	}
	ctx := r.Context()
	obj, err := c.meta.GetObject(ctx, objectID)
	if errors.Is(err, meta.ErrNotFound) || (err == nil && obj.Status != meta.StatusReady) {
		// UPLOADING and DELETING objects do not exist for clients (I7).
		httpx.WriteError(w, http.StatusNotFound, wire.CodeObjectNotFound, fmt.Sprintf("object %s not found", objectID))
		return
	}
	if err != nil {
		c.internalError(w, "get object", err)
		return
	}
	chunks, err := c.meta.ChunkMap(ctx, objectID)
	if err != nil {
		c.internalError(w, "load chunk map", err)
		return
	}
	if len(chunks) != obj.NumChunks {
		c.internalError(w, "chunk map", fmt.Errorf("object %s has %d chunk rows, expected %d", objectID, len(chunks), obj.NumChunks))
		return
	}
	if idx, ok := c.preflight(chunks); !ok {
		c.log.Error("download_unavailable", "object_id", objectID, "chunk_index", idx, "reason", "no effective replica")
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeStorageUnavailable,
			fmt.Sprintf("chunk %d of object %s has no available valid copy", idx, objectID))
		return
	}

	if r.Method == http.MethodHead {
		setHeaders(w.Header(), obj)
		w.WriteHeader(http.StatusOK)
		return
	}

	started := time.Now()
	objHash := sha256.New()
	// Chunk 0 is fetched before the status line and before any object header
	// is set, so the most likely failure (a chunk with no valid copy) is still
	// a clean JSON 503 with correct headers.
	var first []byte
	if len(chunks) > 0 {
		first, err = c.fetchVerified(ctx, obj.ID, chunks[0])
		if err != nil {
			if ctx.Err() != nil {
				return // client gone
			}
			// Worker IDs and per-replica reasons go to the log only: clients
			// never learn chunk locations (spec §5.2, D2).
			c.log.Error("download_unavailable", "object_id", objectID, "chunk_index", 0, "error", err)
			httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeStorageUnavailable,
				fmt.Sprintf("chunk 0 of object %s has no available valid copy", objectID))
			return
		}
	} else if !checksumMatches(objHash, obj.ChecksumSHA256) {
		c.internalError(w, "object checksum", fmt.Errorf("empty object %s has checksum %s", objectID, obj.ChecksumSHA256))
		return
	}
	setHeaders(w.Header(), obj)
	w.WriteHeader(http.StatusOK)
	// Flush after the status line and after every chunk, so that if the
	// response must be aborted later, the client has already received the
	// 200 and the verified prefix and can see the truncation (rather than a
	// bare connection reset with nothing delivered).
	rc := http.NewResponseController(w)
	_ = rc.Flush()

	for i := range chunks {
		data := first
		if i > 0 {
			data, err = c.fetchVerified(ctx, obj.ID, chunks[i])
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.abort(obj.ID, i, err)
			}
		}
		objHash.Write(data)
		if i == len(chunks)-1 && !checksumMatches(objHash, obj.ChecksumSHA256) {
			// Every chunk verified, yet the assembled object does not: an
			// assembly bug (missing, duplicated, or reordered chunk). Never
			// send the final bytes of a wrong object.
			c.abort(obj.ID, i, fmt.Errorf("assembled object checksum does not match %s", obj.ChecksumSHA256))
		}
		if _, err := w.Write(data); err != nil {
			c.log.Info("download_client_gone", "object_id", obj.ID, "chunk_index", i, "error", err)
			return
		}
		if err := rc.Flush(); err != nil {
			c.log.Info("download_client_gone", "object_id", obj.ID, "chunk_index", i, "error", err)
			return
		}
		first = nil
	}
	elapsed := time.Since(started)
	c.log.Info("download_completed", "object_id", obj.ID, "size", obj.Size, "chunks", len(chunks),
		"duration", elapsed, "mb_per_s", float64(obj.Size)/1e6/max(elapsed.Seconds(), 1e-9))
}

// preflight reports whether every chunk has at least one effective replica
// (OK on an ALIVE worker), and otherwise the index of the first that has none.
func (c *Coordinator) preflight(chunks []meta.ChunkWithReplicas) (int, bool) {
	for i, ch := range chunks {
		if len(c.effective(ch)) == 0 {
			return i, false
		}
	}
	return 0, true
}

type candidate struct {
	workerID string
	address  string
}

// effective returns the chunk's OK replicas on ALIVE workers, shuffled so
// read load spreads across replicas.
func (c *Coordinator) effective(ch meta.ChunkWithReplicas) []candidate {
	var out []candidate
	for _, r := range ch.Replicas {
		if r.State != meta.ReplicaOK {
			continue
		}
		if w, ok := c.reg.Get(r.WorkerID); ok && w.State == registry.Alive {
			out = append(out, candidate{workerID: r.WorkerID, address: w.Address})
		}
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// fetchVerified returns the chunk's bytes from the first effective replica
// whose data matches the metadata size and checksum.
func (c *Coordinator) fetchVerified(ctx context.Context, objectID string, ch meta.ChunkWithReplicas) ([]byte, error) {
	cands := c.effective(ch)
	var reasons []string
	for _, cand := range cands {
		got, err := c.client.GetChunk(ctx, cand.address, ch.ID, ch.Size+1)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if reason, corrupt := classify(err); corrupt {
				c.markCorrupt(ctx, objectID, ch, cand.workerID, reason)
			} else {
				c.log.Warn("replica_read_failed", "object_id", objectID, "chunk_id", ch.ID,
					"worker_id", cand.workerID, "error", err)
			}
			reasons = append(reasons, cand.workerID+": "+err.Error())
			continue
		}
		if reason := verify(got.Data, ch); reason != "" {
			c.markCorrupt(ctx, objectID, ch, cand.workerID, reason)
			reasons = append(reasons, cand.workerID+": "+reason)
			continue
		}
		return got.Data, nil
	}
	if len(cands) == 0 {
		reasons = append(reasons, "no effective replica")
	}
	return nil, fmt.Errorf("%w for chunk %d (%s)", ErrNoValidReplica, ch.Idx, strings.Join(reasons, "; "))
}

// verify returns "" if data matches the chunk's metadata, else the reason.
func verify(data []byte, ch meta.ChunkWithReplicas) string {
	if int64(len(data)) != ch.Size {
		return fmt.Sprintf("size_mismatch: got %d bytes, want %d", len(data), ch.Size)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != ch.ChecksumSHA256 {
		return "checksum_mismatch"
	}
	return ""
}

// classify decides whether a worker error proves the replica is bad. A 404
// means the file is gone (counted as CORRUPT so repair rewrites it, DEC-040);
// CHUNK_CORRUPT means the worker cannot parse its own header. Everything else
// (timeouts, refused connections, 5xx) says nothing about the data.
func classify(err error) (reason string, corrupt bool) {
	var we *workerclient.Error
	if !errors.As(err, &we) {
		return "", false
	}
	switch {
	case we.Status == http.StatusNotFound:
		return "missing", true
	case we.Code == wire.CodeChunkCorrupt:
		return "header_corrupt", true
	default:
		return "", false
	}
}

// markCorrupt records the verdict on a context detached from the request, so
// it is not lost if the client disconnects.
func (c *Coordinator) markCorrupt(ctx context.Context, objectID string, ch meta.ChunkWithReplicas, workerID, reason string) {
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	changed, err := c.meta.MarkReplicaCorrupt(mctx, ch.ID, workerID)
	if err != nil {
		c.log.Error("mark_corrupt_failed", "object_id", objectID, "chunk_id", ch.ID, "worker_id", workerID, "error", err)
		return
	}
	c.log.Warn("corruption_detected", "object_id", objectID, "chunk_id", ch.ID, "chunk_index", ch.Idx,
		"worker_id", workerID, "reason", reason, "detected_by", "read", "marked_corrupt", changed)
}

// abort ends a response whose headers were already sent: the client sees a
// truncated body (Content-Length not reached) instead of wrong bytes.
func (c *Coordinator) abort(objectID string, idx int, err error) {
	c.log.Error("download_aborted", "object_id", objectID, "chunk_index", idx, "error", err)
	panic(http.ErrAbortHandler)
}

func (c *Coordinator) internalError(w http.ResponseWriter, op string, err error) {
	c.log.Error("download_failed", "op", op, "error", err)
	httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
}

func checksumMatches(h hash.Hash, want string) bool {
	return hex.EncodeToString(h.Sum(nil)) == want
}

func setHeaders(h http.Header, o meta.Object) {
	h.Set("Content-Type", o.ContentType)
	h.Set("Content-Length", strconv.FormatInt(o.Size, 10))
	h.Set("ETag", `"`+o.ChecksumSHA256+`"`)
	h.Set(wire.HeaderObjectChecksum, o.ChecksumSHA256)
	h.Set("Content-Disposition", contentDisposition(o.Name))
}

// contentDisposition builds an attachment header with a sanitised file name:
// path components and control characters are removed; non-ASCII names are
// encoded per RFC 2231 by mime.FormatMediaType.
func contentDisposition(name string) string {
	if i := strings.LastIndexAny(name, `/\`); i >= 0 {
		name = name[i+1:]
	}
	name = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == '"' {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return "attachment"
	}
	if v := mime.FormatMediaType("attachment", map[string]string{"filename": name}); v != "" {
		return v
	}
	return "attachment"
}
