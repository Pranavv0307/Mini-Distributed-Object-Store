// Package server exposes a worker's chunk store over HTTP (docs/API.md,
// "Internal API — worker endpoints").
package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"

	"objstore/internal/httpx"
	"objstore/internal/ids"
	"objstore/internal/wire"
	"objstore/internal/worker/store"
)

// Config configures a Server.
type Config struct {
	WorkerID string
	// MaxChunkSize is the largest accepted chunk payload (CHUNK_SIZE).
	MaxChunkSize int64
	// MaxConcurrentReads bounds the number of chunk GETs holding a chunk in
	// memory at once; total read memory ≤ MaxConcurrentReads × MaxChunkSize.
	MaxConcurrentReads int
	Logger             *slog.Logger
}

// Server handles the worker's HTTP API.
type Server struct {
	store    *store.Store
	workerID string
	maxChunk int64
	readSem  chan struct{}
	log      *slog.Logger

	// gate admits chunk PUT and DELETE (DEC-006). In standalone mode it is
	// opened at startup (DEC-026); with a controller, the membership agent
	// opens it after each successful registration and closes and drains it
	// before each inventory snapshot.
	gate       writeGate
	registered atomic.Bool
}

// New creates a Server. Writes are refused until OpenWrites is called.
func New(st *store.Store, cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Server{
		store:    st,
		workerID: cfg.WorkerID,
		maxChunk: cfg.MaxChunkSize,
		readSem:  make(chan struct{}, max(cfg.MaxConcurrentReads, 1)),
		log:      logger,
	}
}

// OpenWrites opens the write gate: chunk PUT and DELETE are admitted.
func (s *Server) OpenWrites() { s.gate.setOpen() }

// CloseWritesAndDrain closes the write gate and waits until every admitted
// PUT/DELETE has finished (or ctx ends). Afterwards, until OpenWrites, the
// set of chunk files cannot change through the API, so an inventory snapshot
// is exact.
func (s *Server) CloseWritesAndDrain(ctx context.Context) error {
	return s.gate.closeAndDrain(ctx)
}

// SetRegistered records whether the worker is registered with a controller
// (reported by /healthz).
func (s *Server) SetRegistered(v bool) { s.registered.Store(v) }

// Handler returns the HTTP handler with logging and panic recovery.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /internal/chunks/{chunk_id}", s.putChunk)
	mux.HandleFunc("GET /internal/chunks/{chunk_id}", s.getChunk)
	mux.HandleFunc("DELETE /internal/chunks/{chunk_id}", s.deleteChunk)
	mux.HandleFunc("GET /internal/workers/{worker_id}/chunks", s.listChunks)
	mux.HandleFunc("GET /healthz", s.healthz)
	return httpx.Middleware(s.log, mux)
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	if !ids.ValidChunkID(id) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidChunkID, fmt.Sprintf("invalid chunk id %q", id))
		return
	}
	if !s.gate.enter() {
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeNotRegistered, "worker is not accepting writes until it is registered")
		return
	}
	defer s.gate.exit()
	sum, err := parseSHA256(r.Header.Get(wire.HeaderChunkSHA256))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, err.Error())
		return
	}
	if r.ContentLength < 0 {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, "Content-Length header is required")
		return
	}
	if r.ContentLength > s.maxChunk {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeChunkTooLarge,
			fmt.Sprintf("chunk of %d bytes exceeds the %d-byte limit", r.ContentLength, s.maxChunk))
		return
	}

	created, err := s.store.Put(id, r.ContentLength, sum, r.Body)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrChecksumMismatch):
		s.log.Warn("chunk_put_rejected", "chunk_id", id, "reason", "checksum_mismatch", "error", err)
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeChecksumMismatch, err.Error())
		return
	case errors.Is(err, store.ErrSizeMismatch):
		s.log.Warn("chunk_put_rejected", "chunk_id", id, "reason", "size_mismatch", "error", err)
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeSizeMismatch, err.Error())
		return
	case errors.Is(err, store.ErrTooLarge):
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeChunkTooLarge, err.Error())
		return
	case errors.Is(err, store.ErrInsufficientStorage):
		s.log.Error("chunk_put_failed", "chunk_id", id, "reason", "disk_full", "error", err)
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeInsufficientStorage, "worker disk is full")
		return
	default:
		if r.Context().Err() != nil {
			// The client went away mid-upload; nothing was committed.
			s.log.Info("chunk_put_aborted", "chunk_id", id, "error", err)
			return
		}
		s.log.Error("chunk_put_failed", "chunk_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "failed to store chunk")
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	s.log.Info("chunk_stored", "chunk_id", id, "size", r.ContentLength, "created", created)
	httpx.WriteJSON(w, status, wire.PutChunkResponse{
		ChunkID: id, Size: r.ContentLength, ChecksumSHA256: hex.EncodeToString(sum[:]), Created: created,
	})
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	if !ids.ValidChunkID(id) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidChunkID, fmt.Sprintf("invalid chunk id %q", id))
		return
	}
	select {
	case s.readSem <- struct{}{}:
		defer func() { <-s.readSem }()
	case <-r.Context().Done():
		return
	}
	c, err := s.store.Get(id)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		httpx.WriteError(w, http.StatusNotFound, wire.CodeChunkNotFound, fmt.Sprintf("chunk %s not found", id))
		return
	case errors.Is(err, store.ErrCorrupt):
		s.log.Warn("chunk_header_corrupt", "chunk_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeChunkCorrupt, err.Error())
		return
	default:
		s.log.Error("chunk_get_failed", "chunk_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "failed to read chunk")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.Itoa(len(c.Payload)))
	h.Set(wire.HeaderChunkSHA256, hex.EncodeToString(c.Header.SHA256[:]))
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(c.Payload); err != nil {
		s.log.Info("chunk_get_aborted", "chunk_id", id, "error", err)
	}
}

func (s *Server) deleteChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	if !ids.ValidChunkID(id) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidChunkID, fmt.Sprintf("invalid chunk id %q", id))
		return
	}
	if !s.gate.enter() {
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeNotRegistered, "worker is not accepting writes until it is registered")
		return
	}
	defer s.gate.exit()
	existed, err := s.store.Delete(id)
	if err != nil {
		s.log.Error("chunk_delete_failed", "chunk_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "failed to delete chunk")
		return
	}
	s.log.Info("chunk_deleted", "chunk_id", id, "existed", existed)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listChunks(w http.ResponseWriter, r *http.Request) {
	if wid := r.PathValue("worker_id"); wid != s.workerID {
		httpx.WriteError(w, http.StatusNotFound, wire.CodeWorkerNotFound,
			fmt.Sprintf("this is worker %q, not %q", s.workerID, wid))
		return
	}
	inv, err := s.store.Inventory()
	if err != nil {
		s.log.Error("inventory_failed", "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "failed to list chunks")
		return
	}
	resp := wire.InventoryResponse{WorkerID: s.workerID, Chunks: make([]wire.ChunkInfo, len(inv))}
	for i, e := range inv {
		resp.Chunks[i] = wire.ChunkInfo{ChunkID: e.ChunkID, Size: e.Size}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, wire.WorkerHealth{
		Status:          "ok",
		WorkerID:        s.workerID,
		Registered:      s.registered.Load(),
		AcceptingWrites: s.gate.isOpen(),
	})
}

func parseSHA256(v string) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	if v == "" {
		return sum, fmt.Errorf("%s header is required", wire.HeaderChunkSHA256)
	}
	if len(v) != 2*sha256.Size {
		return sum, fmt.Errorf("%s must be %d hex characters", wire.HeaderChunkSHA256, 2*sha256.Size)
	}
	if _, err := hex.Decode(sum[:], []byte(v)); err != nil {
		return sum, fmt.Errorf("%s is not valid hex: %v", wire.HeaderChunkSHA256, err)
	}
	return sum, nil
}
