// Package api implements the controller's HTTP endpoints. Phase 2 provides
// the membership endpoints (register, heartbeat) and observability
// (/healthz, /cluster); the client object API is added from Phase 3.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"objstore/internal/controller/download"
	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/controller/repair"
	"objstore/internal/controller/upload"
	"objstore/internal/httpx"
	"objstore/internal/ids"
	"objstore/internal/version"
	"objstore/internal/wire"
)

// Request body limits. An inventory entry is ~80 bytes of JSON, so 64 MiB
// allows ~800k chunks per worker, far beyond this system's scale.
const (
	maxRegisterBody  = 64 << 20
	maxHeartbeatBody = 1 << 20
)

// InventoryReconciler compares a registering worker's inventory with the
// metadata (architecture §4.5 table). It runs while the worker is SYNCING and
// must not hold a database transaction across network calls. It must be
// idempotent: the same worker may register many times.
//
// Phase 2 has no metadata yet and uses NoopReconciler; Phase 6 supplies the
// real implementation.
type InventoryReconciler interface {
	ReconcileInventory(ctx context.Context, workerID string, epoch uint64, inventory []wire.ChunkInfo) error
}

// NoopReconciler accepts every inventory unchanged (Phase 2).
type NoopReconciler struct{ Logger *slog.Logger }

// ReconcileInventory logs the inventory size and does nothing else.
func (n NoopReconciler) ReconcileInventory(_ context.Context, workerID string, epoch uint64, inv []wire.ChunkInfo) error {
	if n.Logger != nil {
		n.Logger.Info("inventory_reconciled", "worker_id", workerID, "epoch", epoch,
			"chunks", len(inv), "note", "no metadata yet (Phase 2): nothing to compare")
	}
	return nil
}

// Config configures the API server.
type Config struct {
	HeartbeatInterval time.Duration
	DeadAfter         time.Duration
	RepairGrace       time.Duration
	// Meta, if set, is pinged by /healthz, and enables the object API,
	// /cluster storage totals, and /admin/audit.
	Meta *meta.Store
	// ReplicationFactor is used for /cluster totals and the audit.
	ReplicationFactor int
	// Repair, if set, contributes the /cluster repair section.
	Repair interface{ Stats() repair.Stats }
	// Uploader, if set, serves POST /api/v1/objects.
	Uploader *upload.Coordinator
	// Downloader, if set, serves GET/HEAD /api/v1/objects/{object_id}.
	Downloader *download.Coordinator
	// OnDelete, if set, is called after a client delete (kicks the janitor).
	OnDelete func()
	// MaxConcurrentDownloads bounds simultaneous downloads
	// (MAX_CONCURRENT_DOWNLOADS); each holds at most one chunk in memory.
	MaxConcurrentDownloads int
	// MaxConcurrentUploads bounds simultaneous uploads (MAX_CONCURRENT_UPLOADS);
	// further uploads get 503 SERVER_BUSY. Upload memory is bounded by
	// MaxConcurrentUploads × UPLOAD_WINDOW × CHUNK_SIZE.
	MaxConcurrentUploads int
	Logger               *slog.Logger
	// Inventories supplies metadata-only worker snapshots for audit v2.
	Inventories func(context.Context, []registry.Worker) (map[string][]meta.InventoryEntry, error)
}

// Server serves the controller's HTTP API.
type Server struct {
	reg        *registry.Registry
	reconciler InventoryReconciler
	cfg        Config
	startedAt  time.Time
	uploadSem  chan struct{}
	dlSem      chan struct{}
	log        *slog.Logger
}

// New creates a Server.
func New(reg *registry.Registry, reconciler InventoryReconciler, cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Server{reg: reg, reconciler: reconciler, cfg: cfg, startedAt: reg.Now(),
		uploadSem: make(chan struct{}, max(cfg.MaxConcurrentUploads, 1)),
		dlSem:     make(chan struct{}, max(cfg.MaxConcurrentDownloads, 1)), log: logger}
}

// Handler returns the HTTP handler with logging and panic recovery.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/workers/register", s.register)
	mux.HandleFunc("POST /internal/workers/heartbeat", s.heartbeat)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /cluster", s.cluster)
	if s.cfg.Meta != nil {
		mux.HandleFunc("GET /admin/audit", s.audit)
	}
	if s.cfg.Uploader != nil {
		mux.HandleFunc("POST /api/v1/objects", s.uploadObject)
	}
	s.objectRoutes(mux)
	if s.cfg.Downloader != nil {
		// "GET" patterns also match HEAD.
		mux.HandleFunc("GET /api/v1/objects/{object_id}", s.downloadObject)
	}
	return httpx.Middleware(s.log, mux)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, limit int64, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, limit))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %v", err)
	}
	if dec.More() {
		return errors.New("invalid JSON body: trailing data")
	}
	return nil
}

func validAddress(a string) bool {
	u, err := url.Parse(a)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" &&
		(u.Path == "" || u.Path == "/") && u.RawQuery == ""
}

// register implements the controller side of architecture §4.5 with the
// SYNCING protocol of DEC-006:
//
//  1. validate; 2. BeginRegistration → SYNCING under a new epoch;
//  3. reconcile the inventory (DB-only work in later phases);
//  4. CompleteRegistration → ALIVE only if the epoch is still current.
//
// The reply is sent only after step 4, so a worker that receives 200 knows
// it is ALIVE under the returned epoch.
func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var req wire.RegisterRequest
	if err := decodeJSON(w, r, maxRegisterBody, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, err.Error())
		return
	}
	if !ids.ValidWorkerID(req.WorkerID) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, fmt.Sprintf("invalid worker_id %q", req.WorkerID))
		return
	}
	if !validAddress(req.Address) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, fmt.Sprintf("invalid address %q: want http(s)://host:port", req.Address))
		return
	}
	for _, c := range req.Inventory {
		if !ids.ValidChunkID(c.ChunkID) || c.Size < 0 {
			httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, fmt.Sprintf("invalid inventory entry %+v", c))
			return
		}
	}

	epoch := s.reg.BeginRegistration(req.WorkerID, req.Address, registry.Capacity{
		FreeBytes: req.FreeBytes, UsedBytes: req.UsedBytes, ChunkCount: int64(len(req.Inventory)),
	})
	if err := s.reconciler.ReconcileInventory(r.Context(), req.WorkerID, epoch, req.Inventory); err != nil {
		// The worker stays SYNCING (unusable) and will retry registration; if it
		// stops trying, the failure detector eventually marks it DEAD.
		s.log.Error("inventory_reconcile_failed", "worker_id", req.WorkerID, "epoch", epoch, "error", err)
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeInternal, "inventory reconciliation failed; retry registration")
		return
	}
	if !s.reg.CompleteRegistration(req.WorkerID, epoch) {
		httpx.WriteError(w, http.StatusConflict, wire.CodeRegistrationSuperseded,
			"registration was superseded or the worker was declared dead during reconciliation; register again")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, wire.RegisterResponse{
		Epoch:               epoch,
		HeartbeatIntervalMs: s.cfg.HeartbeatInterval.Milliseconds(),
	})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var req wire.HeartbeatRequest
	if err := decodeJSON(w, r, maxHeartbeatBody, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, err.Error())
		return
	}
	if !ids.ValidWorkerID(req.WorkerID) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, fmt.Sprintf("invalid worker_id %q", req.WorkerID))
		return
	}
	reregister := s.reg.Heartbeat(req.WorkerID, req.Epoch, registry.Capacity{
		FreeBytes: req.FreeBytes, UsedBytes: req.UsedBytes, ChunkCount: req.ChunkCount,
	})
	if !reregister && s.cfg.Meta != nil {
		for _, chunkID := range req.SuspectChunks {
			if !ids.ValidChunkID(chunkID) {
				s.log.Warn("scrub_suspect_rejected", "worker_id", req.WorkerID, "chunk_id", chunkID, "reason", "invalid_id")
				continue
			}
			changed, err := s.cfg.Meta.MarkSuspectCorrupt(r.Context(), chunkID, req.WorkerID)
			if err != nil {
				s.log.Error("scrub_crosscheck_failed", "worker_id", req.WorkerID, "chunk_id", chunkID, "error", err)
				continue
			}
			if changed {
				s.log.Warn("corruption_detected", "worker_id", req.WorkerID, "chunk_id", chunkID, "detected_by", "scrubber")
			}
		}
	}
	action := wire.ActionOK
	if reregister {
		action = wire.ActionReregister
	}
	httpx.WriteJSON(w, http.StatusOK, wire.HeartbeatResponse{Action: action})
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Meta != nil {
		if err := s.cfg.Meta.Ping(r.Context()); err != nil {
			s.log.Error("healthz_db_failed", "error", err)
			httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeInternal, "metadata database unavailable")
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// uploadObject implements POST /api/v1/objects (architecture §4.1).
func (s *Server) uploadObject(w http.ResponseWriter, r *http.Request) {
	select {
	case s.uploadSem <- struct{}{}:
		defer func() { <-s.uploadSem }()
	default:
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeServerBusy,
			fmt.Sprintf("too many concurrent uploads (limit %d); retry later", cap(s.uploadSem)))
		return
	}
	mr, err := r.MultipartReader()
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, "expected a multipart/form-data body: "+err.Error())
		return
	}
	rc := http.NewResponseController(w)
	// Per-read deadlines guard against stalled clients; clear them afterwards
	// so they do not leak into the next request on a keep-alive connection.
	defer func() { _ = rc.SetReadDeadline(time.Time{}) }()
	obj, err := s.cfg.Uploader.Upload(r.Context(), mr, func(t time.Time) { _ = rc.SetReadDeadline(t) })
	if err != nil {
		var ue *upload.Error
		switch {
		case errors.As(err, &ue) && ue.Status != http.StatusInternalServerError:
			// 4xx and 503: the message is safe and useful to show the client.
			if ue.ClientGone {
				s.log.Info("upload_client_gone", "error", err)
			}
			// 400s explain the client's own request; 503s carry only the
			// summary, since their underlying errors can name workers, and
			// clients never learn chunk locations (spec §5.2).
			msg := ue.Message
			if ue.Status < 500 {
				msg = ue.Error()
			} else {
				s.log.Warn("upload_unavailable", "code", ue.Code, "error", err)
			}
			httpx.WriteError(w, ue.Status, ue.Code, msg)
		case r.Context().Err() != nil:
			s.log.Info("upload_client_gone", "error", err)
		default:
			s.log.Error("upload_failed", "error", err)
			httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		}
		return
	}
	w.Header().Set("Location", "/api/v1/objects/"+obj.ID)
	httpx.WriteJSON(w, http.StatusCreated, objectInfo(obj))
}

// downloadObject implements GET and HEAD /api/v1/objects/{object_id}
// (architecture §4.2).
func (s *Server) downloadObject(w http.ResponseWriter, r *http.Request) {
	select {
	case s.dlSem <- struct{}{}:
		defer func() { <-s.dlSem }()
	default:
		httpx.WriteError(w, http.StatusServiceUnavailable, wire.CodeServerBusy,
			fmt.Sprintf("too many concurrent downloads (limit %d); retry later", cap(s.dlSem)))
		return
	}
	s.cfg.Downloader.ServeObject(w, r, r.PathValue("object_id"))
}

func objectInfo(o meta.Object) wire.ObjectInfo {
	md := o.CustomMetadata
	if md == nil {
		md = map[string]string{}
	}
	return wire.ObjectInfo{
		ObjectID:        o.ID,
		Name:            o.Name,
		Size:            o.Size,
		ContentType:     o.ContentType,
		ChecksumSHA256:  o.ChecksumSHA256,
		NumChunks:       o.NumChunks,
		CreatedAt:       o.CreatedAt.Format(time.RFC3339Nano),
		ModifiedAt:      o.ModifiedAt.Format(time.RFC3339Nano),
		MetadataVersion: o.MetadataVersion,
		CustomMetadata:  md,
	}
}

func (s *Server) cluster(w http.ResponseWriter, r *http.Request) {
	v := s.ClusterView()
	if s.cfg.Meta != nil {
		h, err := s.cfg.Meta.ChunkHealth(r.Context(), s.aliveIDs(), s.cfg.ReplicationFactor)
		if err != nil {
			s.log.Error("cluster_storage_failed", "error", err)
			httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
			return
		}
		v.Config.ReplicationFactor = s.cfg.ReplicationFactor
		v.Storage = &wire.StorageView{
			Chunks: wire.ChunkTotals{Total: h.Total, FullyReplicated: h.FullyReplicated,
				UnderReplicated: h.UnderReplicated, ZeroEffective: h.ZeroEffective, OverReplicated: h.OverReplicated},
			ObjectsByStatus: h.ObjectsByStatus,
			ReplicasByState: h.ReplicasByState,
			PendingDeletes:  h.PendingDeletes,
		}
	}
	if s.cfg.Repair != nil {
		st := s.cfg.Repair.Stats()
		rv := &wire.RepairView{InFlight: st.InFlight, Completed: st.Completed, Failed: st.Failed,
			Trimmed: st.Trimmed, BytesCopied: st.BytesCopied, WaitingForGrace: st.WaitingForGrace,
			Unrecoverable: st.Unrecoverable, Rounds: st.Rounds, LastRoundDuration: st.LastRoundDuration.Milliseconds()}
		if !st.LastRoundAt.IsZero() {
			at := st.LastRoundAt.UTC().Format(time.RFC3339Nano)
			rv.LastRoundAt = &at
		}
		v.Repair = rv
	}
	httpx.WriteJSON(w, http.StatusOK, v)
}

func (s *Server) aliveIDs() []string {
	alive := s.reg.Alive()
	ids := make([]string, len(alive))
	for i, w := range alive {
		ids[i] = w.ID
	}
	return ids
}

// audit implements GET /admin/audit (architecture §5.9): invariants checked
// against live metadata. Always 200; "ok" reports the safety invariants,
// "steady_state" reports I5.
func (s *Server) audit(w http.ResponseWriter, r *http.Request) {
	rep, err := s.cfg.Meta.Audit(r.Context(), s.aliveIDs(), s.cfg.ReplicationFactor)
	if err != nil {
		s.log.Error("audit_failed", "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		return
	}
	if s.cfg.Inventories != nil {
		invs, err := s.cfg.Inventories(r.Context(), s.reg.Alive())
		if err != nil {
			s.log.Error("audit_inventory_failed", "error", err)
			httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
			return
		}
		check, err := s.cfg.Meta.AuditInventory(r.Context(), invs)
		if err != nil {
			s.log.Error("audit_inventory_failed", "error", err)
			httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
			return
		}
		rep.Checks = append(rep.Checks, check)
		if check.Violations > 0 {
			rep.OK = false
		}
	}
	if !rep.OK {
		s.log.Error("audit_violation", "checks", rep.Checks)
	}
	httpx.WriteJSON(w, http.StatusOK, rep)
}

// ClusterView builds the /cluster snapshot.
func (s *Server) ClusterView() wire.ClusterView {
	now := s.reg.Now()
	workers := s.reg.Snapshot()
	v := wire.ClusterView{
		Controller: wire.ControllerInfo{Version: version.Version, StartedAt: s.startedAt.UTC().Format(time.RFC3339Nano)},
		Config: wire.ClusterConfig{
			HeartbeatIntervalMs: s.cfg.HeartbeatInterval.Milliseconds(),
			DeadAfterMs:         s.cfg.DeadAfter.Milliseconds(),
			RepairGraceMs:       s.cfg.RepairGrace.Milliseconds(),
		},
		Workers: make([]wire.WorkerView, 0, len(workers)),
	}
	for _, wk := range workers {
		view := wire.WorkerView{
			WorkerID:           wk.ID,
			Address:            wk.Address,
			State:              string(wk.State),
			Epoch:              wk.Epoch,
			LastHeartbeatAgeMs: now.Sub(wk.LastHeartbeat).Milliseconds(),
			RegisteredAt:       wk.RegisteredAt.UTC().Format(time.RFC3339Nano),
			RepairEligible:     wk.RepairEligible(now, s.cfg.RepairGrace),
			FreeBytes:          wk.Capacity.FreeBytes,
			UsedBytes:          wk.Capacity.UsedBytes,
			ChunkCount:         wk.Capacity.ChunkCount,
		}
		if wk.State == registry.Dead {
			ds := wk.DeadSince.UTC().Format(time.RFC3339Nano)
			view.DeadSince = &ds
		}
		v.Workers = append(v.Workers, view)
		v.Summary.Total++
		switch wk.State {
		case registry.Alive:
			v.Summary.Alive++
		case registry.Syncing:
			v.Summary.Syncing++
		case registry.Dead:
			v.Summary.Dead++
		}
	}
	return v
}
