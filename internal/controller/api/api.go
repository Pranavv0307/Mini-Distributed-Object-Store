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

	"objstore/internal/controller/registry"
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
	Logger            *slog.Logger
}

// Server serves the controller's HTTP API.
type Server struct {
	reg        *registry.Registry
	reconciler InventoryReconciler
	cfg        Config
	startedAt  time.Time
	log        *slog.Logger
}

// New creates a Server.
func New(reg *registry.Registry, reconciler InventoryReconciler, cfg Config) *Server {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Server{reg: reg, reconciler: reconciler, cfg: cfg, startedAt: reg.Now(), log: logger}
}

// Handler returns the HTTP handler with logging and panic recovery.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/workers/register", s.register)
	mux.HandleFunc("POST /internal/workers/heartbeat", s.heartbeat)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /cluster", s.cluster)
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
	action := wire.ActionOK
	if reregister {
		action = wire.ActionReregister
	}
	httpx.WriteJSON(w, http.StatusOK, wire.HeartbeatResponse{Action: action})
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) cluster(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, s.ClusterView())
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
