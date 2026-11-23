package api

import (
	_ "embed"
	"net/http"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/controller/registry"
	"objstore/internal/httpx"
	"objstore/internal/wire"
)

//go:embed dashboard.html
var dashboardHTML []byte

type dashboardSnapshot struct {
	GeneratedAt string            `json:"generated_at"`
	Cluster     wire.ClusterView  `json:"cluster"`
	Audit       meta.AuditReport  `json:"audit"`
	Workers     []dashboardWorker `json:"workers"`
	Objects     []dashboardObject `json:"objects"`
}

type dashboardWorker struct {
	Worker         wire.WorkerView      `json:"worker"`
	Inventory      []dashboardInventory `json:"inventory"`
	InventoryError string               `json:"inventory_error,omitempty"`
}

type dashboardInventory struct {
	ChunkID string `json:"chunk_id"`
	Size    int64  `json:"size"`
}

type dashboardObject struct {
	Info   wire.ObjectInfo  `json:"info"`
	Chunks []wire.ChunkView `json:"chunks"`
}

func (s *Server) dashboardPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/dashboard" {
		http.Redirect(w, r, "/dashboard/", http.StatusTemporaryRedirect)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(dashboardHTML)
}

func (s *Server) dashboardData(w http.ResponseWriter, r *http.Request) {
	objects, err := s.cfg.Meta.DashboardObjects(r.Context())
	if err != nil {
		s.log.Error("dashboard_metadata_failed", "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "dashboard metadata unavailable")
		return
	}

	snapshot := dashboardSnapshot{
		GeneratedAt: s.reg.Now().UTC().Format(time.RFC3339Nano),
		Cluster:     s.ClusterView(),
		Workers:     make([]dashboardWorker, 0),
		Objects:     make([]dashboardObject, 0, len(objects)),
	}
	audit, auditErr := s.cfg.Meta.Audit(r.Context(), s.aliveIDs(), s.cfg.ReplicationFactor)
	if auditErr != nil {
		s.log.Error("dashboard_audit_failed", "error", auditErr)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "dashboard audit unavailable")
		return
	}
	snapshot.Audit = audit
	for _, worker := range s.reg.Snapshot() {
		view := s.workerView(worker)
		dw := dashboardWorker{Worker: view, Inventory: []dashboardInventory{}}
		if s.cfg.Inventory != nil && worker.State != registry.Dead {
			inventory, invErr := s.cfg.Inventory(r.Context(), worker)
			if invErr != nil {
				dw.InventoryError = invErr.Error()
			} else {
				dw.Inventory = make([]dashboardInventory, len(inventory))
				for i, item := range inventory {
					dw.Inventory[i] = dashboardInventory{ChunkID: item.ChunkID, Size: item.Size}
				}
			}
		} else if worker.State == registry.Dead {
			dw.InventoryError = "worker is DEAD; inventory unavailable"
		}
		snapshot.Workers = append(snapshot.Workers, dw)
	}
	for _, object := range objects {
		do := dashboardObject{Info: objectInfo(object.Object), Chunks: make([]wire.ChunkView, len(object.Chunks))}
		for i, chunk := range object.Chunks {
			do.Chunks[i] = wire.ChunkView{
				Index: chunk.Idx, ChunkID: chunk.ID, Size: chunk.Size,
				ChecksumSHA256: chunk.ChecksumSHA256,
				Replicas:       make([]wire.ReplicaView, len(chunk.Replicas)),
			}
			for j, replica := range chunk.Replicas {
				do.Chunks[i].Replicas[j] = wire.ReplicaView{WorkerID: replica.WorkerID, State: replica.State}
			}
		}
		snapshot.Objects = append(snapshot.Objects, do)
	}
	httpx.WriteJSON(w, http.StatusOK, snapshot)
}

func (s *Server) workerView(worker registry.Worker) wire.WorkerView {
	now := s.reg.Now()
	view := wire.WorkerView{
		WorkerID: worker.ID, Address: worker.Address, State: string(worker.State),
		Epoch: worker.Epoch, LastHeartbeatAgeMs: now.Sub(worker.LastHeartbeat).Milliseconds(),
		RegisteredAt:   worker.RegisteredAt.UTC().Format(time.RFC3339Nano),
		RepairEligible: worker.RepairEligible(now, s.cfg.RepairGrace),
		FreeBytes:      worker.Capacity.FreeBytes, UsedBytes: worker.Capacity.UsedBytes,
		ChunkCount: worker.Capacity.ChunkCount,
	}
	if worker.State == registry.Dead {
		deadSince := worker.DeadSince.UTC().Format(time.RFC3339Nano)
		view.DeadSince = &deadSince
	}
	return view
}
