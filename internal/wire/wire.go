// Package wire holds the JSON request/response types, HTTP header names, and
// stable error codes shared by the controller, workers, and clients.
// docs/API.md is the human-readable contract for everything defined here.
package wire

// HeaderChunkSHA256 carries a chunk's SHA-256 (64 hex chars): the expected
// checksum on PUT, and the checksum stored in the chunk header on GET.
const HeaderChunkSHA256 = "X-Chunk-SHA256"

// HeaderObjectChecksum carries an object's whole-object SHA-256 on download.
const HeaderObjectChecksum = "X-Object-Checksum-SHA256"

// Stable error codes (docs/API.md, DEC-015).
const (
	CodeInvalidRequest         = "INVALID_REQUEST"
	CodeInvalidChunkID         = "INVALID_CHUNK_ID"
	CodeChecksumMismatch       = "CHECKSUM_MISMATCH"
	CodeSizeMismatch           = "SIZE_MISMATCH"
	CodeChunkTooLarge          = "CHUNK_TOO_LARGE"
	CodeChunkNotFound          = "CHUNK_NOT_FOUND"
	CodeChunkCorrupt           = "CHUNK_CORRUPT"
	CodeWorkerNotFound         = "WORKER_NOT_FOUND"
	CodeNotRegistered          = "NOT_REGISTERED"
	CodeInsufficientStorage    = "INSUFFICIENT_STORAGE"
	CodeInternal               = "INTERNAL"
	CodeRegistrationSuperseded = "REGISTRATION_SUPERSEDED"
	CodeMissingFile            = "MISSING_FILE"
	CodeInvalidMetadata        = "INVALID_METADATA"
	CodeMetadataLimitExceeded  = "METADATA_LIMIT_EXCEEDED"
	CodeObjectTooLarge         = "OBJECT_TOO_LARGE"
	CodeInsufficientWorkers    = "INSUFFICIENT_WORKERS"
	CodeUploadFailed           = "UPLOAD_FAILED"
	CodeServerBusy             = "SERVER_BUSY"
	CodeObjectNotFound         = "OBJECT_NOT_FOUND"
	CodeInvalidObjectID        = "INVALID_OBJECT_ID"
	CodeStorageUnavailable     = "STORAGE_UNAVAILABLE"
)

// ErrorResponse is the body of every non-2xx JSON response.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail is a stable machine-readable code plus a human message.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PutChunkResponse is returned by a worker after a chunk PUT.
type PutChunkResponse struct {
	ChunkID        string `json:"chunk_id"`
	Size           int64  `json:"size"`
	ChecksumSHA256 string `json:"checksum_sha256"`
	// Created is true if no file for this chunk existed before the PUT.
	Created bool `json:"created"`
}

// ChunkInfo describes one chunk file in a worker inventory.
type ChunkInfo struct {
	ChunkID string `json:"chunk_id"`
	// Size is the number of payload bytes on disk (file size minus header).
	Size int64 `json:"size"`
}

// InventoryResponse is a worker's list of fully written chunk files.
type InventoryResponse struct {
	WorkerID string      `json:"worker_id"`
	Chunks   []ChunkInfo `json:"chunks"`
}

// WorkerHealth is the body of a worker's GET /healthz.
type WorkerHealth struct {
	Status          string `json:"status"`
	WorkerID        string `json:"worker_id"`
	Registered      bool   `json:"registered"`
	AcceptingWrites bool   `json:"accepting_writes"`
}

// ---- Membership (worker → controller) ---------------------------------------

// RegisterRequest is sent by a worker on every start and whenever the
// controller asks it to re-register. Inventory is a snapshot taken while the
// worker's write gate was closed and drained (DEC-006).
type RegisterRequest struct {
	WorkerID  string      `json:"worker_id"`
	Address   string      `json:"address"`
	FreeBytes int64       `json:"free_bytes"` // -1 if unknown
	UsedBytes int64       `json:"used_bytes"`
	Inventory []ChunkInfo `json:"inventory"`
}

// RegisterResponse is returned once the controller has reconciled the
// inventory and marked the worker ALIVE under Epoch.
type RegisterResponse struct {
	Epoch               uint64 `json:"epoch"`
	HeartbeatIntervalMs int64  `json:"heartbeat_interval_ms"`
}

// HeartbeatRequest reports liveness and capacity. Epoch must be the one from
// the worker's latest successful registration.
type HeartbeatRequest struct {
	WorkerID      string   `json:"worker_id"`
	Epoch         uint64   `json:"epoch"`
	FreeBytes     int64    `json:"free_bytes"` // -1 if unknown
	UsedBytes     int64    `json:"used_bytes"`
	ChunkCount    int64    `json:"chunk_count"`
	SuspectChunks []string `json:"suspect_chunks"` // scrubber findings (Phase 8)
}

// Heartbeat actions (DEC-020).
const (
	ActionOK         = "ok"
	ActionReregister = "reregister"
)

// HeartbeatResponse tells the worker whether its registration is still valid.
type HeartbeatResponse struct {
	Action string `json:"action"`
}

// ---- Observability ------------------------------------------------------------

// ClusterView is the body of the controller's GET /cluster.
type ClusterView struct {
	Controller ControllerInfo `json:"controller"`
	Config     ClusterConfig  `json:"config"`
	Summary    WorkerSummary  `json:"summary"`
	Workers    []WorkerView   `json:"workers"`
}

// ControllerInfo identifies the controller process.
type ControllerInfo struct {
	Version   string `json:"version"`
	StartedAt string `json:"started_at"`
}

// ClusterConfig echoes the liveness settings in effect.
type ClusterConfig struct {
	HeartbeatIntervalMs int64 `json:"heartbeat_interval_ms"`
	DeadAfterMs         int64 `json:"dead_after_ms"`
	RepairGraceMs       int64 `json:"repair_grace_ms"`
}

// WorkerSummary counts workers by state.
type WorkerSummary struct {
	Total   int `json:"total"`
	Alive   int `json:"alive"`
	Syncing int `json:"syncing"`
	Dead    int `json:"dead"`
}

// WorkerView is one worker as the controller sees it.
type WorkerView struct {
	WorkerID string `json:"worker_id"`
	Address  string `json:"address"`
	State    string `json:"state"` // SYNCING | ALIVE | DEAD
	Epoch    uint64 `json:"epoch"`
	// LastHeartbeatAgeMs is measured on the controller's clock (D11).
	LastHeartbeatAgeMs int64   `json:"last_heartbeat_age_ms"`
	RegisteredAt       string  `json:"registered_at"`
	DeadSince          *string `json:"dead_since"`
	// RepairEligible: DEAD for longer than REPAIR_GRACE (D12).
	RepairEligible bool  `json:"repair_eligible"`
	FreeBytes      int64 `json:"free_bytes"`
	UsedBytes      int64 `json:"used_bytes"`
	ChunkCount     int64 `json:"chunk_count"`
}

// ---- Client object API ----------------------------------------------------------

// ObjectInfo describes a committed object (upload response; base of the
// metadata response).
type ObjectInfo struct {
	ObjectID        string            `json:"object_id"`
	Name            string            `json:"name"`
	Size            int64             `json:"size"`
	ContentType     string            `json:"content_type"`
	ChecksumSHA256  string            `json:"checksum_sha256"`
	NumChunks       int               `json:"num_chunks"`
	CreatedAt       string            `json:"created_at"`
	ModifiedAt      string            `json:"modified_at"`
	MetadataVersion int64             `json:"metadata_version"`
	CustomMetadata  map[string]string `json:"custom_metadata"`
}
