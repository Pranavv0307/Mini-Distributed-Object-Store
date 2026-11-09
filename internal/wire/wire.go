// Package wire holds the JSON request/response types, HTTP header names, and
// stable error codes shared by the controller, workers, and clients.
// docs/API.md is the human-readable contract for everything defined here.
package wire

// HeaderChunkSHA256 carries a chunk's SHA-256 (64 hex chars): the expected
// checksum on PUT, and the checksum stored in the chunk header on GET.
const HeaderChunkSHA256 = "X-Chunk-SHA256"

// Stable error codes (docs/API.md, DEC-015).
const (
	CodeInvalidRequest      = "INVALID_REQUEST"
	CodeInvalidChunkID      = "INVALID_CHUNK_ID"
	CodeChecksumMismatch    = "CHECKSUM_MISMATCH"
	CodeSizeMismatch        = "SIZE_MISMATCH"
	CodeChunkTooLarge       = "CHUNK_TOO_LARGE"
	CodeChunkNotFound       = "CHUNK_NOT_FOUND"
	CodeChunkCorrupt        = "CHUNK_CORRUPT"
	CodeWorkerNotFound      = "WORKER_NOT_FOUND"
	CodeNotRegistered       = "NOT_REGISTERED"
	CodeInsufficientStorage = "INSUFFICIENT_STORAGE"
	CodeInternal            = "INTERNAL"
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
