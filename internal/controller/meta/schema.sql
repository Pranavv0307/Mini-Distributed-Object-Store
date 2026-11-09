-- Metadata schema, version 1. Applied once by meta.Open (see migrate in meta.go).
-- Times are Unix nanoseconds (UTC) on the controller's clock.

CREATE TABLE objects (
    object_id        TEXT    PRIMARY KEY,
    name             TEXT    NOT NULL,
    size             INTEGER NOT NULL DEFAULT 0,
    content_type     TEXT    NOT NULL,
    checksum_sha256  TEXT    NOT NULL DEFAULT '',   -- set at commit
    num_chunks       INTEGER NOT NULL DEFAULT 0,    -- set at commit
    status           TEXT    NOT NULL CHECK (status IN ('UPLOADING', 'READY', 'DELETING')),
    created_at       INTEGER NOT NULL,
    modified_at      INTEGER NOT NULL,
    last_activity    INTEGER NOT NULL,              -- janitor's stale-upload signal (Phase 9)
    metadata_version INTEGER NOT NULL DEFAULT 1,    -- DEC-014
    custom_metadata  TEXT    NOT NULL DEFAULT '{}'  -- JSON object, string values (DEC-013)
);
CREATE INDEX objects_by_status ON objects (status);
-- Keyset pagination for listing READY objects (Phase 5).
CREATE INDEX objects_ready_by_created ON objects (created_at, object_id) WHERE status = 'READY';

CREATE TABLE chunks (
    chunk_id        TEXT    PRIMARY KEY,
    object_id       TEXT    NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
    idx             INTEGER NOT NULL CHECK (idx >= 0),
    size            INTEGER NOT NULL CHECK (size > 0),
    checksum_sha256 TEXT    NOT NULL,
    UNIQUE (object_id, idx)
);

-- One row per copy. The primary key enforces invariant I3 for every state
-- (never two rows for one chunk on one worker), DEC-008.
CREATE TABLE replicas (
    chunk_id   TEXT    NOT NULL REFERENCES chunks (chunk_id) ON DELETE CASCADE,
    worker_id  TEXT    NOT NULL,
    state      TEXT    NOT NULL CHECK (state IN ('PENDING', 'OK', 'CORRUPT')),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (chunk_id, worker_id)
);
CREATE INDEX replicas_by_worker ON replicas (worker_id);

-- Durable deletion intent (DEC-007). No foreign keys: an entry outlives the
-- chunk and object rows it came from. Mutually exclusive with a replica row
-- for the same (chunk_id, worker_id) (invariant I8), enforced in the claim
-- transactions.
CREATE TABLE pending_deletes (
    chunk_id        TEXT    NOT NULL,
    worker_id       TEXT    NOT NULL,
    object_id       TEXT    NOT NULL,
    reason          TEXT    NOT NULL,
    created_at      INTEGER NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT    NOT NULL DEFAULT '',
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chunk_id, worker_id)
);
CREATE INDEX pending_deletes_by_object ON pending_deletes (object_id);
CREATE INDEX pending_deletes_due ON pending_deletes (next_attempt_at);
