// Package ids defines and validates the identifiers used across the system:
// object IDs, chunk IDs, and worker IDs.
//
// Chunk IDs become file names on worker disks, so validation is deliberately
// strict: a closed character set and a fixed shape make path traversal
// ("../", "/", "\") impossible by construction.
package ids

import (
	"crypto/rand"
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// IndexDigits is the zero-padded width of the chunk index in a chunk ID.
const IndexDigits = 6

// MaxChunkIndex is the largest index representable in a chunk ID.
const MaxChunkIndex = 999_999

// ErrInvalid is returned (wrapped) for any malformed identifier.
var ErrInvalid = errors.New("invalid identifier")

const uuidV4 = `[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`

var (
	objectIDRe = regexp.MustCompile(`^` + uuidV4 + `$`)
	chunkIDRe  = regexp.MustCompile(`^(` + uuidV4 + `)_([0-9]{6})$`)
	workerIDRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
)

// NewObjectID returns a random, canonical (lowercase) UUIDv4.
func NewObjectID() string {
	var b [16]byte
	// crypto/rand.Read never returns an error on supported platforms (Go ≥ 1.24
	// crashes the program instead), so the error is intentionally not handled.
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 9562 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ValidObjectID reports whether s is a canonical lowercase UUIDv4.
func ValidObjectID(s string) bool { return objectIDRe.MatchString(s) }

// ValidChunkID reports whether s has the form {object_id}_{idx:06d}.
func ValidChunkID(s string) bool { return chunkIDRe.MatchString(s) }

// ValidWorkerID reports whether s is an acceptable worker identity: lowercase
// letters, digits, '-' and '_', starting with a letter or digit, ≤ 63 chars.
func ValidWorkerID(s string) bool { return workerIDRe.MatchString(s) }

// ChunkID builds the chunk ID for chunk idx of objectID.
func ChunkID(objectID string, idx int) (string, error) {
	if !ValidObjectID(objectID) {
		return "", fmt.Errorf("%w: object id %q", ErrInvalid, objectID)
	}
	if idx < 0 || idx > MaxChunkIndex {
		return "", fmt.Errorf("%w: chunk index %d out of range [0, %d]", ErrInvalid, idx, MaxChunkIndex)
	}
	return fmt.Sprintf("%s_%0*d", objectID, IndexDigits, idx), nil
}

// ParseChunkID splits a chunk ID into its object ID and index. The index in a
// chunk ID is informational only: chunk order always comes from metadata.
func ParseChunkID(s string) (objectID string, idx int, err error) {
	m := chunkIDRe.FindStringSubmatch(s)
	if m == nil {
		return "", 0, fmt.Errorf("%w: chunk id %q", ErrInvalid, s)
	}
	idx, err = strconv.Atoi(m[2])
	if err != nil { // unreachable: the regexp guarantees six digits
		return "", 0, fmt.Errorf("%w: chunk id %q: %v", ErrInvalid, s, err)
	}
	return m[1], idx, nil
}
