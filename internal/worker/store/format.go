package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

// HeaderSize is the size in bytes of the header at the start of every chunk
// file (DEC-012).
//
// Layout (all integers big-endian):
//
//	offset  size  field
//	0       4     magic "OSCK"
//	4       1     format version (1)
//	5       3     reserved, zero
//	8       8     payload size in bytes (uint64)
//	16      32    SHA-256 of the payload
//	48      ...   payload
//
// Keeping the checksum inside the chunk file means one atomic rename installs
// data and checksum together; a sidecar file could tear between two renames.
const HeaderSize = 48

const formatVersion uint8 = 1

var magic = [4]byte{'O', 'S', 'C', 'K'}

// Header is the decoded chunk file header.
type Header struct {
	Version     uint8
	PayloadSize int64
	SHA256      [sha256.Size]byte
}

func (h Header) marshal() []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], magic[:])
	b[4] = h.Version
	binary.BigEndian.PutUint64(b[8:16], uint64(h.PayloadSize))
	copy(b[16:48], h.SHA256[:])
	return b
}

// parseHeader decodes the first HeaderSize bytes of b. Any malformed header
// is reported as ErrCorrupt.
func parseHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, fmt.Errorf("%w: file is %d bytes, shorter than the %d-byte header", ErrCorrupt, len(b), HeaderSize)
	}
	if !bytes.Equal(b[0:4], magic[:]) {
		return Header{}, fmt.Errorf("%w: bad magic %q", ErrCorrupt, b[0:4])
	}
	if b[4] != formatVersion {
		return Header{}, fmt.Errorf("%w: unsupported format version %d", ErrCorrupt, b[4])
	}
	size := binary.BigEndian.Uint64(b[8:16])
	if size > math.MaxInt64 {
		return Header{}, fmt.Errorf("%w: payload size %d out of range", ErrCorrupt, size)
	}
	h := Header{Version: b[4], PayloadSize: int64(size)}
	copy(h.SHA256[:], b[16:48])
	return h, nil
}
