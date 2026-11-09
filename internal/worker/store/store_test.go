package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

const (
	testObj    = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"
	testID     = testObj + "_000000"
	maxPayload = 64 << 10
)

func openTest(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, Options{Fsync: true, MaxPayloadSize: maxPayload})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func payload(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

func put(t *testing.T, s *Store, id string, data []byte) bool {
	t.Helper()
	created, err := s.Put(id, int64(len(data)), sha256.Sum256(data), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Put(%s): %v", id, err)
	}
	return created
}

func tmpEntries(t *testing.T, dir string) []os.DirEntry {
	t.Helper()
	es, err := os.ReadDir(filepath.Join(dir, "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	return es
}

func TestPutGetRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 4096, maxPayload} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir := t.TempDir()
			s := openTest(t, dir)
			data := payload(n, 3)
			if !put(t, s, testID, data) {
				t.Fatal("first Put reported created=false")
			}
			c, err := s.Get(testID)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !bytes.Equal(c.Payload, data) {
				t.Fatal("payload mismatch")
			}
			if c.Header.PayloadSize != int64(n) || c.Header.SHA256 != sha256.Sum256(data) {
				t.Fatalf("header = %+v", c.Header)
			}
			if len(tmpEntries(t, dir)) != 0 {
				t.Fatal("temp files left behind after successful Put")
			}
		})
	}
}

func TestPutIsIdempotentAndReplaces(t *testing.T) {
	s := openTest(t, t.TempDir())
	data := payload(1000, 1)
	if !put(t, s, testID, data) {
		t.Fatal("first Put: created=false")
	}
	if put(t, s, testID, data) {
		t.Fatal("second identical Put: created=true, want false")
	}
	other := payload(500, 9)
	put(t, s, testID, other) // a different valid write replaces the file
	c, err := s.Get(testID)
	if err != nil || !bytes.Equal(c.Payload, other) {
		t.Fatalf("after replacement: err=%v, payload equal=%v", err, bytes.Equal(c.Payload, other))
	}
}

func TestPutRejectsBadInputWithoutSideEffects(t *testing.T) {
	data := payload(1000, 1)
	good := sha256.Sum256(data)
	cases := []struct {
		name string
		id   string
		size int64
		sum  [32]byte
		body io.Reader
		want error
	}{
		{"checksum mismatch", testID, 1000, sha256.Sum256([]byte("x")), bytes.NewReader(data), ErrChecksumMismatch},
		{"body too short", testID, 1001, good, bytes.NewReader(data), ErrSizeMismatch},
		{"body too long", testID, 999, good, bytes.NewReader(data), ErrSizeMismatch},
		{"unexpected EOF", testID, 1000, good, io.MultiReader(bytes.NewReader(data[:10]), errReader{io.ErrUnexpectedEOF}), ErrSizeMismatch},
		{"too large", testID, maxPayload + 1, good, bytes.NewReader(data), ErrTooLarge},
		{"traversal id", "../" + testID, 1000, good, bytes.NewReader(data), ErrInvalidID},
		{"bad id", "x", 1000, good, bytes.NewReader(data), ErrInvalidID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := openTest(t, dir)
			_, err := s.Put(tc.id, tc.size, tc.sum, tc.body)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if _, err := s.Get(testID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get after rejected Put: err = %v, want ErrNotFound", err)
			}
			if es := tmpEntries(t, dir); len(es) != 0 {
				t.Fatalf("temp files left behind: %v", es)
			}
		})
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestFailedPutKeepsPreviousVersion(t *testing.T) {
	s := openTest(t, t.TempDir())
	data := payload(100, 1)
	put(t, s, testID, data)
	bad := payload(100, 2)
	if _, err := s.Put(testID, 100, sha256.Sum256(data), bytes.NewReader(bad)); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("err = %v, want checksum mismatch", err)
	}
	c, err := s.Get(testID)
	if err != nil || !bytes.Equal(c.Payload, data) {
		t.Fatal("rejected Put damaged the existing chunk")
	}
}

func TestDeleteIsIdempotent(t *testing.T) {
	s := openTest(t, t.TempDir())
	put(t, s, testID, payload(10, 1))
	if existed, err := s.Delete(testID); err != nil || !existed {
		t.Fatalf("first Delete = %v, %v", existed, err)
	}
	if existed, err := s.Delete(testID); err != nil || existed {
		t.Fatalf("second Delete = %v, %v; want false, nil", existed, err)
	}
	if _, err := s.Get(testID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: %v", err)
	}
	if _, err := s.Delete("../etc"); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Delete(bad id) err = %v", err)
	}
}

// TestCorruptionIsVisible checks that the store never hides corruption: the
// bytes it returns are what is on disk, so the controller's checksum catches
// appended, truncated, or flipped bytes, and a bad header is ErrCorrupt.
func TestCorruptionIsVisible(t *testing.T) {
	data := payload(1000, 5)
	sum := sha256.Sum256(data)
	tests := []struct {
		name     string
		mutate   func(path string) error
		wantErr  error
		wantSize int64 // payload length returned by Get / reported by Inventory
	}{
		{"appended bytes", func(p string) error { return appendFile(p, []byte("garbage")) }, nil, 1007},
		{"truncated", func(p string) error { return os.Truncate(p, HeaderSize+600) }, nil, 600},
		{"flipped byte", func(p string) error { return flipByte(p, HeaderSize+10) }, nil, 1000},
		{"bad magic", func(p string) error { return flipByte(p, 0) }, ErrCorrupt, 1000},
		{"shorter than header", func(p string) error { return os.Truncate(p, 10) }, ErrCorrupt, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			s := openTest(t, dir)
			put(t, s, testID, data)
			if err := tc.mutate(filepath.Join(dir, "chunks", testID)); err != nil {
				t.Fatal(err)
			}
			c, err := s.Get(testID)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Get err = %v, want %v", err, tc.wantErr)
				}
			} else {
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if int64(len(c.Payload)) != tc.wantSize {
					t.Fatalf("payload len = %d, want %d", len(c.Payload), tc.wantSize)
				}
				if sha256.Sum256(c.Payload) == sum {
					t.Fatal("corrupted payload still hashes to the original checksum")
				}
			}
			inv, err := s.Inventory()
			if err != nil || len(inv) != 1 || inv[0].Size != tc.wantSize {
				t.Fatalf("Inventory = %+v, %v; want size %d", inv, err, tc.wantSize)
			}
			rep, err := Inspect(filepath.Join(dir, "chunks", testID))
			if err != nil || rep.Valid {
				t.Fatalf("Inspect = %+v, %v; want invalid", rep, err)
			}
		})
	}
}

func TestGetCapsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	put(t, s, testID, payload(10, 1))
	if err := appendFile(filepath.Join(dir, "chunks", testID), make([]byte, 3*maxPayload)); err != nil {
		t.Fatal(err)
	}
	c, err := s.Get(testID)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(c.Payload)) != maxPayload+1 {
		t.Fatalf("payload len = %d, want cap %d", len(c.Payload), maxPayload+1)
	}
}

func TestOpenRemovesTempFilesAndKeepsChunks(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	data := payload(100, 1)
	put(t, s, testID, data)
	// Simulate a crash mid-write: a partial temp file for another chunk.
	partial := filepath.Join(dir, "tmp", testObj+"_000001.123.tmp")
	if err := os.WriteFile(partial, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2 := openTest(t, dir)
	if es := tmpEntries(t, dir); len(es) != 0 {
		t.Fatalf("temp files survived Open: %v", es)
	}
	inv, err := s2.Inventory()
	if err != nil || len(inv) != 1 || inv[0].ChunkID != testID || inv[0].Size != 100 {
		t.Fatalf("Inventory = %+v, %v", inv, err)
	}
	if c, err := s2.Get(testID); err != nil || !bytes.Equal(c.Payload, data) {
		t.Fatal("committed chunk lost across reopen")
	}
}

func TestInventoryIgnoresForeignFiles(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	put(t, s, testObj+"_000001", payload(5, 1))
	put(t, s, testObj+"_000000", payload(7, 1))
	if err := os.WriteFile(filepath.Join(dir, "chunks", "README"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "chunks", testObj+"_000009"), 0o755); err != nil {
		t.Fatal(err)
	}
	inv, err := s.Inventory()
	if err != nil {
		t.Fatal(err)
	}
	want := []Entry{{testObj + "_000000", 7}, {testObj + "_000001", 5}}
	if len(inv) != len(want) || inv[0] != want[0] || inv[1] != want[1] {
		t.Fatalf("Inventory = %+v, want %+v", inv, want)
	}
}

// TestConcurrentOperationsSameChunk hammers one chunk ID with writers of two
// different valid payloads, readers, and deleters. Every successful read
// must return one of the two complete payloads with a matching header: never
// a mixture, a partial file, or a header/payload mismatch.
func TestConcurrentOperationsSameChunk(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	a, b := payload(maxPayload, 1), payload(maxPayload/2, 2)
	sumA, sumB := sha256.Sum256(a), sha256.Sum256(b)

	var wg sync.WaitGroup
	errs := make(chan error, 1000)
	for w := range 4 {
		wg.Go(func() {
			for range 25 {
				data := a
				if w%2 == 1 {
					data = b
				}
				if _, err := s.Put(testID, int64(len(data)), sha256.Sum256(data), bytes.NewReader(data)); err != nil {
					errs <- fmt.Errorf("put: %w", err)
				}
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 50 {
				c, err := s.Get(testID)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					errs <- fmt.Errorf("get: %w", err)
					continue
				}
				got := sha256.Sum256(c.Payload)
				if !(got == sumA && c.Header.SHA256 == sumA) && !(got == sumB && c.Header.SHA256 == sumB) {
					errs <- errors.New("read a torn or mismatched chunk")
				}
			}
		})
	}
	wg.Go(func() {
		for range 25 {
			if _, err := s.Delete(testID); err != nil {
				errs <- fmt.Errorf("delete: %w", err)
			}
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if es := tmpEntries(t, dir); len(es) != 0 {
		t.Fatalf("temp files left behind: %d", len(es))
	}
}

func TestOpenValidatesOptions(t *testing.T) {
	if _, err := Open(t.TempDir(), Options{}); err == nil {
		t.Fatal("Open with zero MaxPayloadSize succeeded")
	}
}

func appendFile(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func flipByte(path string, off int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		return err
	}
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], off)
	return err
}
