package ids

import (
	"errors"
	"strings"
	"testing"
)

const goodObj = "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"

func TestNewObjectIDIsValidAndUnique(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id := NewObjectID()
		if !ValidObjectID(id) {
			t.Fatalf("NewObjectID() = %q, not a valid object ID", id)
		}
		if seen[id] {
			t.Fatalf("duplicate object ID %q", id)
		}
		seen[id] = true
	}
}

func TestValidObjectID(t *testing.T) {
	cases := map[string]bool{
		goodObj:                                 true,
		strings.ToUpper(goodObj):                false, // must be canonical lowercase
		"3f2b8c1e-9a4d-1e2f-8b7a-1c2d3e4f5a6b":  false, // version 1, not 4
		"3f2b8c1e-9a4d-4e2f-7b7a-1c2d3e4f5a6b":  false, // wrong variant
		"3f2b8c1e9a4d4e2f8b7a1c2d3e4f5a6b":      false, // no hyphens
		"":                                      false,
		goodObj + "0":                           false,
		"../../3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4": false,
	}
	for in, want := range cases {
		if got := ValidObjectID(in); got != want {
			t.Errorf("ValidObjectID(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestValidChunkID(t *testing.T) {
	cases := map[string]bool{
		goodObj + "_000000":         true,
		goodObj + "_999999":         true,
		goodObj + "_00000":          false, // 5 digits
		goodObj + "_0000000":        false, // 7 digits
		goodObj + "-000000":         false,
		goodObj + "_00000a":         false,
		"../" + goodObj + "_000000": false,
		goodObj + "_000000/..":      false,
		goodObj + "_000000\x00":     false,
		"..":                        false,
		"a/b":                       false,
		`a\b`:                       false,
		"":                          false,
	}
	for in, want := range cases {
		if got := ValidChunkID(in); got != want {
			t.Errorf("ValidChunkID(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestChunkIDRoundTrip(t *testing.T) {
	for _, idx := range []int{0, 1, 42, MaxChunkIndex} {
		id, err := ChunkID(goodObj, idx)
		if err != nil {
			t.Fatalf("ChunkID(%d): %v", idx, err)
		}
		if !ValidChunkID(id) {
			t.Fatalf("ChunkID(%d) = %q is not valid", idx, id)
		}
		obj, got, err := ParseChunkID(id)
		if err != nil || obj != goodObj || got != idx {
			t.Fatalf("ParseChunkID(%q) = %q, %d, %v", id, obj, got, err)
		}
	}
	if id, _ := ChunkID(goodObj, 7); id != goodObj+"_000007" {
		t.Fatalf("ChunkID zero padding: got %q", id)
	}
}

func TestChunkIDRejectsBadInput(t *testing.T) {
	for _, idx := range []int{-1, MaxChunkIndex + 1} {
		if _, err := ChunkID(goodObj, idx); !errors.Is(err, ErrInvalid) {
			t.Errorf("ChunkID(idx=%d) err = %v, want ErrInvalid", idx, err)
		}
	}
	if _, err := ChunkID("nope", 0); !errors.Is(err, ErrInvalid) {
		t.Errorf("ChunkID(bad object) err = %v, want ErrInvalid", err)
	}
	if _, _, err := ParseChunkID("nope"); !errors.Is(err, ErrInvalid) {
		t.Errorf("ParseChunkID(bad) err = %v, want ErrInvalid", err)
	}
}

func TestValidWorkerID(t *testing.T) {
	cases := map[string]bool{
		"worker1":               true,
		"w":                     true,
		"worker-2_a":            true,
		"Worker1":               false,
		"-worker":               false,
		"":                      false,
		"worker/1":              false,
		"..":                    false,
		strings.Repeat("a", 63): true,
		strings.Repeat("a", 64): false,
	}
	for in, want := range cases {
		if got := ValidWorkerID(in); got != want {
			t.Errorf("ValidWorkerID(%q) = %v, want %v", in, got, want)
		}
	}
}
