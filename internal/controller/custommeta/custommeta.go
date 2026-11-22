// Package custommeta validates user-defined object metadata (DEC-013): a
// flat JSON object of string keys to string values, bounded in size to
// protect the metadata database (architecture §5.7).
package custommeta

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// Limits (architecture §5.7).
const (
	MaxKeys       = 50
	MaxKeyBytes   = 128
	MaxValueBytes = 1024
	// MaxDocumentBytes bounds the raw JSON document (an upload's metadata part
	// or a PATCH body): 50 × (128 + 1024) plus JSON overhead fits easily.
	MaxDocumentBytes = 64 << 10
)

// ErrInvalid: not a flat JSON object of string values.
var ErrInvalid = errors.New("invalid custom metadata")

// ErrLimit: too many keys, or a key or value too long.
var ErrLimit = errors.New("custom metadata limit exceeded")

// ParseUpload parses the metadata part of an upload. Every value must be a
// string (null is not allowed here; it is only a deletion marker in PATCH).
func ParseUpload(raw []byte) (map[string]string, error) {
	if len(raw) > MaxDocumentBytes {
		return nil, fmt.Errorf("%w: document larger than %d bytes", ErrLimit, MaxDocumentBytes)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]string{}, nil // an empty metadata part means "none"
	}
	var generic map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("%w: must be a JSON object: %v", ErrInvalid, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: trailing data after the JSON object", ErrInvalid)
	}
	if generic == nil {
		return nil, fmt.Errorf("%w: must be a JSON object, not null", ErrInvalid)
	}
	out := make(map[string]string, len(generic))
	for k, v := range generic {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%w: value of %q must be a string, got %s", ErrInvalid, k, jsonType(v))
		}
		out[k] = s
	}
	return out, Validate(out)
}

// Validate checks key/value limits on a complete metadata map.
func Validate(m map[string]string) error {
	if len(m) > MaxKeys {
		return fmt.Errorf("%w: %d keys, at most %d allowed", ErrLimit, len(m), MaxKeys)
	}
	for k, v := range m {
		switch {
		case k == "":
			return fmt.Errorf("%w: keys must not be empty", ErrInvalid)
		case !utf8.ValidString(k) || !utf8.ValidString(v):
			return fmt.Errorf("%w: keys and values must be valid UTF-8", ErrInvalid)
		case len(k) > MaxKeyBytes:
			return fmt.Errorf("%w: key %.20q… is %d bytes, at most %d", ErrLimit, k, len(k), MaxKeyBytes)
		case len(v) > MaxValueBytes:
			return fmt.Errorf("%w: value of %q is %d bytes, at most %d", ErrLimit, k, len(v), MaxValueBytes)
		}
	}
	return nil
}

func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// ParsePatch parses a PATCH body (DEC-014): a JSON object whose values are
// strings (add or update the key) or null (remove the key).
func ParsePatch(raw []byte) (map[string]*string, error) {
	if len(raw) > MaxDocumentBytes {
		return nil, fmt.Errorf("%w: document larger than %d bytes", ErrLimit, MaxDocumentBytes)
	}
	var generic map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("%w: body must be a JSON object: %v", ErrInvalid, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: trailing data after the JSON object", ErrInvalid)
	}
	if generic == nil {
		return nil, fmt.Errorf("%w: body must be a JSON object, not null", ErrInvalid)
	}
	out := make(map[string]*string, len(generic))
	for k, v := range generic {
		switch x := v.(type) {
		case nil:
			out[k] = nil
		case string:
			out[k] = &x
		default:
			return nil, fmt.Errorf("%w: value of %q must be a string or null, got %s", ErrInvalid, k, jsonType(v))
		}
	}
	return out, nil
}

// Merge applies patch to current (which it may modify) and validates the
// result against the limits.
func Merge(current map[string]string, patch map[string]*string) (map[string]string, error) {
	if current == nil {
		current = map[string]string{}
	}
	for k, v := range patch {
		if v == nil {
			delete(current, k)
		} else {
			current[k] = *v
		}
	}
	return current, Validate(current)
}
