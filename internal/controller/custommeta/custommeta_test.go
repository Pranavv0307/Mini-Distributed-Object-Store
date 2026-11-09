package custommeta

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseUploadAccepts(t *testing.T) {
	for raw, want := range map[string]int{
		`{"owner":"alice","project":"x"}`: 2,
		`{}`:                              0,
		``:                                0,
		"  \n ":                           0,
		`{"emoji":"✓"}`:                   1,
	} {
		m, err := ParseUpload([]byte(raw))
		if err != nil || len(m) != want {
			t.Errorf("ParseUpload(%q) = %v, %v; want %d keys", raw, m, err, want)
		}
	}
}

func TestParseUploadRejects(t *testing.T) {
	many := make([]string, MaxKeys+1)
	for i := range many {
		many[i] = fmt.Sprintf(`"k%d":"v"`, i)
	}
	cases := map[string]error{
		`[1,2]`:                             ErrInvalid,
		`"str"`:                             ErrInvalid,
		`null`:                              ErrInvalid,
		`{"a":1}`:                           ErrInvalid,
		`{"a":true}`:                        ErrInvalid,
		`{"a":null}`:                        ErrInvalid,
		`{"a":{"b":"c"}}`:                   ErrInvalid,
		`{"a":"b"} {"c":"d"}`:               ErrInvalid,
		`{"":"v"}`:                          ErrInvalid,
		`{not json`:                         ErrInvalid,
		`{` + strings.Join(many, ",") + `}`: ErrLimit,
		`{"` + strings.Repeat("k", MaxKeyBytes+1) + `":"v"}`:    ErrLimit,
		`{"k":"` + strings.Repeat("v", MaxValueBytes+1) + `"}`:  ErrLimit,
		`{"k":"` + strings.Repeat("v", MaxDocumentBytes) + `"}`: ErrLimit,
	}
	for raw, want := range cases {
		if _, err := ParseUpload([]byte(raw)); !errors.Is(err, want) {
			t.Errorf("ParseUpload(%.40q) err = %v, want %v", raw, err, want)
		}
	}
}

func TestLimitsAreInclusive(t *testing.T) {
	m := map[string]string{strings.Repeat("k", MaxKeyBytes): strings.Repeat("v", MaxValueBytes)}
	if err := Validate(m); err != nil {
		t.Fatalf("max-size key and value rejected: %v", err)
	}
}

func TestParsePatchAndMerge(t *testing.T) {
	p, err := ParsePatch([]byte(`{"a":"1","b":null}`))
	if err != nil || *p["a"] != "1" || p["b"] != nil || len(p) != 2 {
		t.Fatalf("ParsePatch = %v, %v", p, err)
	}
	m, err := Merge(map[string]string{"b": "x", "c": "y"}, p)
	if err != nil || len(m) != 2 || m["a"] != "1" || m["c"] != "y" {
		t.Fatalf("Merge = %v, %v", m, err)
	}
	for _, bad := range []string{`[]`, `null`, `{"a":1}`, `{"a":{}}`, `{"a":"b"}x`, ``} {
		if _, err := ParsePatch([]byte(bad)); !errors.Is(err, ErrInvalid) {
			t.Errorf("ParsePatch(%q) err = %v", bad, err)
		}
	}
	// Limits apply to the merged result.
	cur := map[string]string{}
	for i := range MaxKeys {
		cur[fmt.Sprint(i)] = "v"
	}
	if _, err := Merge(cur, map[string]*string{"extra": ptr("v")}); !errors.Is(err, ErrLimit) {
		t.Fatalf("51st key: %v", err)
	}
	if _, err := Merge(map[string]string{"k": "v"}, map[string]*string{"": ptr("v")}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty key: %v", err)
	}
}

func ptr(s string) *string { return &s }
