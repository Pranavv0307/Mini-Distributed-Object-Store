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
