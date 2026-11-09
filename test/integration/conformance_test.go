package integration

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"objstore/internal/wire"
)

// TestSpecConformance walks every row of the specification's client API
// table (§5) and status table (§5.7) and checks status, content type, and
// the JSON error body. It is the M5 "conformance checklist"; docs/API.md
// lists the same rows.
//
// Not covered here: 500 (no deterministic trigger; unit tests cover the
// mapping) and 503 SERVER_BUSY (TestServerBusy).
func TestSpecConformance(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	data := randomBytes(t, 2500)
	unknown := "3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"

	var id string
	type row struct {
		name   string
		do     func() apiResult
		status int
		code   string // expected error code ("" for success)
	}
	ct, body := multipartBody(t, metaPart(`{"owner":"alice"}`), filePart("doc.pdf", data))
	ctNoFile, bodyNoFile := multipartBody(t, metaPart(`{}`))
	rows := []row{
		{"POST /api/v1/objects → 201", func() apiResult {
			r := c.call(http.MethodPost, "/api/v1/objects", body, map[string]string{"Content-Type": ct})
			var info wire.ObjectInfo
			if r.status == 201 {
				r.decode(t, &info)
				id = info.ObjectID
			}
			return r
		}, 201, ""},
		{"POST without file → 400", func() apiResult {
			return c.call(http.MethodPost, "/api/v1/objects", bodyNoFile, map[string]string{"Content-Type": ctNoFile})
		}, 400, wire.CodeMissingFile},
		{"POST not multipart → 400", func() apiResult {
			return c.call(http.MethodPost, "/api/v1/objects", []byte("x"), map[string]string{"Content-Type": "text/plain"})
		}, 400, wire.CodeInvalidRequest},
		{"GET object → 200", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects/"+id, nil, nil) }, 200, ""},
		{"GET object unknown → 404", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects/"+unknown, nil, nil) }, 404, wire.CodeObjectNotFound},
		{"GET object bad id → 400", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects/x", nil, nil) }, 400, wire.CodeInvalidObjectID},
		{"GET metadata → 200", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects/"+id+"/metadata", nil, nil) }, 200, ""},
		{"GET metadata unknown → 404", func() apiResult {
			return c.call(http.MethodGet, "/api/v1/objects/"+unknown+"/metadata", nil, nil)
		}, 404, wire.CodeObjectNotFound},
		{"PATCH metadata → 200", func() apiResult {
			return c.call(http.MethodPatch, "/api/v1/objects/"+id+"/metadata", []byte(`{"owner":"bob"}`), map[string]string{"Content-Type": "application/json"})
		}, 200, ""},
		{"PATCH metadata unknown → 404", func() apiResult {
			return c.call(http.MethodPatch, "/api/v1/objects/"+unknown+"/metadata", []byte(`{"a":"b"}`), nil)
		}, 404, wire.CodeObjectNotFound},
		{"PATCH metadata invalid → 400", func() apiResult {
			return c.call(http.MethodPatch, "/api/v1/objects/"+id+"/metadata", []byte(`{"a":1}`), nil)
		}, 400, wire.CodeInvalidMetadata},
		{"PATCH metadata stale If-Match → 409", func() apiResult {
			return c.call(http.MethodPatch, "/api/v1/objects/"+id+"/metadata", []byte(`{"a":"b"}`), map[string]string{"If-Match": `"v1"`})
		}, 409, wire.CodeVersionConflict},
		{"GET list → 200", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects", nil, nil) }, 200, ""},
		{"GET list bad limit → 400", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects?limit=0", nil, nil) }, 400, wire.CodeInvalidPagination},
		{"DELETE object → 204", func() apiResult { return c.call(http.MethodDelete, "/api/v1/objects/"+id, nil, nil) }, 204, ""},
		{"DELETE object again → 404", func() apiResult { return c.call(http.MethodDelete, "/api/v1/objects/"+id, nil, nil) }, 404, wire.CodeObjectNotFound},
		{"GET deleted object → 404", func() apiResult { return c.call(http.MethodGet, "/api/v1/objects/"+id, nil, nil) }, 404, wire.CodeObjectNotFound},
		{"GET download, no valid copy → 503", func() apiResult {
			id2 := c.mustUpload(data)
			ch := c.chunkMap(id2)[0]
			for _, rep := range ch.Replicas {
				corruptFile(t, c.chunkPath(rep.WorkerID, ch.ID))
			}
			return c.call(http.MethodGet, "/api/v1/objects/"+id2, nil, nil)
		}, 503, wire.CodeStorageUnavailable},
		{"POST with < 2 workers → 503", func() apiResult {
			c.workers[1].stop()
			waitFor(t, clusterLimit, "worker2 DEAD", func() bool { return len(c.reg.Alive()) == 1 })
			return c.call(http.MethodPost, "/api/v1/objects", body, map[string]string{"Content-Type": ct})
		}, 503, wire.CodeInsufficientWorkers},
	}

	var report strings.Builder
	for _, rw := range rows {
		r := rw.do()
		ok := r.status == rw.status
		if ok && rw.code != "" {
			ok = strings.HasPrefix(r.header.Get("Content-Type"), "application/json") &&
				r.err.Error.Code == rw.code && r.err.Error.Message != ""
		}
		if ok && rw.status == 200 && !strings.HasPrefix(rw.name, "GET object") {
			ok = json.Valid(r.body)
		}
		mark := "PASS"
		if !ok {
			mark = "FAIL"
			t.Errorf("%s: got %d %s %q", rw.name, r.status, r.err.Error.Code, r.err.Error.Message)
		}
		report.WriteString(mark + "  " + rw.name + "\n")
	}
	t.Log("\n" + report.String())
}
