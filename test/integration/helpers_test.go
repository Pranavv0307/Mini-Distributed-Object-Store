package integration

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// faults injects failures in front of a worker's HTTP handler. All fields
// may be changed while the worker runs. Only chunk PUTs are affected;
// GET, DELETE, inventory, and healthz pass through.
type faults struct {
	failPuts      atomic.Bool  // answer every PUT 503 without storing
	failAfterPuts atomic.Int64 // if > 0: PUTs succeed this many times, then failPuts turns on
	putsSeen      atomic.Int64
	corruptPuts   atomic.Bool  // flip a byte of the PUT body (worker rejects: checksum)
	slowResponse  atomic.Int64 // nanoseconds: store the chunk, then delay the response
	putLatency    atomic.Int64 // nanoseconds: delay before handling a PUT (simulated network/disk latency)
}

func (f *faults) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			next.ServeHTTP(w, r)
			return
		}
		if d := time.Duration(f.putLatency.Load()); d > 0 {
			time.Sleep(d)
		}
		n := f.putsSeen.Add(1)
		if limit := f.failAfterPuts.Load(); limit > 0 && n > limit {
			f.failPuts.Store(true)
		}
		if f.failPuts.Load() {
			http.Error(w, `{"error":{"code":"INTERNAL","message":"injected failure"}}`, http.StatusServiceUnavailable)
			return
		}
		if f.corruptPuts.Load() {
			body, _ := io.ReadAll(r.Body)
			if len(body) > 0 {
				body[len(body)/2] ^= 0xff
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		if d := time.Duration(f.slowResponse.Load()); d > 0 {
			// The chunk IS stored; only the answer is late. The controller
			// times out and must not assume the write failed.
			rec := httptest.NewRecorder()
			next.ServeHTTP(rec, r)
			select {
			case <-time.After(d):
			case <-r.Context().Done():
			}
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// part is one multipart form part.
type part struct {
	field, filename, contentType string
	data                         []byte
}

func filePart(name string, data []byte) part {
	return part{field: "file", filename: name, contentType: "application/octet-stream", data: data}
}

func metaPart(json string) part { return part{field: "metadata", data: []byte(json)} }

// multipartBody encodes parts; it returns the Content-Type header and body.
func multipartBody(t testing.TB, parts ...part) (string, []byte) {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, p := range parts {
		h := textproto.MIMEHeader{}
		disp := `form-data; name="` + p.field + `"`
		if p.filename != "" {
			disp += `; filename="` + p.filename + `"`
		}
		h.Set("Content-Disposition", disp)
		if p.contentType != "" {
			h.Set("Content-Type", p.contentType)
		}
		pw, err := mw.CreatePart(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pw.Write(p.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.Close(); err != nil {
		t.Fatal(err)
	}
	return mw.FormDataContentType(), buf.Bytes()
}

// truncate cuts a multipart body in the middle of its file data, i.e. before
// the closing boundary, as if the client disconnected mid-upload. Callers
// must pass a keep that falls inside the file data.
func truncate(t testing.TB, body []byte, keep int) []byte {
	t.Helper()
	// The closing boundary is the last "\r\n--" in the body; cutting before it
	// (and after the part headers) leaves a part with no terminating boundary.
	closing := strings.LastIndex(string(body), "\r\n--")
	headersEnd := strings.Index(string(body), "\r\n\r\n")
	if keep <= headersEnd || keep >= closing {
		t.Fatalf("truncate(%d) of a %d-byte body would not cut inside the file data", keep, len(body))
	}
	return body[:keep]
}
