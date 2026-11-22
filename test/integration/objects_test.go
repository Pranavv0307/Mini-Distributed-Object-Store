package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"objstore/internal/controller/meta"
	"objstore/internal/wire"
)

type apiResult struct {
	status int
	header http.Header
	body   []byte
	err    wire.ErrorResponse
}

func (c *uploadCluster) call(method, path string, body []byte, hdr map[string]string) apiResult {
	c.t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, _ := http.NewRequest(method, c.url+path, rdr)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	r := apiResult{status: resp.StatusCode, header: resp.Header, body: raw}
	if resp.StatusCode >= 400 {
		_ = json.Unmarshal(raw, &r.err)
	}
	return r
}

func (r apiResult) decode(t testing.TB, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %q: %v", r.body, err)
	}
}

func (c *uploadCluster) metadata(id string) (wire.ObjectMetadata, apiResult) {
	c.t.Helper()
	r := c.call(http.MethodGet, "/api/v1/objects/"+id+"/metadata", nil, nil)
	var md wire.ObjectMetadata
	if r.status == http.StatusOK {
		r.decode(c.t, &md)
	}
	return md, r
}

func (c *uploadCluster) patch(id, body string, ifMatch string) (wire.ObjectMetadata, apiResult) {
	c.t.Helper()
	h := map[string]string{"Content-Type": "application/json"}
	if ifMatch != "" {
		h["If-Match"] = ifMatch
	}
	r := c.call(http.MethodPatch, "/api/v1/objects/"+id+"/metadata", []byte(body), h)
	var md wire.ObjectMetadata
	if r.status == http.StatusOK {
		r.decode(c.t, &md)
	}
	return md, r
}

// TestMetadataGet: system fields, custom metadata, and the chunk map with
// replica locations (spec §3: "chunk information and storage locations").
func TestMetadataGet(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 4, chunkSize: 1000})
	data := randomBytes(t, 2500)
	up := c.upload(metaPart(`{"owner":"alice"}`), part{field: "file", filename: "a.txt", contentType: "text/plain", data: data})
	md, r := c.metadata(up.info.ObjectID)
	if r.status != 200 || r.header.Get("ETag") != `"v1"` {
		t.Fatalf("status %d, ETag %q", r.status, r.header.Get("ETag"))
	}
	if !reflect.DeepEqual(md.ObjectInfo, up.info) || md.CustomMetadata["owner"] != "alice" {
		t.Fatalf("metadata fields %+v differ from upload response %+v", md.ObjectInfo, up.info)
	}
	if len(md.Chunks) != 3 {
		t.Fatalf("chunks = %d", len(md.Chunks))
	}
	for i, ch := range md.Chunks {
		if ch.Index != i || ch.ChunkID != fmt.Sprintf("%s_%06d", up.info.ObjectID, i) || len(ch.Replicas) != 2 ||
			ch.Replicas[0].WorkerID == ch.Replicas[1].WorkerID || ch.Replicas[0].State != "OK" || ch.ChecksumSHA256 == "" {
			t.Fatalf("chunk %d = %+v", i, ch)
		}
	}
	if md.Chunks[2].Size != 500 {
		t.Fatalf("last chunk size %d", md.Chunks[2].Size)
	}
	// Errors.
	if _, r := c.metadata("nope"); r.status != 400 || r.err.Error.Code != wire.CodeInvalidObjectID {
		t.Fatalf("malformed: %d %+v", r.status, r.err)
	}
	if _, r := c.metadata("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b"); r.status != 404 || r.err.Error.Code != wire.CodeObjectNotFound {
		t.Fatalf("unknown: %d %+v", r.status, r.err)
	}
}

func TestMetadataPatch(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	data := randomBytes(t, 3000)
	up := c.upload(metaPart(`{"owner":"alice","tmp":"x"}`), filePart("f", data))
	id := up.info.ObjectID
	before, _ := c.metadata(id)

	md, r := c.patch(id, `{"owner":"bob","project":"p1","tmp":null}`, "")
	if r.status != 200 || r.header.Get("ETag") != `"v2"` || md.MetadataVersion != 2 {
		t.Fatalf("patch: %d %q %+v", r.status, r.header.Get("ETag"), r.err)
	}
	want := map[string]string{"owner": "bob", "project": "p1"}
	if fmt.Sprint(md.CustomMetadata) != fmt.Sprint(want) {
		t.Fatalf("custom metadata = %v, want %v", md.CustomMetadata, want)
	}
	if md.ModifiedAt == before.ModifiedAt || md.CreatedAt != before.CreatedAt {
		t.Fatalf("timestamps: created %s→%s modified %s→%s", before.CreatedAt, md.CreatedAt, before.ModifiedAt, md.ModifiedAt)
	}
	// PATCH never touches chunks or replicas (M5 checkpoint).
	if fmt.Sprint(md.Chunks) != fmt.Sprint(before.Chunks) || md.Size != before.Size || md.ChecksumSHA256 != before.ChecksumSHA256 {
		t.Fatalf("chunk map or data fields changed:\n%v\n%v", before.Chunks, md.Chunks)
	}
	// Repeating the same PATCH changes nothing (idempotent).
	if again, _ := c.patch(id, `{"owner":"bob"}`, ""); again.MetadataVersion != 2 || again.ModifiedAt != md.ModifiedAt {
		t.Fatalf("no-op patch changed version/time: %+v", again.ObjectInfo)
	}
	// If-Match.
	if _, r := c.patch(id, `{"x":"1"}`, `"v1"`); r.status != 409 || r.err.Error.Code != wire.CodeVersionConflict {
		t.Fatalf("stale If-Match: %d %+v", r.status, r.err)
	}
	if md, r := c.patch(id, `{"x":"1"}`, `"v2"`); r.status != 200 || md.MetadataVersion != 3 {
		t.Fatalf("matching If-Match: %d %+v", r.status, r.err)
	}
	if _, r := c.patch(id, `{"y":"1"}`, `*`); r.status != 200 {
		t.Fatalf("If-Match *: %d", r.status)
	}
	// Validation.
	for body, code := range map[string]string{
		`{"n":1}`:  wire.CodeInvalidMetadata,
		`[]`:       wire.CodeInvalidMetadata,
		`not json`: wire.CodeInvalidMetadata,
		`{"":"v"}`: wire.CodeInvalidMetadata,
		`{"k":"` + strings.Repeat("v", 1025) + `"}`: wire.CodeMetadataLimitExceeded,
	} {
		if _, r := c.patch(id, body, ""); r.status != 400 || r.err.Error.Code != code {
			t.Errorf("patch %.30q: %d %+v, want 400 %s", body, r.status, r.err, code)
		}
	}
	// Content-Type is not checked (DEC-045): curl -d sends form-urlencoded.
	if r := c.call(http.MethodPatch, "/api/v1/objects/"+id+"/metadata", []byte(`{"ct":"any"}`), map[string]string{"Content-Type": "application/x-www-form-urlencoded"}); r.status != 200 {
		t.Fatalf("form-urlencoded JSON body: %d %+v", r.status, r.err)
	}
	if _, r := c.patch(id, `{"a":"b"}`, `v2`); r.status != 400 {
		t.Fatalf("malformed If-Match: %d", r.status)
	}
	// Limits apply to the merged result: 50 keys total is fine, 51 is not.
	cur, _ := c.metadata(id)
	var keys []string
	for i := len(cur.CustomMetadata); i < 50; i++ {
		keys = append(keys, fmt.Sprintf(`"k%d":"v"`, i))
	}
	if _, r := c.patch(id, "{"+strings.Join(keys, ",")+"}", ""); r.status != 200 {
		t.Fatalf("filling to 50 keys: %d %+v", r.status, r.err)
	}
	if _, r := c.patch(id, `{"one-too-many":"v"}`, ""); r.status != 400 || r.err.Error.Code != wire.CodeMetadataLimitExceeded {
		t.Fatalf("51st key: %d %+v", r.status, r.err)
	}
	if _, r := c.patch("3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b", `{"a":"b"}`, ""); r.status != 404 {
		t.Fatalf("unknown object: %d", r.status)
	}
	// The object's data is unchanged by all of this.
	c.wantDownload(id, data)
}

func TestConcurrentPatchesAreNotLost(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	id := c.mustUpload(randomBytes(t, 100))
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			if _, r := c.patch(id, fmt.Sprintf(`{"k%d":"v"}`, i), ""); r.status != 200 {
				t.Errorf("patch %d: %d", i, r.status)
			}
		})
	}
	wg.Wait()
	md, _ := c.metadata(id)
	if len(md.CustomMetadata) != 20 || md.MetadataVersion != 21 {
		t.Fatalf("%d keys, version %d after 20 concurrent patches", len(md.CustomMetadata), md.MetadataVersion)
	}
}

func (c *uploadCluster) listAll(limit int) ([]wire.ObjectSummary, int) {
	c.t.Helper()
	var all []wire.ObjectSummary
	cursor, pages := "", 0
	for {
		q := url.Values{"limit": {fmt.Sprint(limit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		r := c.call(http.MethodGet, "/api/v1/objects?"+q.Encode(), nil, nil)
		if r.status != 200 {
			c.t.Fatalf("list: %d %+v", r.status, r.err)
		}
		var page wire.ObjectList
		r.decode(c.t, &page)
		pages++
		all = append(all, page.Objects...)
		if page.NextCursor == nil {
			return all, pages
		}
		cursor = *page.NextCursor
	}
}

func TestListPagination(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	if all, pages := c.listAll(10); len(all) != 0 || pages != 1 {
		t.Fatalf("empty store: %d objects, %d pages", len(all), pages)
	}
	ids := map[string]bool{}
	for i := range 23 {
		ids[c.mustUpload(randomBytes(t, 10+i))] = true
	}
	all, pages := c.listAll(10)
	if len(all) != 23 || pages != 3 {
		t.Fatalf("%d objects in %d pages", len(all), pages)
	}
	for i, o := range all {
		if !ids[o.ObjectID] || o.Name != "obj.bin" || o.ContentType != "application/octet-stream" || o.Size == 0 {
			t.Fatalf("entry %d = %+v", i, o)
		}
		if i > 0 && (o.CreatedAt < all[i-1].CreatedAt || (o.CreatedAt == all[i-1].CreatedAt && o.ObjectID <= all[i-1].ObjectID)) {
			t.Fatalf("not ordered at %d", i)
		}
		delete(ids, o.ObjectID)
	}
	// Default limit (100) returns everything in one page.
	if r := c.call(http.MethodGet, "/api/v1/objects", nil, nil); r.status != 200 {
		t.Fatalf("default list: %d", r.status)
	} else {
		var page wire.ObjectList
		r.decode(t, &page)
		if len(page.Objects) != 23 || page.NextCursor != nil {
			t.Fatalf("default page: %d objects, next %v", len(page.Objects), page.NextCursor)
		}
	}
	for _, q := range []string{"limit=0", "limit=1001", "limit=x", "cursor=!!!", "cursor=bm90LWEtY3Vyc29y"} {
		if r := c.call(http.MethodGet, "/api/v1/objects?"+q, nil, nil); r.status != 400 || r.err.Error.Code != wire.CodeInvalidPagination {
			t.Errorf("?%s: %d %+v", q, r.status, r.err)
		}
	}
}

// TestListStableUnderConcurrentUploads: objects present before paging starts
// are each listed exactly once even while new uploads commit between pages.
func TestListStableUnderConcurrentUploads(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	existing := map[string]bool{}
	for range 15 {
		existing[c.mustUpload(randomBytes(t, 50))] = true
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				c.upload(filePart("new", randomBytes(t, 50)))
			}
		}
	})
	seen := map[string]int{}
	cursor := ""
	for {
		q := url.Values{"limit": {"4"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		r := c.call(http.MethodGet, "/api/v1/objects?"+q.Encode(), nil, nil)
		var page wire.ObjectList
		r.decode(t, &page)
		for _, o := range page.Objects {
			seen[o.ObjectID]++
		}
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
		time.Sleep(5 * time.Millisecond) // let uploads commit between pages
	}
	close(stop)
	wg.Wait()
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("object %s listed %d times", id, n)
		}
	}
	for id := range existing {
		if seen[id] != 1 {
			t.Fatalf("pre-existing object %s missing from the listing", id)
		}
	}
}

// TestDelete is the M5 delete checkpoint: the object vanishes at once for
// every read path; its files vanish shortly after; deleting again is 404.
func TestDelete(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	keep := c.mustUpload(randomBytes(t, 1500))
	id := c.mustUpload(randomBytes(t, 4500))
	if r := c.call(http.MethodDelete, "/api/v1/objects/"+id, nil, nil); r.status != 204 || len(r.body) != 0 {
		t.Fatalf("delete: %d %q", r.status, r.body)
	}
	if r := c.download(id); r.status != 404 {
		t.Fatalf("download after delete: %d", r.status)
	}
	if _, r := c.metadata(id); r.status != 404 {
		t.Fatalf("metadata after delete: %d", r.status)
	}
	if _, r := c.patch(id, `{"a":"b"}`, ""); r.status != 404 {
		t.Fatalf("patch after delete: %d", r.status)
	}
	all, _ := c.listAll(100)
	if len(all) != 1 || all[0].ObjectID != keep {
		t.Fatalf("listing after delete: %+v", all)
	}
	if r := c.call(http.MethodDelete, "/api/v1/objects/"+id, nil, nil); r.status != 404 {
		t.Fatalf("second delete: %d", r.status)
	}
	c.waitClean()
	if n := c.filesOnWorkers(id); n != 0 {
		t.Fatalf("%d files of the deleted object remain", n)
	}
	if n := c.filesOnWorkers(keep); n != 4 {
		t.Fatalf("the other object has %d files, want 4", n)
	}
	if _, err := c.meta.GetObject(context.Background(), id); err == nil {
		t.Fatal("deleted object's row was not purged")
	}
	if r := c.call(http.MethodDelete, "/api/v1/objects/bad", nil, nil); r.status != 400 {
		t.Fatalf("malformed id: %d", r.status)
	}
}

// TestDeleteWithWorkerDown: delete returns 204 at once; the down worker's
// files are removed when it comes back.
func TestDeleteWithWorkerDown(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	id := c.mustUpload(randomBytes(t, 6000))
	victim := c.chunkMap(id)[0].Replicas[0].WorkerID
	w := c.byID[victim]
	w.stop()
	waitFor(t, clusterLimit, victim+" DEAD", func() bool { v, _ := c.reg.Get(victim); return v.State == "DEAD" })

	if r := c.call(http.MethodDelete, "/api/v1/objects/"+id, nil, nil); r.status != 204 {
		t.Fatalf("delete with a worker down: %d", r.status)
	}
	// Live workers are cleaned; the down worker's entries wait.
	waitFor(t, clusterLimit, "live workers cleaned", func() bool {
		for _, other := range c.workers {
			if other.id == victim {
				continue
			}
			if inv, _ := other.store.Inventory(); len(inv) != 0 {
				return false
			}
		}
		return true
	})
	if inv, _ := w.store.Inventory(); len(inv) == 0 {
		t.Fatal("the down worker has no files; the test did not exercise the retry")
	}
	if o, err := c.meta.GetObject(context.Background(), id); err != nil || o.Status != meta.StatusDeleting {
		t.Fatalf("object should stay DELETING while a copy remains: %+v, %v", o, err)
	}
	w.start()
	c.waitClean()
	if inv, _ := w.store.Inventory(); len(inv) != 0 {
		t.Fatalf("returned worker still holds %d files", len(inv))
	}
	if _, err := c.meta.GetObject(context.Background(), id); err == nil {
		t.Fatal("object row not purged after the worker returned")
	}
}

// TestDeleteDuringDownload (architecture §5.6): a delete racing a streaming
// download may cut the stream, but never makes it return wrong bytes.
func TestDeleteDuringDownload(t *testing.T) {
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	data := randomBytes(t, 20_000)
	id := c.mustUpload(data)
	for _, w := range c.workers {
		w.faults.getLatency.Store(int64(20 * time.Millisecond))
	}
	resp, err := http.Get(c.url + "/api/v1/objects/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 3000)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	if r := c.call(http.MethodDelete, "/api/v1/objects/"+id, nil, nil); r.status != 204 {
		t.Fatalf("delete: %d", r.status)
	}
	rest, readErr := io.ReadAll(resp.Body)
	got := append(buf, rest...)
	if !bytes.Equal(got, data[:len(got)]) {
		t.Fatal("download returned wrong bytes after a concurrent delete")
	}
	t.Logf("download after concurrent delete: %d of %d bytes, read error: %v", len(got), len(data), readErr)
	c.waitClean()
}

// TestObjctl exercises the CLI binary against the in-process cluster.
func TestObjctl(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the objctl binary")
	}
	bin := buildBinary(t, "objctl")
	c := newUploadCluster(t, clusterOpts{workers: 3, chunkSize: 1000})
	dir := t.TempDir()
	src := filepath.Join(dir, "in.bin")
	data := randomBytes(t, 4321)
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		cmd := exec.Command(bin, append([]string{"-server", c.url}, args...)...)
		var out, errb bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &errb
		err := cmd.Run()
		if err != nil {
			return out.String() + errb.String(), err
		}
		return out.String(), nil
	}
	out, err := run("upload", src, "-meta", "owner=alice", "-type", "text/plain")
	if err != nil {
		t.Fatalf("upload: %v\n%s", err, out)
	}
	var info wire.ObjectInfo
	if err := json.Unmarshal([]byte(out), &info); err != nil || info.Size != 4321 || info.CustomMetadata["owner"] != "alice" || info.Name != "in.bin" {
		t.Fatalf("upload output: %v\n%s", err, out)
	}
	dst := filepath.Join(dir, "out.bin")
	if out, err := run("download", info.ObjectID, "-o", dst); err != nil {
		t.Fatalf("download: %v\n%s", err, out)
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs")
	}
	if out, err := run("patch", info.ObjectID, "owner=bob", "tmp="); err != nil || !strings.Contains(out, `"owner": "bob"`) {
		t.Fatalf("patch: %v\n%s", err, out)
	}
	if out, err := run("patch", info.ObjectID, "-if-match", "v1", "x=y"); err == nil || !strings.Contains(out, "VERSION_CONFLICT") {
		t.Fatalf("stale -if-match should fail with VERSION_CONFLICT: %v\n%s", err, out)
	}
	if out, err := run("stat", info.ObjectID); err != nil || !strings.Contains(out, `"chunks"`) {
		t.Fatalf("stat: %v\n%s", err, out)
	}
	out, err = run("chunks", info.ObjectID)
	if err != nil || !strings.Contains(out, "CHUNK") || strings.Count(out, "(OK)") != 2*5 {
		t.Fatalf("chunks: want 5 chunks with 2 OK copies each: %v\n%s", err, out)
	}
	for _, ch := range c.chunkMap(info.ObjectID) {
		for _, r := range ch.Replicas {
			if !strings.Contains(out, r.WorkerID+" (OK)") {
				t.Fatalf("chunks output lacks %s:\n%s", r.WorkerID, out)
			}
		}
	}
	if out, err := run("audit"); err != nil || !strings.Contains(out, `"ok": true`) {
		t.Fatalf("audit: %v\n%s", err, out)
	}
	if out, err := run("list", "-all", "-limit", "1"); err != nil || !strings.Contains(out, info.ObjectID) {
		t.Fatalf("list: %v\n%s", err, out)
	}
	if out, err := run("cluster"); err != nil || !strings.Contains(out, `"alive": 3`) {
		t.Fatalf("cluster: %v\n%s", err, out)
	}
	if out, err := run("delete", info.ObjectID); err != nil {
		t.Fatalf("delete: %v\n%s", err, out)
	}
	if out, err := run("stat", info.ObjectID); err == nil || !strings.Contains(out, "OBJECT_NOT_FOUND") {
		t.Fatalf("stat after delete should fail: %v\n%s", err, out)
	}
	if _, err := os.Stat(dst + ".missing"); err == nil {
		t.Fatal("unexpected file")
	}
	// A failed download leaves no output file.
	missing := filepath.Join(dir, "missing.bin")
	if out, err := run("download", info.ObjectID, "-o", missing); err == nil {
		t.Fatalf("download of a deleted object succeeded:\n%s", out)
	}
	if _, err := os.Stat(missing); err == nil {
		t.Fatal("a failed download left an output file")
	}
	var ee *exec.ExitError
	if _, err := run("frobnicate"); err == nil || !asExit(err, &ee) || ee.ExitCode() != 2 {
		t.Fatalf("unknown command: %v", err)
	}
}

func asExit(err error, target **exec.ExitError) bool {
	e, ok := err.(*exec.ExitError)
	if ok {
		*target = e
	}
	return ok
}

func TestObjctlEmptyList(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the objctl binary")
	}
	bin := buildBinary(t, "objctl")
	c := newUploadCluster(t, clusterOpts{workers: 2, chunkSize: 1000})
	out, err := exec.Command(bin, "-server", c.url, "list").Output()
	if err != nil || !strings.Contains(string(out), `"objects": []`) {
		t.Fatalf("empty list: %v\n%s", err, out)
	}
}
