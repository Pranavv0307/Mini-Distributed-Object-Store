// Command objctl is a small client for the object store's REST API. It only
// knows the controller's address (OBJSTORE_URL or -server), never worker
// addresses.
//
//	objctl [-server URL] upload FILE [-name NAME] [-type CONTENT_TYPE] [-meta k=v]...
//	objctl [-server URL] download ID [-o FILE]
//	objctl [-server URL] stat ID
//	objctl [-server URL] chunks ID      (table: each chunk and where its copies are)
//	objctl [-server URL] patch ID [-if-match vN] k=v | k= ...   (k= removes k)
//	objctl [-server URL] list [-limit N] [-all]
//	objctl [-server URL] delete ID
//	objctl [-server URL] cluster
//	objctl [-server URL] audit
//	objctl version
//
// JSON responses are printed indented. Downloads are verified locally: the
// byte count must equal Content-Length and the SHA-256 must equal the
// X-Object-Checksum-SHA256 header (architecture §3.6, layer 4).
// Exit status: 0 success, 1 request failed, 2 usage error.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"

	"objstore/internal/version"
	"objstore/internal/wire"
)

type usageError struct{ msg string }

func (e usageError) Error() string { return e.msg }

func main() {
	err := run(os.Args[1:], os.Stdout)
	var ue usageError
	switch {
	case err == nil:
	case errors.As(err, &ue):
		fmt.Fprintln(os.Stderr, "objctl:", err)
		fmt.Fprintln(os.Stderr, "usage: objctl [-server URL] upload|download|stat|chunks|patch|list|delete|cluster|audit|version ...")
		os.Exit(2)
	default:
		fmt.Fprintln(os.Stderr, "objctl:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	global := flag.NewFlagSet("objctl", flag.ContinueOnError)
	server := global.String("server", envOr("OBJSTORE_URL", "http://localhost:8080"), "controller base URL (env OBJSTORE_URL)")
	if err := global.Parse(args); err != nil {
		return usageError{err.Error()}
	}
	rest := global.Args()
	if len(rest) == 0 {
		return usageError{"missing command"}
	}
	c := &client{base: strings.TrimRight(*server, "/"), http: &http.Client{}, out: out}
	cmd, cargs := rest[0], rest[1:]
	switch cmd {
	case "upload":
		return c.upload(cargs)
	case "download":
		return c.download(cargs)
	case "stat":
		return c.stat(cargs)
	case "chunks":
		return c.chunks(cargs)
	case "patch":
		return c.patch(cargs)
	case "list":
		return c.list(cargs)
	case "delete":
		return c.del(cargs)
	case "cluster":
		return c.getJSON("/cluster")
	case "audit":
		return c.getJSON("/admin/audit")
	case "version":
		fmt.Fprintln(out, version.String("objctl"))
		return nil
	default:
		return usageError{fmt.Sprintf("unknown command %q", cmd)}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type client struct {
	base string
	http *http.Client
	out  io.Writer
}

// apiError turns a non-2xx response into an error with the server's code.
func apiError(resp *http.Response) error {
	var e wire.ErrorResponse
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if json.Unmarshal(raw, &e) == nil && e.Error.Code != "" {
		return fmt.Errorf("HTTP %d %s: %s", resp.StatusCode, e.Error.Code, e.Error.Message)
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
}

func (c *client) printJSON(r io.Reader) error {
	var v any
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	enc := json.NewEncoder(c.out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func (c *client) do(req *http.Request, okStatus int) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != okStatus {
		defer resp.Body.Close()
		return nil, apiError(resp)
	}
	return resp, nil
}

func (c *client) getJSON(path string) error {
	req, _ := http.NewRequest(http.MethodGet, c.base+path, nil)
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.printJSON(resp.Body)
}

func oneID(args []string, cmd string) (string, error) {
	if len(args) != 1 {
		return "", usageError{cmd + " needs exactly one object ID"}
	}
	return args[0], nil
}

func (c *client) stat(args []string) error {
	id, err := oneID(args, "stat")
	if err != nil {
		return err
	}
	return c.getJSON("/api/v1/objects/" + url.PathEscape(id) + "/metadata")
}

// chunks prints an object's chunk map as a table: one row per chunk with its
// size, the start of its checksum, and every copy with its worker and state.
func (c *client) chunks(args []string) error {
	id, err := oneID(args, "chunks")
	if err != nil {
		return err
	}
	req, _ := http.NewRequest(http.MethodGet, c.base+"/api/v1/objects/"+url.PathEscape(id)+"/metadata", nil)
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var md wire.ObjectMetadata
	if err := json.NewDecoder(resp.Body).Decode(&md); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	fmt.Fprintf(c.out, "object %s  %q  %d bytes  %d chunks\n", md.ObjectID, md.Name, md.Size, md.NumChunks)
	tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CHUNK\tSIZE\tSHA-256\tCOPIES")
	for _, ch := range md.Chunks {
		copies := make([]string, len(ch.Replicas))
		for i, r := range ch.Replicas {
			copies[i] = r.WorkerID + " (" + r.State + ")"
		}
		sum := ch.ChecksumSHA256
		if len(sum) > 12 {
			sum = sum[:12] + "…"
		}
		fmt.Fprintf(tw, "%d\t%d\t%s\t%s\n", ch.Index, ch.Size, sum, strings.Join(copies, ", "))
	}
	return tw.Flush()
}

// multiFlag collects repeated -meta k=v flags.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func (c *client) upload(args []string) error {
	fs := flag.NewFlagSet("upload", flag.ContinueOnError)
	name := fs.String("name", "", "object name (default: the file's base name)")
	ctype := fs.String("type", "application/octet-stream", "content type")
	var metas multiFlag
	fs.Var(&metas, "meta", "custom metadata key=value (repeatable)")
	path, rest := "", args
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		path, rest = rest[0], rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return usageError{err.Error()}
	}
	if path == "" && fs.NArg() == 1 {
		path = fs.Arg(0)
	}
	if path == "" {
		return usageError{"upload needs a FILE"}
	}
	if *name == "" {
		*name = filepath.Base(path)
	}
	md := map[string]string{}
	for _, kv := range metas {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return usageError{fmt.Sprintf("-meta %q is not key=value", kv)}
		}
		md[k] = v
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Stream the multipart body: metadata part first, then the file, without
	// loading the file into memory.
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		err := func() error {
			if len(md) > 0 {
				b, _ := json.Marshal(md)
				if err := mw.WriteField("metadata", string(b)); err != nil {
					return err
				}
			}
			h := textproto.MIMEHeader{}
			h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, *name))
			h.Set("Content-Type", *ctype)
			part, err := mw.CreatePart(h)
			if err != nil {
				return err
			}
			if _, err := io.Copy(part, f); err != nil {
				return err
			}
			return mw.Close()
		}()
		pw.CloseWithError(err)
	}()
	req, _ := http.NewRequest(http.MethodPost, c.base+"/api/v1/objects", pr)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.do(req, http.StatusCreated)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.printJSON(resp.Body)
}

func (c *client) download(args []string) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	outPath := fs.String("o", "", "output file (default: stdout)")
	id, rest := "", args
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		id, rest = rest[0], rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return usageError{err.Error()}
	}
	if id == "" {
		return usageError{"download needs an object ID"}
	}
	req, _ := http.NewRequest(http.MethodGet, c.base+"/api/v1/objects/"+url.PathEscape(id), nil)
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var dst io.Writer = c.out
	var tmp string
	if *outPath != "" {
		// Write to a temp file and rename only after verification, so a failed
		// download never leaves a plausible-looking output file.
		f, err := os.CreateTemp(filepath.Dir(*outPath), ".objctl-*")
		if err != nil {
			return err
		}
		tmp = f.Name()
		defer func() { f.Close(); os.Remove(tmp) }()
		dst = f
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(dst, h), resp.Body)
	wantLen, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	wantSum := resp.Header.Get(wire.HeaderObjectChecksum)
	gotSum := hex.EncodeToString(h.Sum(nil))
	switch {
	case copyErr != nil:
		return fmt.Errorf("download interrupted after %d of %d bytes: %w", n, wantLen, copyErr)
	case n != wantLen:
		return fmt.Errorf("download incomplete: %d of %d bytes", n, wantLen)
	case wantSum != "" && gotSum != wantSum:
		return fmt.Errorf("checksum mismatch: got %s, want %s", gotSum, wantSum)
	}
	if tmp != "" {
		if f, ok := dst.(*os.File); ok {
			if err := f.Close(); err != nil {
				return err
			}
		}
		if err := os.Rename(tmp, *outPath); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "objctl: wrote %d bytes to %s (sha256 %s verified)\n", n, *outPath, gotSum)
	}
	return nil
}

func (c *client) patch(args []string) error {
	fs := flag.NewFlagSet("patch", flag.ContinueOnError)
	ifMatch := fs.String("if-match", "", `required metadata version, e.g. v3`)
	id, rest := "", args
	if len(rest) > 0 && !strings.HasPrefix(rest[0], "-") {
		id, rest = rest[0], rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		return usageError{err.Error()}
	}
	if id == "" || fs.NArg() == 0 {
		return usageError{"patch needs an object ID and at least one key=value (key= removes the key)"}
	}
	body := map[string]any{}
	for _, kv := range fs.Args() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return usageError{fmt.Sprintf("%q is not key=value", kv)}
		}
		if v == "" {
			body[k] = nil
		} else {
			body[k] = v
		}
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPatch, c.base+"/api/v1/objects/"+url.PathEscape(id)+"/metadata", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if *ifMatch != "" {
		req.Header.Set("If-Match", `"`+strings.Trim(*ifMatch, `"`)+`"`)
	}
	resp, err := c.do(req, http.StatusOK)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.printJSON(resp.Body)
}

func (c *client) list(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	limit := fs.Int("limit", 100, "objects per page")
	all := fs.Bool("all", false, "follow next_cursor until the last page")
	if err := fs.Parse(args); err != nil {
		return usageError{err.Error()}
	}
	objects := []wire.ObjectSummary{} // "[]" not "null" when empty
	cursor := ""
	for {
		q := url.Values{"limit": {strconv.Itoa(*limit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		req, _ := http.NewRequest(http.MethodGet, c.base+"/api/v1/objects?"+q.Encode(), nil)
		resp, err := c.do(req, http.StatusOK)
		if err != nil {
			return err
		}
		var page wire.ObjectList
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			return err
		}
		objects = append(objects, page.Objects...)
		if !*all || page.NextCursor == nil {
			enc := json.NewEncoder(c.out)
			enc.SetIndent("", "  ")
			return enc.Encode(wire.ObjectList{Objects: objects, NextCursor: page.NextCursor})
		}
		cursor = *page.NextCursor
	}
}

func (c *client) del(args []string) error {
	id, err := oneID(args, "delete")
	if err != nil {
		return err
	}
	req, _ := http.NewRequest(http.MethodDelete, c.base+"/api/v1/objects/"+url.PathEscape(id), nil)
	resp, err := c.do(req, http.StatusNoContent)
	if err != nil {
		return err
	}
	resp.Body.Close()
	fmt.Fprintf(c.out, "deleted %s\n", id)
	return nil
}
