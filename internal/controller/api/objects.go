package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"objstore/internal/controller/custommeta"
	"objstore/internal/controller/meta"
	"objstore/internal/httpx"
	"objstore/internal/ids"
	"objstore/internal/wire"
)

// Listing limits (docs/API.md).
const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

func (s *Server) objectRoutes(mux *http.ServeMux) {
	if s.cfg.Meta == nil {
		return
	}
	mux.HandleFunc("GET /api/v1/objects/{object_id}/metadata", s.getMetadata)
	mux.HandleFunc("PATCH /api/v1/objects/{object_id}/metadata", s.patchMetadata)
	mux.HandleFunc("DELETE /api/v1/objects/{object_id}", s.deleteObject)
	mux.HandleFunc("GET /api/v1/objects", s.listObjects)
}

// objectID validates the path parameter, writing a 400 if invalid.
func objectID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("object_id")
	if !ids.ValidObjectID(id) {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidObjectID, fmt.Sprintf("invalid object id %q", id))
		return "", false
	}
	return id, true
}

func notFound(w http.ResponseWriter, id string) {
	httpx.WriteError(w, http.StatusNotFound, wire.CodeObjectNotFound, fmt.Sprintf("object %s not found", id))
}

// getMetadata implements GET /api/v1/objects/{id}/metadata (§4.3).
func (s *Server) getMetadata(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(w, r)
	if !ok {
		return
	}
	md, err := s.loadMetadata(r, id)
	if errors.Is(err, meta.ErrNotFound) {
		notFound(w, id)
		return
	}
	if err != nil {
		s.log.Error("get_metadata_failed", "object_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		return
	}
	w.Header().Set("ETag", versionETag(md.MetadataVersion))
	httpx.WriteJSON(w, http.StatusOK, md)
}

// loadMetadata reads a READY object and its chunk map. The two reads are not
// one transaction; the status is re-checked afterwards, and since an object
// never returns to READY, a concurrent delete is reported as not found rather
// than as an object with a half-empty chunk map.
func (s *Server) loadMetadata(r *http.Request, id string) (wire.ObjectMetadata, error) {
	ctx := r.Context()
	o, err := s.cfg.Meta.GetObject(ctx, id)
	if err != nil {
		return wire.ObjectMetadata{}, err
	}
	if o.Status != meta.StatusReady {
		return wire.ObjectMetadata{}, meta.ErrNotFound
	}
	cm, err := s.cfg.Meta.ChunkMap(ctx, id)
	if err != nil {
		return wire.ObjectMetadata{}, err
	}
	if again, err := s.cfg.Meta.GetObject(ctx, id); err != nil || again.Status != meta.StatusReady {
		return wire.ObjectMetadata{}, meta.ErrNotFound
	}
	return objectMetadata(o, cm), nil
}

func objectMetadata(o meta.Object, cm []meta.ChunkWithReplicas) wire.ObjectMetadata {
	md := wire.ObjectMetadata{ObjectInfo: objectInfo(o), Chunks: make([]wire.ChunkView, len(cm))}
	for i, c := range cm {
		v := wire.ChunkView{Index: c.Idx, ChunkID: c.ID, Size: c.Size, ChecksumSHA256: c.ChecksumSHA256,
			Replicas: make([]wire.ReplicaView, len(c.Replicas))}
		for j, rep := range c.Replicas {
			v.Replicas[j] = wire.ReplicaView{WorkerID: rep.WorkerID, State: rep.State}
		}
		md.Chunks[i] = v
	}
	return md
}

func versionETag(v int64) string { return `"v` + strconv.FormatInt(v, 10) + `"` }

var ifMatchRe = regexp.MustCompile(`^"v([0-9]{1,18})"$`)

// parseIfMatch returns the required version (nil if absent or "*").
func parseIfMatch(h string) (*int64, error) {
	if h == "" || h == "*" {
		return nil, nil
	}
	m := ifMatchRe.FindStringSubmatch(h)
	if m == nil {
		return nil, fmt.Errorf(`If-Match must be "*" or an ETag like "v3", got %q`, h)
	}
	v, err := strconv.ParseInt(m[1], 10, 64)
	return &v, err
}

// patchMetadata implements PATCH /api/v1/objects/{id}/metadata (§4.3,
// DEC-014): JSON merge on custom metadata, null removes a key, optional
// If-Match → 409 on version mismatch. Never touches chunks or workers.
func (s *Server) patchMetadata(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(w, r)
	if !ok {
		return
	}
	// The body is always parsed as JSON, whatever Content-Type says
	// (DEC-045): `curl -X PATCH -d '{...}'` sends form-urlencoded by default,
	// and the strict JSON-object validation below is what protects the data.
	ifVersion, err := parseIfMatch(r.Header.Get("If-Match"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, err.Error())
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, custommeta.MaxDocumentBytes+1))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidRequest, "reading body: "+err.Error())
		return
	}
	patch, err := custommeta.ParsePatch(raw)
	if err != nil {
		writeMetadataError(w, err)
		return
	}
	o, err := s.cfg.Meta.UpdateMetadata(r.Context(), id, ifVersion, func(cur map[string]string) (map[string]string, error) {
		return custommeta.Merge(cur, patch)
	})
	switch {
	case err == nil:
	case errors.Is(err, meta.ErrNotFound):
		notFound(w, id)
		return
	case errors.Is(err, meta.ErrVersionConflict):
		httpx.WriteError(w, http.StatusConflict, wire.CodeVersionConflict, strings.TrimPrefix(err.Error(), "meta: "))
		return
	case errors.Is(err, custommeta.ErrInvalid), errors.Is(err, custommeta.ErrLimit):
		writeMetadataError(w, err)
		return
	default:
		s.log.Error("patch_metadata_failed", "object_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		return
	}
	s.log.Info("metadata_updated", "object_id", id, "metadata_version", o.MetadataVersion, "keys_patched", len(patch))
	md, err := s.loadMetadata(r, id)
	if errors.Is(err, meta.ErrNotFound) { // deleted right after the update
		notFound(w, id)
		return
	}
	if err != nil {
		s.log.Error("get_metadata_failed", "object_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		return
	}
	w.Header().Set("ETag", versionETag(md.MetadataVersion))
	httpx.WriteJSON(w, http.StatusOK, md)
}

func writeMetadataError(w http.ResponseWriter, err error) {
	code := wire.CodeInvalidMetadata
	if errors.Is(err, custommeta.ErrLimit) {
		code = wire.CodeMetadataLimitExceeded
	}
	httpx.WriteError(w, http.StatusBadRequest, code, err.Error())
}

// deleteObject implements DELETE /api/v1/objects/{id} (§4.4, D18): one
// transaction makes the object DELETING (invisible to everyone) and turns its
// replicas into pending deletes; 204 is returned at once and the janitor
// removes the files, retrying until every worker has confirmed.
func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	id, ok := objectID(w, r)
	if !ok {
		return
	}
	moved, err := s.cfg.Meta.MarkDeleting(r.Context(), id, "client_delete", meta.StatusReady)
	if err != nil {
		s.log.Error("delete_failed", "object_id", id, "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		return
	}
	if !moved {
		notFound(w, id) // unknown, still uploading, or already being deleted
		return
	}
	s.log.Info("object_deleted", "object_id", id)
	if s.cfg.OnDelete != nil {
		s.cfg.OnDelete()
	}
	w.WriteHeader(http.StatusNoContent)
}

type cursorJSON struct {
	T  int64  `json:"t"`
	ID string `json:"id"`
}

func encodeCursor(c *meta.ListCursor) *string {
	if c == nil {
		return nil
	}
	b, _ := json.Marshal(cursorJSON{T: c.CreatedAt, ID: c.ObjectID}) // cannot fail
	s := base64.RawURLEncoding.EncodeToString(b)
	return &s
}

func decodeCursor(s string) (*meta.ListCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("cursor is not valid")
	}
	var c cursorJSON
	if err := json.Unmarshal(b, &c); err != nil || c.T < 0 || !ids.ValidObjectID(c.ID) {
		return nil, errors.New("cursor is not valid")
	}
	return &meta.ListCursor{CreatedAt: c.T, ObjectID: c.ID}, nil
}

// listObjects implements GET /api/v1/objects?limit=&cursor= (§4.3): READY
// objects only, ordered by creation time then ID, keyset pagination.
func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := defaultListLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxListLimit {
			httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidPagination,
				fmt.Sprintf("limit must be an integer in [1, %d], got %q", maxListLimit, v))
			return
		}
		limit = n
	}
	var after *meta.ListCursor
	if v := q.Get("cursor"); v != "" {
		c, err := decodeCursor(v)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, wire.CodeInvalidPagination, err.Error())
			return
		}
		after = c
	}
	objs, next, err := s.cfg.Meta.ListObjects(r.Context(), limit, after)
	if err != nil {
		s.log.Error("list_failed", "error", err)
		httpx.WriteError(w, http.StatusInternalServerError, wire.CodeInternal, "internal error")
		return
	}
	out := wire.ObjectList{Objects: make([]wire.ObjectSummary, len(objs)), NextCursor: encodeCursor(next)}
	for i, o := range objs {
		out.Objects[i] = wire.ObjectSummary{
			ObjectID: o.ID, Name: o.Name, Size: o.Size, ContentType: o.ContentType,
			ChecksumSHA256: o.ChecksumSHA256, NumChunks: o.NumChunks,
			CreatedAt: o.CreatedAt.Format(time.RFC3339Nano), ModifiedAt: o.ModifiedAt.Format(time.RFC3339Nano),
		}
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}
