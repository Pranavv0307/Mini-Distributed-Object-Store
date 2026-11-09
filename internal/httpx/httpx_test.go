package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"objstore/internal/wire"
)

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteError(rec, http.StatusNotFound, wire.CodeChunkNotFound, "nope")
	if rec.Code != http.StatusNotFound || rec.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("code=%d ct=%q", rec.Code, rec.Header().Get("Content-Type"))
	}
	var e wire.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatal(err)
	}
	if e.Error.Code != wire.CodeChunkNotFound || e.Error.Message != "nope" {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestMiddlewareRecoversPanics(t *testing.T) {
	h := Middleware(slog.New(slog.DiscardHandler), http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", rec.Code)
	}
	var e wire.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error.Code != wire.CodeInternal {
		t.Fatalf("body = %s, err = %v", rec.Body.String(), err)
	}
}
