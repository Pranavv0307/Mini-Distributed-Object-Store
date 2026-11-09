// Package httpx contains small HTTP helpers shared by the controller and the
// workers: JSON responses, the standard error body, and request middleware.
package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"objstore/internal/wire"
)

// WriteJSON writes v as a JSON response with the given status. Encoding errors
// after the header is sent cannot be reported to the client and are ignored;
// they only occur when the connection is already broken.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes the standard JSON error body (DEC-015).
func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, wire.ErrorResponse{Error: wire.ErrorDetail{Code: code, Message: message}})
}

// statusRecorder captures the status code and body size for request logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Middleware logs every request as one structured line and converts handler
// panics into a 500 JSON error (unless the response was already started).
// Requests to /healthz are logged at debug level to keep logs readable.
func Middleware(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w}
		start := time.Now()
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler {
					panic(p) // deliberate connection abort; let net/http handle it
				}
				logger.Error("handler_panic", "panic", p, "stack", string(debug.Stack()),
					"method", r.Method, "path", r.URL.Path)
				if rec.status == 0 {
					WriteError(rec, http.StatusInternalServerError, wire.CodeInternal, "internal error")
				}
			}
			status := rec.status
			if status == 0 {
				status = http.StatusOK
			}
			level := slog.LevelInfo
			if r.URL.Path == "/healthz" {
				level = slog.LevelDebug
			}
			logger.LogAttrs(r.Context(), level, "http_request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("bytes_out", rec.bytes),
				slog.Int64("bytes_in", r.ContentLength),
				slog.Duration("duration", time.Since(start)),
			)
		}()
		next.ServeHTTP(rec, r)
	})
}
