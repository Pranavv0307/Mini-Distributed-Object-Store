// Package workerclient is the only code in the controller that talks to
// workers (architecture §2.2). Every call has a connect timeout and an
// overall per-request timeout (§5.5: "a call with no timeout is a latent
// outage").
//
// Callers must not treat an error as proof that an operation did not happen:
// a timed-out PUT may still have stored the chunk. The upload path therefore
// records every failed claim as a pending delete (DEC-007).
package workerclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"objstore/internal/wire"
)

// Error is a non-success HTTP response from a worker.
type Error struct {
	Op      string
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("worker %s: %d %s: %s", e.Op, e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("worker %s: HTTP %d", e.Op, e.Status)
}

// Client calls worker endpoints.
type Client struct {
	http           *http.Client
	requestTimeout time.Duration
}

// New creates a Client. connectTimeout bounds TCP connection setup
// (WORKER_CONNECT_TIMEOUT), requestTimeout bounds each whole request
// (WORKER_REQUEST_TIMEOUT).
func New(connectTimeout, requestTimeout time.Duration) *Client {
	tr := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: requestTimeout,
	}
	return &Client{http: &http.Client{Transport: tr}, requestTimeout: requestTimeout}
}

// PutChunk stores data as chunkID on the worker at baseURL, asking the
// worker to verify it against sum.
func (c *Client) PutChunk(ctx context.Context, baseURL, chunkID string, data []byte, sum [sha256.Size]byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, baseURL+"/internal/chunks/"+chunkID, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(wire.HeaderChunkSHA256, hex.EncodeToString(sum[:]))
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("worker put %s: %w", chunkID, err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return responseError("put "+chunkID, resp)
	}
	return nil
}

// DeleteChunk deletes chunkID on the worker. Deleting an absent chunk
// succeeds (the worker answers 204 either way).
func (c *Client) DeleteChunk(ctx context.Context, baseURL, chunkID string) error {
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL+"/internal/chunks/"+chunkID, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("worker delete %s: %w", chunkID, err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return responseError("delete "+chunkID, resp)
	}
	return nil
}

// Chunk is a chunk as returned by a worker: the bytes as stored, unverified.
type Chunk struct {
	Data []byte
	// StoredSHA256 is the worker's own record from the chunk header; the
	// controller's metadata checksum is the authority (§3.6).
	StoredSHA256 string
}

// GetChunk fetches chunkID, reading at most maxBytes (callers pass the
// expected size + 1 so an oversized reply is detectable but bounded).
func (c *Client) GetChunk(ctx context.Context, baseURL, chunkID string, maxBytes int64) (Chunk, error) {
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/internal/chunks/"+chunkID, nil)
	if err != nil {
		return Chunk{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Chunk{}, fmt.Errorf("worker get %s: %w", chunkID, err)
	}
	defer drainClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return Chunk{}, responseError("get "+chunkID, resp)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return Chunk{}, fmt.Errorf("worker get %s: read body: %w", chunkID, err)
	}
	return Chunk{Data: data, StoredSHA256: resp.Header.Get(wire.HeaderChunkSHA256)}, nil
}

func responseError(op string, resp *http.Response) error {
	e := &Error{Op: op, Status: resp.StatusCode}
	var body wire.ErrorResponse
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if json.Unmarshal(raw, &body) == nil {
		e.Code, e.Message = body.Error.Code, body.Error.Message
	} else if len(raw) > 0 {
		e.Message = strconv.Quote(string(raw))
	}
	return e
}

// drainClose reads a little of the remaining body so the connection can be
// reused, then closes it.
func drainClose(rc io.ReadCloser) {
	_, _ = io.CopyN(io.Discard, rc, 64<<10)
	_ = rc.Close()
}
