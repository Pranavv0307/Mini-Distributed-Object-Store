// Package agent is the worker's membership agent: it registers the worker
// with the controller (with an inventory snapshot) and then sends heartbeats,
// re-registering whenever the controller asks (architecture §4.5, DEC-006).
//
// Cycle:
//
//	close write gate and drain in-flight writes
//	→ snapshot inventory → POST /internal/workers/register (retry with backoff)
//	→ open write gate → heartbeat every interval
//	→ on "reregister": start the cycle again
//
// The write gate is closed for the whole time the worker is not registered,
// so the inventory the controller reconciles is exact (no write can land
// between the snapshot and the controller marking the worker ALIVE).
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"objstore/internal/wire"
	"objstore/internal/worker/store"
)

// Store is the part of the chunk store the agent needs.
type Store interface {
	Inventory() ([]store.Entry, error)
	Stats() (store.Stats, error)
	FreeBytes() int64
}

// Gate is the part of the worker HTTP server the agent controls.
type Gate interface {
	OpenWrites()
	CloseWritesAndDrain(ctx context.Context) error
	SetRegistered(bool)
}

// Config configures an Agent.
type Config struct {
	WorkerID      string
	AdvertiseAddr string
	ControllerURL string
	// RegisterTimeout bounds one registration attempt, including the
	// controller's inventory reconciliation. Default 30s.
	RegisterTimeout time.Duration
	// HeartbeatTimeout bounds one heartbeat request. Default 5s.
	HeartbeatTimeout time.Duration
	// BackoffMin/BackoffMax bound the delay between failed registration
	// attempts (exponential with jitter). Defaults 200ms and 5s.
	BackoffMin, BackoffMax time.Duration
	Client                 *http.Client
	Logger                 *slog.Logger
}

// Agent runs the membership protocol for one worker.
type Agent struct {
	cfg   Config
	store Store
	gate  Gate
	log   *slog.Logger
}

// New creates an Agent; zero Config durations get defaults.
func New(cfg Config, st Store, gate Gate) *Agent {
	if cfg.RegisterTimeout <= 0 {
		cfg.RegisterTimeout = 30 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 5 * time.Second
	}
	if cfg.BackoffMin <= 0 {
		cfg.BackoffMin = 200 * time.Millisecond
	}
	if cfg.BackoffMax < cfg.BackoffMin {
		cfg.BackoffMax = max(5*time.Second, cfg.BackoffMin)
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Agent{cfg: cfg, store: st, gate: gate, log: logger}
}

// Run registers and heartbeats until ctx is done; it then returns ctx.Err().
// Controller unavailability is never fatal: the agent keeps retrying.
func (a *Agent) Run(ctx context.Context) error {
	for {
		epoch, interval, err := a.register(ctx)
		if err != nil {
			return err // only ctx errors reach here
		}
		a.heartbeatLoop(ctx, epoch, interval)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// register closes and drains the write gate, then registers with a fresh
// inventory snapshot until it succeeds, and finally reopens the gate.
func (a *Agent) register(ctx context.Context) (uint64, time.Duration, error) {
	a.gate.SetRegistered(false)
	if err := a.gate.CloseWritesAndDrain(ctx); err != nil {
		return 0, 0, err
	}
	backoff := a.cfg.BackoffMin
	for attempt := 1; ; attempt++ {
		resp, err := a.registerOnce(ctx)
		if err == nil {
			a.gate.SetRegistered(true)
			a.gate.OpenWrites()
			interval := time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
			if interval <= 0 {
				interval = 3 * time.Second
			}
			a.log.Info("worker_registered", "epoch", resp.Epoch, "attempts", attempt,
				"heartbeat_interval", interval)
			return resp.Epoch, interval, nil
		}
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		a.log.Warn("worker_register_failed", "attempt", attempt, "retry_in", backoff, "error", err)
		if err := sleep(ctx, jitter(backoff)); err != nil {
			return 0, 0, err
		}
		backoff = min(backoff*2, a.cfg.BackoffMax)
	}
}

func (a *Agent) registerOnce(ctx context.Context) (wire.RegisterResponse, error) {
	// The gate is closed, so the snapshot cannot race with API writes.
	inv, err := a.store.Inventory()
	if err != nil {
		return wire.RegisterResponse{}, fmt.Errorf("inventory: %w", err)
	}
	req := wire.RegisterRequest{
		WorkerID:  a.cfg.WorkerID,
		Address:   a.cfg.AdvertiseAddr,
		FreeBytes: a.store.FreeBytes(),
		Inventory: make([]wire.ChunkInfo, len(inv)),
	}
	for i, e := range inv {
		req.Inventory[i] = wire.ChunkInfo{ChunkID: e.ChunkID, Size: e.Size}
		req.UsedBytes += e.Size
	}
	var resp wire.RegisterResponse
	err = a.postJSON(ctx, a.cfg.RegisterTimeout, "/internal/workers/register", req, &resp)
	return resp, err
}

// heartbeatLoop sends heartbeats until the controller asks for
// re-registration or ctx ends. Failed heartbeats (controller down or
// unreachable) are logged and retried on the next tick; the worker keeps
// serving reads and writes meanwhile, since only the controller calls it.
func (a *Agent) heartbeatLoop(ctx context.Context, epoch uint64, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		action, err := a.heartbeatOnce(ctx, epoch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			if failures == 1 || failures%10 == 0 {
				a.log.Warn("heartbeat_failed", "consecutive_failures", failures, "error", err)
			}
			continue
		}
		if failures > 0 {
			a.log.Info("heartbeat_recovered", "after_failures", failures)
			failures = 0
		}
		if action == wire.ActionReregister {
			a.log.Info("worker_reregistering", "reason", "controller_requested", "epoch", epoch)
			return
		}
	}
}

func (a *Agent) heartbeatOnce(ctx context.Context, epoch uint64) (string, error) {
	st, err := a.store.Stats()
	if err != nil {
		return "", fmt.Errorf("stats: %w", err)
	}
	req := wire.HeartbeatRequest{
		WorkerID:      a.cfg.WorkerID,
		Epoch:         epoch,
		FreeBytes:     a.store.FreeBytes(),
		UsedBytes:     st.UsedBytes,
		ChunkCount:    st.ChunkCount,
		SuspectChunks: []string{},
	}
	var resp wire.HeartbeatResponse
	if err := a.postJSON(ctx, a.cfg.HeartbeatTimeout, "/internal/workers/heartbeat", req, &resp); err != nil {
		return "", err
	}
	switch resp.Action {
	case wire.ActionOK, wire.ActionReregister:
		return resp.Action, nil
	default:
		return "", fmt.Errorf("unknown heartbeat action %q", resp.Action)
	}
}

// postJSON POSTs in and decodes a 200 response into out. Any other status is
// an error carrying the controller's error code.
func (a *Agent) postJSON(ctx context.Context, timeout time.Duration, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.ControllerURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e wire.ErrorResponse
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if json.Unmarshal(raw, &e) == nil && e.Error.Code != "" {
			return fmt.Errorf("controller returned %d %s: %s", resp.StatusCode, e.Error.Code, e.Error.Message)
		}
		return fmt.Errorf("controller returned %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// jitter returns a duration uniformly in [d/2, d), so that workers restarted
// together do not retry in lockstep.
func jitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}
