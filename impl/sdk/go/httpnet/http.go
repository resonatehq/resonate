// Package httpnet provides an HTTP-based transport implementation of the
// resonate.Network interface, talking to a live Resonate server via JSON
// envelopes over POST and SSE long-poll for push messages.
package httpnet

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// HTTPNetwork is a Network implementation that talks to a live Resonate server.
//
//   - Requests: POST {URL}/ with the JSON envelope as the body.
//   - Push messages: SSE long-poll on GET {URL}/poll/{group}/{pid}.
//   - Reconnect: on failure, sleep with exponential backoff doubling from 1s
//     up to 60s; reset to 1s after a successful connect.
//
// Addresses use the poll:// scheme: poll://uni@<group>/<pid> for unicast,
// poll://any@<group>/<pid> for anycast.
type HTTPNetwork struct {
	url     string
	pid     string
	group   string
	unicast string
	anycast string
	auth    string

	client *http.Client

	mu          sync.RWMutex
	subscribers []func(string)

	startMu sync.Mutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// HTTPOptions configures an HTTPNetwork. Pid empty means auto-generate, Group
// empty means "default", Auth empty means no Authorization header.
type HTTPOptions struct {
	PID    string
	Group  string
	Auth   string
	Client *http.Client
}

// NewHTTP builds an HTTPNetwork.
func NewHTTP(url string, opts HTTPOptions) *HTTPNetwork {
	pid := opts.PID
	if pid == "" {
		pid = randomPID()
	}
	group := opts.Group
	if group == "" {
		group = "default"
	}
	client := opts.Client
	if client == nil {
		client = http.DefaultClient
	}
	url = strings.TrimRight(url, "/")
	return &HTTPNetwork{
		url:     url,
		pid:     pid,
		group:   group,
		unicast: fmt.Sprintf("poll://uni@%s/%s", group, pid),
		anycast: fmt.Sprintf("poll://any@%s/%s", group, pid),
		auth:    opts.Auth,
		client:  client,
	}
}

// PID returns the unique process identifier for this network instance.
func (h *HTTPNetwork) PID() string { return h.pid }

// Group returns the routing group this instance belongs to (default: "default").
func (h *HTTPNetwork) Group() string { return h.group }

// Unicast returns the point-to-point poll address for this instance (poll://uni@<group>/<pid>).
func (h *HTTPNetwork) Unicast() string { return h.unicast }

// Anycast returns the load-balanced group address for this instance (poll://any@<group>/<pid>).
func (h *HTTPNetwork) Anycast() string { return h.anycast }

// TargetResolver converts a logical target name to a poll:// anycast address for that group.
func (h *HTTPNetwork) TargetResolver(target string) string { return "poll://any@" + target }

// Start launches the SSE long-poll goroutine that delivers push frames to Recv subscribers.
// Subsequent calls are no-ops if already started.
func (h *HTTPNetwork) Start(ctx context.Context) error {
	h.startMu.Lock()
	defer h.startMu.Unlock()
	if h.cancel != nil {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.runSSE(runCtx)
	}()
	return nil
}

// Stop cancels the SSE goroutine, waits for it to exit, and clears all Recv subscribers.
func (h *HTTPNetwork) Stop() error {
	h.startMu.Lock()
	cancel := h.cancel
	h.cancel = nil
	h.startMu.Unlock()
	if cancel != nil {
		cancel()
	}
	h.wg.Wait()
	h.mu.Lock()
	h.subscribers = nil
	h.mu.Unlock()
	return nil
}

// Send posts the JSON envelope to the server and returns the response envelope JSON.
func (h *HTTPNetwork) Send(ctx context.Context, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url+"/", strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.auth != "" {
		req.Header.Set("Authorization", "Bearer "+h.auth)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// Recv registers cb to receive raw push-message JSON frames delivered via SSE.
func (h *HTTPNetwork) Recv(cb func(raw string)) {
	if cb == nil {
		return
	}
	h.mu.Lock()
	h.subscribers = append(h.subscribers, cb)
	h.mu.Unlock()
}

func (h *HTTPNetwork) snapshotSubscribers() []func(string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]func(string), len(h.subscribers))
	copy(out, h.subscribers)
	return out
}

// runSSE is the SSE long-poll goroutine. It reconnects with exponential
// backoff from 1s up to 60s; backoff resets to 1s after a successful connect.
func (h *HTTPNetwork) runSSE(ctx context.Context) {
	backoff := time.Second
	const maxBackoff = 60 * time.Second
	endpoint := fmt.Sprintf("%s/poll/%s/%s", h.url, h.group, h.pid)

	for {
		if ctx.Err() != nil {
			return
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			if !waitOrCancel(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		if h.auth != "" {
			req.Header.Set("Authorization", "Bearer "+h.auth)
		}

		resp, err := h.client.Do(req)
		if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if resp != nil {
				_ = resp.Body.Close()
			}
			if !waitOrCancel(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		backoff = time.Second // reset on success
		h.readSSEStream(ctx, resp.Body)
		_ = resp.Body.Close()

		if !waitOrCancel(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func (h *HTTPNetwork) readSSEStream(ctx context.Context, body io.ReadCloser) {
	reader := bufio.NewReader(body)
	var event bytes.Buffer
	for {
		if ctx.Err() != nil {
			return
		}
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			// Strip trailing \r\n or \n.
			if n := len(line); n >= 2 && line[n-2] == '\r' {
				line = line[:n-2]
			} else if line[n-1] == '\n' {
				line = line[:n-1]
			}
			if len(line) == 0 {
				// End of event.
				h.dispatchSSEEvent(event.Bytes())
				event.Reset()
			} else {
				event.Write(line)
				event.WriteByte('\n')
			}
		}
		if err != nil {
			return
		}
	}
}

func (h *HTTPNetwork) dispatchSSEEvent(buf []byte) {
	if len(buf) == 0 {
		return
	}
	subs := h.snapshotSubscribers()
	for _, line := range bytes.Split(buf, []byte("\n")) {
		data, ok := bytes.CutPrefix(line, []byte("data:"))
		if !ok {
			continue
		}
		data = bytes.TrimSpace(data)
		if len(data) == 0 {
			continue
		}
		raw := string(data)
		for _, cb := range subs {
			cb(raw)
		}
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	n := cur * 2
	if n > max {
		return max
	}
	return n
}

func waitOrCancel(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func randomPID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixMilli())
	}
	return hex.EncodeToString(b[:])
}
