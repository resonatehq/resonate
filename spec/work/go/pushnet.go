package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	resonate "github.com/resonatehq/resonate/impl/sdk/go"
)

// PushNetwork is the SDK's `Network` for a server that pushes.
//
// `httpnet` receives work by holding `GET /poll/<group>/<pid>` open as a
// server-sent-event stream, at a `poll://` address. A server that only
// pushes — the object-store server in impl/server/s3 is one — cannot
// deliver to that. This transport turns it around: the worker listens, its
// address is a plain `http://` URL, and the server POSTs each message there.
//
// The message is the same JSON either way — `{"kind":"execute",...}` or
// `{"kind":"unblock",...}` — so the body of each POST goes to the `Recv`
// callbacks exactly as one SSE `data:` line would. Requests to the server
// are a POST of the envelope, as in `httpnet`.
type PushNetwork struct {
	url       string // the server
	pid       string
	group     string
	advertise string // how the server reaches it, e.g. "http://client:41733/"

	listener net.Listener
	client   *http.Client
	server   *http.Server

	mu          sync.Mutex
	subscribers []func(raw string)
}

var _ resonate.Network = (*PushNetwork)(nil)

// NewPushNetwork binds its listener now, so the address it advertises is
// the one it holds — port 0 takes whatever is free, which is what lets two
// drivers run side by side without agreeing on ports first.
func NewPushNetwork(url, pid, host string, port int) (*PushNetwork, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("push listener on :%d: %w", port, err)
	}
	bound := ln.Addr().(*net.TCPAddr).Port
	return &PushNetwork{
		url:       strings.TrimRight(url, "/"),
		pid:       pid,
		group:     "default",
		advertise: fmt.Sprintf("http://%s:%d/", host, bound),
		listener:  ln,
		client:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (p *PushNetwork) PID() string   { return p.pid }
func (p *PushNetwork) Group() string { return p.group }

// Every address is this worker's own: there is one worker per network, and
// whatever the server pushes for it — an execute for a task it created, an
// unblock for a promise it listens on — comes here.
func (p *PushNetwork) Unicast() string                { return p.advertise }
func (p *PushNetwork) Anycast() string                { return p.advertise }
func (p *PushNetwork) TargetResolver(_ string) string { return p.advertise }

func (p *PushNetwork) Start(ctx context.Context) error {
	ln := p.listener
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || len(body) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		raw := string(body)
		for _, cb := range p.snapshot() {
			cb(raw)
		}
		// Taken, not done: the message is the server's offer, and the work it
		// starts reports back through the protocol like any other.
		w.WriteHeader(http.StatusOK)
	})
	p.server = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := p.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("push listener %s: %v\n", p.advertise, err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = p.Stop()
	}()
	return nil
}

func (p *PushNetwork) Stop() error {
	p.mu.Lock()
	p.subscribers = nil
	srv := p.server
	p.server = nil
	p.mu.Unlock()
	if srv == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}

func (p *PushNetwork) Send(ctx context.Context, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/", strings.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
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

func (p *PushNetwork) Recv(cb func(raw string)) {
	if cb == nil {
		return
	}
	p.mu.Lock()
	p.subscribers = append(p.subscribers, cb)
	p.mu.Unlock()
}

func (p *PushNetwork) snapshot() []func(raw string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]func(raw string){}, p.subscribers...)
}
