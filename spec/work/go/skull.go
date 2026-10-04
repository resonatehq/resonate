package main

import (
	"encoding/json"
	"net"
	"os"
	"sync"
)

// Streaming the trace out of a skulld guest.
//
// Under skulld every container has the agent's socket at
// /run/skull/libskull.sock (skulld mounts /run/skull into each one). One
// SOCK_SEQPACKET packet is one message: a type byte, then the payload, and
// `J` carries a JSON event. That is all libskull.so does for `skull_emit`, so
// a Go binary can speak it without cgo.
//
// The agent forwards every `J` event to the host, where it lands in the run's
// events.jsonl on the virtual clock. Each recorded request/response pair goes
// out as one `{"resonate_trace": …}` event the moment it completes, so a
// workload killed at the run's deadline has still delivered everything it
// saw, and the checkers run on the host afterwards.
const skullSocket = "/run/skull/libskull.sock"

type skullSink struct {
	mu   sync.Mutex
	conn *net.UnixConn
}

// dialSkull connects to the agent, or returns nil outside a skulld guest.
func dialSkull() *skullSink {
	if _, err := os.Stat(skullSocket); err != nil {
		return nil
	}
	conn, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: skullSocket, Net: "unixpacket"})
	if err != nil {
		return nil
	}
	return &skullSink{conn: conn}
}

func (s *skullSink) emit(event any) {
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.conn.Write(append([]byte{'J'}, payload...))
}

// traceRow is one event as the host-side checkers want it: the NDJSON fields
// (kind, now, req, res) and the history fields (call, return, client), plus
// the invocation it came from.
type traceRow struct {
	Invocation string          `json:"invocation"`
	Kind       string          `json:"kind"`
	Now        uint64          `json:"now"`
	Req        json.RawMessage `json:"req"`
	Res        json.RawMessage `json:"res"`
	Client     string          `json:"client"`
	Call       int64           `json:"call"`
	Return     int64           `json:"return"`
	Ambiguous  bool            `json:"ambiguous,omitempty"`
}

func (s *skullSink) trace(invocation string, e Event) {
	s.emit(map[string]any{"resonate_trace": traceRow{
		Invocation: invocation, Kind: e.Kind, Now: e.Now, Req: e.Req, Res: e.Res,
		Client: e.Client, Call: e.Call, Return: e.Return, Ambiguous: e.Ambiguous,
	}})
}
