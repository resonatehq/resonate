package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// debugStart pauses the server's background loops and enables
// `resonate:debug_time`.
//
// Without it the server judges deadlines against wall clock — ~1.7e12 —
// while promises carry small `timeoutAt` values, and whatever was pending
// when a sweep fired gets expired. That mismatch is the entire cause of
// the one REFUTED capture in the original corpus, and the banner "Debug mode
// enabled — background loops paused" prints unconditionally at startup
// WITHOUT pausing anything: only this call does.
func debugStart(url string, reset bool) error {
	kinds := []string{"debug.start"}
	if reset {
		kinds = append(kinds, "debug.reset")
	}
	for _, kind := range kinds {
		body, _ := json.Marshal(map[string]any{
			"kind": kind,
			"head": map[string]any{"corrId": "scenarios", "version": "2026-04-01"},
			"data": map[string]any{},
		})
		resp, err := http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			return err
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		// Current servers make debug a startup flag — `debug.start` does not
		// exist, and a server started with it is already paused. Only a
		// server that knows the kind and refuses it is an error.
		if kind == "debug.start" && resp.StatusCode == http.StatusBadRequest && bytes.Contains(raw, []byte("Unknown operation")) {
			continue
		}
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("%s: HTTP %d: %s", kind, resp.StatusCode, raw)
		}
	}
	return nil
}

// debugTicker moves the server's clock to the recorder's, every `every`,
// until ctx ends.
//
// Under the debug flag nothing on the server runs on wall time: a deadline
// fires when the caller's clock passes it, and that clock only moves when a
// request carries it. Workers blocked on a durable sleep send nothing, so
// without this their timers would never come due. A tick is an internal step
// in the specification's sense — the checkers recover those themselves — so
// it is not recorded.
func debugTicker(ctx context.Context, urls []string, rec *Recorder, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Exclusive: no request is between its stamp and its answer while the
		// servers' clocks move (see Recorder.inFlight).
		rec.inFlight.Lock()
		now := rec.Now()
		body, _ := json.Marshal(map[string]any{
			"kind": "debug.tick",
			"head": map[string]any{"corrId": "tick", "version": "2026-04-01", "resonate:debug_time": now},
			"data": map[string]any{"time": now},
		})
		// Every server: each keeps its own clock under the debug flag.
		for _, url := range urls {
			resp, err := http.Post(url, "application/json", bytes.NewReader(body))
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
		rec.inFlight.Unlock()
	}
}
