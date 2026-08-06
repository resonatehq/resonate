package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// debugStart pauses the server's background loops and enables
// `resonate:debug_time`.
//
// Without it the server judges deadlines against wall clock — ~1.7e12 —
// while promises carry small `timeoutAt` values, and whatever was pending
// when a sweep fired gets expired. That mismatch is the entire cause of
// the one REFUTED capture in valid/traces, and the banner "Debug mode
// enabled — background loops paused" prints unconditionally at startup
// WITHOUT pausing anything: only this call does.
func debugStart(url string) error {
	for _, kind := range []string{"debug.start", "debug.reset"} {
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
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("%s: HTTP %d: %s", kind, resp.StatusCode, raw)
		}
	}
	return nil
}
