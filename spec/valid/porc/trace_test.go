package model

import (
	"strings"
	"testing"
)

// The two checkers must agree on what the EVIDENCE is before they can
// agree on a verdict.
//
// valid/lean/json.lean decodes each response into the specification's
// kind-specific record: `TaskGetRes` carries a task and no promise,
// `TaskSuspendRes` carries neither. A server is free to return more. If
// this loader kept what the specification drops, the Go checker would
// compare fields the Lean checker never sees and refute traces Lean
// accepts — a disagreement about the reading, not about the server.
func TestResponseProjectionMatchesTheSpecRecords(t *testing.T) {
	// Every line carries BOTH a promise and a task in `data`, which is
	// more than any of these kinds models.
	const extra = `"promise":{"id":"p","state":"pending","timeoutAt":10,"createdAt":0},` +
		`"task":{"id":"p","state":"claimed","version":3}`

	cases := []struct {
		kind          string
		promise, task bool
	}{
		{"promise.get", true, false},
		{"promise.create", true, false},
		{"promise.settle", true, false},
		{"promise.register_callback", true, false},
		{"promise.register_listener", true, false},
		{"task.get", false, true},
		{"task.acquire", true, true},
		{"task.fulfill", true, false},
		{"task.suspend", false, false},
		{"task.release", false, false},
		{"task.halt", false, false},
		{"task.continue", false, false},
		{"task.heartbeat", false, false},
	}

	for _, c := range cases {
		line := `{"kind":"` + c.kind + `","now":1,"req":{"id":"p","version":0,"pid":"w","ttl":5,` +
			`"awaited":"p","address":"poll://a@w","actions":[{"data":{"awaited":"q","awaiter":"p"}}],` +
			`"action":{"kind":"promise.settle","data":{"id":"q","state":"resolved"}},` +
			`"tasks":[{"id":"p","version":0}],"state":"resolved"},` +
			`"res":{"head":{"status":200},"data":{` + extra + `}}}`
		_, resps, err := LoadTrace(strings.NewReader(line))
		if err != nil {
			t.Fatalf("%s: %v", c.kind, err)
		}
		got := resps[0]
		if (got.Promise != nil) != c.promise {
			t.Errorf("%s: promise carried=%v, want %v", c.kind, got.Promise != nil, c.promise)
		}
		if (got.Task != nil) != c.task {
			t.Errorf("%s: task carried=%v, want %v", c.kind, got.Task != nil, c.task)
		}
	}
}

// `TaskFenceAction` has exactly two constructors. valid/lean/json.lean throws
// on anything else; so must this, or a corrupt file is a decode error to
// one checker and a plain 400 to the other.
func TestUnknownFenceActionIsADecodeError(t *testing.T) {
	const line = `{"kind":"task.fence","now":1,` +
		`"req":{"id":"t","version":1,"action":{"kind":"promise.explode","data":{"id":"q"}}},` +
		`"res":{"head":{"status":200}}}`
	if _, _, err := LoadTrace(strings.NewReader(line)); err == nil {
		t.Fatal("an unsupported fence action decoded silently; Lean rejects it")
	}
}
