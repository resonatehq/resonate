package model

import (
	"strings"
	"testing"
)

func TestResponseProjectionMatchesTheSpecRecords(t *testing.T) {

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

func TestUnknownFenceActionIsADecodeError(t *testing.T) {
	const line = `{"kind":"task.fence","now":1,` +
		`"req":{"id":"t","version":1,"action":{"kind":"promise.explode","data":{"id":"q"}}},` +
		`"res":{"head":{"status":200}}}`
	if _, _, err := LoadTrace(strings.NewReader(line)); err == nil {
		t.Fatal("an unsupported fence action decoded silently; Lean rejects it")
	}
}
