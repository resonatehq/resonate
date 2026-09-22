package model

import (
	"strings"
	"testing"

	"github.com/anishathalye/porcupine"
)

func loadPending(t *testing.T, ndjson string) ([]Op, []Response) {
	t.Helper()
	ops, resps, err := LoadTrace(strings.NewReader(strings.TrimSpace(ndjson) + "\n"))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return ops, resps
}

const pendingCreate = `{"kind":"promise.create","now":1000,"req":{"id":"wf","timeoutAt":700000,"param":{},"tags":{"resonate:target":"poll://any@default"}},"res":{"kind":"promise.create","head":{"corrId":"t1","status":200,"version":"v"},"data":{"promise":{"id":"wf","state":"pending","param":{},"value":{},"tags":{"resonate:target":"poll://any@default"},"timeoutAt":700000,"createdAt":1000}}}}
{"kind":"task.acquire","now":1010,"req":{"id":"wf","version":0,"pid":"pa","ttl":300000},"res":{"kind":"task.acquire","head":{"corrId":"t2","status":500,"version":"v"},"data":"outcome unknowable"}}`

func TestPendingAppliedBranchAccepted(t *testing.T) {
	ops, resps := loadPending(t, pendingCreate+`
{"kind":"task.get","now":1020,"req":{"id":"wf"},"res":{"kind":"task.get","head":{"corrId":"t3","status":200,"version":"v"},"data":{"task":{"id":"wf","state":"acquired","version":1,"resumes":0,"pid":"pa","ttl":300000}}}}`)
	for _, d := range disciplines {
		if r := check(t, d, ops, resps, 1, true); r != porcupine.Ok {
			t.Fatalf("%v: applied branch refused: %v", d, r)
		}
		w, at, ok := Replay(d, ops, resps)
		if !ok {
			t.Fatalf("%v: replay failed at %d", d, at)
		}
		if !containsNote(w, "applied, response unobserved") {
			t.Fatalf("%v: witness does not name the applied pending op: %v", d, w)
		}
	}
}

func TestPendingNotAppliedBranchAccepted(t *testing.T) {
	ops, resps := loadPending(t, pendingCreate+`
{"kind":"task.get","now":1020,"req":{"id":"wf"},"res":{"kind":"task.get","head":{"corrId":"t3","status":200,"version":"v"},"data":{"task":{"id":"wf","state":"pending","version":0,"resumes":0}}}}`)
	for _, d := range disciplines {
		if r := check(t, d, ops, resps, 1, true); r != porcupine.Ok {
			t.Fatalf("%v: not-applied branch refused: %v", d, r)
		}
	}
}

func TestPendingStillRefutesTheImpossible(t *testing.T) {
	ops, resps := loadPending(t, pendingCreate+`
{"kind":"task.get","now":1020,"req":{"id":"wf"},"res":{"kind":"task.get","head":{"corrId":"t3","status":200,"version":"v"},"data":{"task":{"id":"wf","state":"acquired","version":1,"resumes":0,"pid":"px","ttl":300000}}}}`)
	for _, d := range disciplines {
		if r := check(t, d, ops, resps, 1, true); r != porcupine.Illegal {
			t.Fatalf("%v: impossible pid accepted under a pending op: %v", d, r)
		}
	}
}

func TestPendingRelease(t *testing.T) {
	ops, resps := loadPending(t, pendingCreate[:strings.Index(pendingCreate, "\n")]+`
{"kind":"task.acquire","now":1010,"req":{"id":"wf","version":0,"pid":"pa","ttl":300000},"res":{"kind":"task.acquire","head":{"corrId":"t2","status":200,"version":"v"},"data":{"task":{"id":"wf","state":"acquired","version":1,"resumes":0,"pid":"pa","ttl":300000},"promise":{"id":"wf","state":"pending","param":{},"value":{},"tags":{"resonate:target":"poll://any@default"},"timeoutAt":700000,"createdAt":1000}}}}
{"kind":"task.release","now":1020,"req":{"id":"wf","version":1},"res":{"kind":"task.release","head":{"corrId":"t3","status":500,"version":"v"},"data":"outcome unknowable"}}
{"kind":"task.get","now":1030,"req":{"id":"wf"},"res":{"kind":"task.get","head":{"corrId":"t4","status":200,"version":"v"},"data":{"task":{"id":"wf","state":"pending","version":1,"resumes":0}}}}`)
	for _, d := range disciplines {
		if r := check(t, d, ops, resps, 1, true); r != porcupine.Ok {
			t.Fatalf("%v: landed release under a pending op refused: %v", d, r)
		}
	}
}

func containsNote(w []string, substr string) bool {
	for _, s := range w {
		if strings.Contains(s, substr) {
			return true
		}
	}
	return false
}
