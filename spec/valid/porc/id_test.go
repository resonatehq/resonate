package model

import "testing"

func targeted(origin string) Tags {
	return Tags{"resonate:target": "poll://any@w1", "resonate:origin": origin}
}

func TestOriginSchemaIsEnforced(t *testing.T) {
	cases := []struct {
		name string
		id   string
		tags Tags
		want int
	}{
		{"id prefixed by its origin", "a:1", Tags{"resonate:origin": "a"}, 200},
		{"id equal to its origin", "a", Tags{"resonate:origin": "a"}, 200},
		{"no origin tag at all", "loose", Tags{}, 200},
		{"id not prefixed by its origin", "b:1", Tags{"resonate:origin": "a"}, 400},
		{"origin carrying a colon", "a:b:1", Tags{"resonate:origin": "a:b"}, 400},
	}
	for _, c := range cases {
		s := &ServerState{}
		got := s.PromiseCreate(Projected, PromiseCreateReq{c.id, 9_000_000, c.tags, nil}, 1000).Status
		if got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}

func TestCrossOriginIsRejected(t *testing.T) {
	s := &ServerState{}
	s.PromiseCreate(Projected, PromiseCreateReq{"o:root", 9_000_000, targeted("o"), nil}, 1000)
	s.PromiseCreate(Projected, PromiseCreateReq{"o:kid", 9_000_000, Tags{"resonate:external": "true"}, nil}, 1000)
	s.PromiseCreate(Projected, PromiseCreateReq{"other:kid", 9_000_000, Tags{"resonate:external": "true"}, nil}, 1000)
	s.TaskAcquire(Projected, "o:root", 0, "w1", 5_000, 1000)

	if got := s.PromiseRegisterCallback(Projected, "o:kid", "o:root", 1000).Status; got != 200 {
		t.Errorf("same-origin callback: got %d, want 200", got)
	}
	if got := s.PromiseRegisterCallback(Projected, "other:kid", "o:root", 1000).Status; got != 400 {
		t.Errorf("cross-origin callback: got %d, want 400", got)
	}
	if got := s.TaskSuspend(Projected, "o:root", 1, []string{"other:kid"}, 1000).Status; got != 400 {
		t.Errorf("suspend awaiting a foreign origin: got %d, want 400", got)
	}
	refs := []TaskRef{{ID: "o:root", Version: 1}, {ID: "other:root", Version: 1}}
	if got := s.TaskHeartbeat(Projected, "w1", refs, 1000).Status; got != 400 {
		t.Errorf("heartbeat across origins: got %d, want 400", got)
	}
}

func TestGeneratorEmitsOffSchemaOps(t *testing.T) {
	want := map[string]bool{
		"promise.create": false, "promise.register_callback": false,
		"task.suspend": false, "task.heartbeat": false,
	}
	for seed := 1; seed <= 60; seed++ {
		g := NewGen(int64(seed), "o1")
		for _, st := range g.Script(120) {
			if st.op != nil && rejectedBySchema(*st.op) {
				want[st.op.Kind] = true
			}
		}
	}
	for kind, seen := range want {
		if !seen {
			t.Errorf("no off-schema %s was ever generated; the backend is never asked to reject one", kind)
		}
	}
}
