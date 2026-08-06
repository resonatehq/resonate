package model

import "testing"

// Validation outranks existence.
//
// `spec/01-protocol` states it and `spec/02-abstract/p.lean` implements
// it: in every handler that can return 400, the 400 guard is the FIRST
// statement, before any `viewPromise`/`viewTask`. So a request that is
// both malformed AND aimed at an object that does not exist must answer
// 400, never 404.
//
// That ordering is observable — it is the difference between "your request
// is wrong" and "your object is missing" — so it is part of the protocol,
// not an implementation detail. This model is meant to follow the Lean to
// the letter, so the ordering is pinned here rather than left to a reading
// of the code.
//
// Each case below targets `ghost`, which is never created.
func TestValidationOutranksExistence(t *testing.T) {
	const ghost = "o.ghost"

	cases := []struct {
		name string
		run  func(*ServerState, Discipline) Response
	}{
		{"promise.settle to a non-settable state",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseSettle(d, ghost, Pending, 100)
			}},
		{"promise.settle to rejected_timedout (server-owned)",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseSettle(d, ghost, RejectedTimedout, 100)
			}},
		{"promise.register_callback awaiting itself",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseRegisterCallback(d, ghost, ghost, 100)
			}},
		{"promise.register_listener with an undeliverable address",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseRegisterListener(d, ghost, "nonsense", 100)
			}},
		{"task.suspend with an empty awaited list",
			func(s *ServerState, d Discipline) Response {
				return s.TaskSuspend(d, ghost, 0, nil, 100)
			}},
		{"task.suspend awaiting itself",
			func(s *ServerState, d Discipline) Response {
				return s.TaskSuspend(d, ghost, 0, []string{ghost}, 100)
			}},
		{"task.suspend with a duplicate awaited",
			func(s *ServerState, d Discipline) Response {
				return s.TaskSuspend(d, ghost, 0, []string{"o.a", "o.a"}, 100)
			}},
		{"task.fulfill to a non-settable state",
			func(s *ServerState, d Discipline) Response {
				return s.TaskFulfill(d, ghost, 0, Pending, 100)
			}},
	}

	for _, c := range cases {
		for _, d := range disciplines {
			s := &ServerState{}
			got := c.run(s, d)
			if got.Status != 400 {
				t.Errorf("%s / %v: got %d, want 400 — validation must outrank existence",
					c.name, d, got.Status)
			}
		}
	}
}

// The converse, so the test above cannot pass by a handler that returns
// 400 for everything: a WELL-FORMED request at a missing object is 404.
func TestWellFormedRequestsStillReport404(t *testing.T) {
	const ghost = "o.ghost"

	cases := []struct {
		name string
		run  func(*ServerState, Discipline) Response
		want int
	}{
		{"promise.get", func(s *ServerState, d Discipline) Response {
			return s.PromiseGet(d, ghost, 100)
		}, 404},
		{"promise.settle to resolved", func(s *ServerState, d Discipline) Response {
			return s.PromiseSettle(d, ghost, Resolved, 100)
		}, 404},
		{"promise.register_listener with a valid address", func(s *ServerState, d Discipline) Response {
			return s.PromiseRegisterListener(d, ghost, "poll://any@w1", 100)
		}, 404},
		{"task.get", func(s *ServerState, d Discipline) Response {
			return s.TaskGet(d, ghost, 100)
		}, 404},
		{"task.suspend on a distinct awaited", func(s *ServerState, d Discipline) Response {
			return s.TaskSuspend(d, ghost, 0, []string{"o.a"}, 100)
		}, 404},
		{"task.fulfill to resolved", func(s *ServerState, d Discipline) Response {
			return s.TaskFulfill(d, ghost, 0, Resolved, 100)
		}, 404},
	}

	for _, c := range cases {
		for _, d := range disciplines {
			s := &ServerState{}
			if got := c.run(s, d); got.Status != c.want {
				t.Errorf("%s / %v: got %d, want %d", c.name, d, got.Status, c.want)
			}
		}
	}
}
