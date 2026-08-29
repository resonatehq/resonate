package model

import "testing"

func TestValidationOutranksExistence(t *testing.T) {
	const ghost = "o:ghost"

	cases := []struct {
		name string
		run  func(*ServerState, Discipline) Response
	}{
		{"promise.settle to a non-settable state",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseSettle(d, ghost, Pending, nil, 100)
			}},
		{"promise.settle to rejected_timedout (server-owned)",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseSettle(d, ghost, RejectedTimedout, nil, 100)
			}},
		{"promise.register_callback awaiting itself",
			func(s *ServerState, d Discipline) Response {
				return s.PromiseRegisterCallback(d, ghost, ghost, 100)
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
				return s.TaskSuspend(d, ghost, 0, []string{"o:a", "o:a"}, 100)
			}},
		{"task.fulfill to a non-settable state",
			func(s *ServerState, d Discipline) Response {
				return s.TaskFulfill(d, ghost, 0, Pending, nil, 100)
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

func TestWellFormedRequestsStillReport404(t *testing.T) {
	const ghost = "o:ghost"

	cases := []struct {
		name string
		run  func(*ServerState, Discipline) Response
		want int
	}{
		{"promise.get", func(s *ServerState, d Discipline) Response {
			return s.PromiseGet(d, ghost, 100)
		}, 404},
		{"promise.settle to resolved", func(s *ServerState, d Discipline) Response {
			return s.PromiseSettle(d, ghost, Resolved, nil, 100)
		}, 404},
		{"promise.register_listener", func(s *ServerState, d Discipline) Response {
			return s.PromiseRegisterListener(d, ghost, "poll://any@w1", 100)
		}, 404},
		{"task.get", func(s *ServerState, d Discipline) Response {
			return s.TaskGet(d, ghost, 100)
		}, 404},
		{"task.suspend on a distinct awaited", func(s *ServerState, d Discipline) Response {
			return s.TaskSuspend(d, ghost, 0, []string{"o:a"}, 100)
		}, 404},
		{"task.fulfill to resolved", func(s *ServerState, d Discipline) Response {
			return s.TaskFulfill(d, ghost, 0, Resolved, nil, 100)
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
