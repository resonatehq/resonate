package checker

import "sync"

// Timeline renders an HTML timeline of a history.
type Timeline struct {
	mu      sync.Mutex
	history map[string]interface{}
}

func NewTimeline() Timeline {
	return Timeline{
		mu:      sync.Mutex{},
		history: make(map[string]interface{}, 0),
	}
}

func HTML() string {
	return ""
}
