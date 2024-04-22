package monitors

import (
	"fmt"
	"sync"

	"github.com/resonatehq/resonate/internal/announcements"
)

type taskMonitor struct {
	eventTypeCount map[string]int // Map to count occurrences of each event type
	mutex          sync.Mutex     // Mutex for thread safety
}

func NewTaskMonitor() *taskMonitor {
	return &taskMonitor{
		eventTypeCount: make(map[string]int),
	}
}

func (tm *taskMonitor) Apply(event announcements.Event) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Increment event type count
	tm.eventTypeCount[event.Type]++

	// If the event type is TaskCreated or TaskCompleted, print the event
	if event.Type == "TaskCreated" || event.Type == "TaskCompleted" {
		fmt.Println("Received event:", event.Type, event.Data)
	}
}

// Status returns any errors or status information associated with the taskMonitor.
func (tm *taskMonitor) Status() []error {
	return nil
}
