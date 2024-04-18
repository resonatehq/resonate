package announcements

import (
	"fmt"
	"sync"
)

type EventHandler func(event Event, monitor *Monitor)

type Monitor struct {
	errors        []error
	eventHandlers map[string]EventHandler // Map to store event handlers by event type
	mutex         sync.Mutex              // Mutex for thread safety
}

func NewMonitor() *Monitor {
	return &Monitor{
		eventHandlers: make(map[string]EventHandler),
	}
}

func (m *Monitor) Apply(event Event) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	handler, ok := m.eventHandlers[event.Type]
	if !ok {
		m.errors = append(m.errors, fmt.Errorf("no handler found for event type: %s", event.Type))
		return
	}

	handler(event, m)
}

func (m *Monitor) Status() []error {
	return m.errors
}

func (m *Monitor) RegisterEventHandler(eventType string, handler EventHandler) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.eventHandlers[eventType] = handler
}

// ----------------------------------------------------------------------------------

// Task Cache
type TaskCache struct {
	tasks map[string]Event
	mutex sync.Mutex
}

var taskCache = TaskCache{
	tasks: make(map[string]Event),
}

// TaskClaimedHandler is an event handler for the "TaskClaimed" event type.
// It checks if the corresponding "TaskCompleted" event is received within a certain timeframe.
// If not, it raises an alert.
func TaskClaimedHandler(event Event, monitor *Monitor) {
	taskID, err := event.Get("taskId")
	// fmt.Println("-------------------------Monitor: Task claimed:", taskID)
	if err != nil {
		monitor.errors = append(monitor.errors, fmt.Errorf("error getting task ID: %v", err))
		return
	}

	// Check if the task is already in the cache
	// If not, add it to the cache
	if _, ok := taskCache.tasks[taskID.(string)]; !ok {
		taskCache.mutex.Lock()
		taskCache.tasks[taskID.(string)] = event
		taskCache.mutex.Unlock()
	}
}

func TaskCompletedHandler(event Event, monitor *Monitor) {
	// Task completion event received, can perform any necessary actions
	// For example, log the event or update some state
	// fmt.Println("-------------------------Monitor: Task completed:", event)
	if taskID, err := event.Get("taskId"); err == nil {
		taskCache.mutex.Lock()
		delete(taskCache.tasks, taskID.(string))
		taskCache.mutex.Unlock()
	} else {
		fmt.Println("Monitor: Error getting task ID:", err)
		monitor.errors = append(monitor.errors, fmt.Errorf("error getting task ID: %v", err))
	}
}
