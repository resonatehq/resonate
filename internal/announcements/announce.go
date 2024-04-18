package announcements

import (
	"fmt"
	"sync"
)

type Event struct {
	Type string
	Data map[string]interface{}
}

func NewEvent(eventType string, initialData ...map[string]interface{}) *Event {
	var data map[string]interface{}
	if len(initialData) > 0 {
		data = initialData[0] // Use the first map provided if any.
	} else {
		data = make(map[string]interface{})
	}

	return &Event{
		Type: eventType,
		Data: data,
	}
}

func (e *Event) Set(key string, value interface{}) {
	e.Data[key] = value
}

func (e *Event) Get(key string) (interface{}, error) {
	value, exists := e.Data[key]
	if !exists {
		return nil, fmt.Errorf("key '%s' does not exist in event of type '%s'", key, e.Type)
	}
	return value, nil
}

type Announcement interface {
	Announce(event *Event)
	Register(monitor *Monitor)
}

type NoopAnnouncement struct{}

type DstAnnouncement struct {
	announcements []Event
	monitors      []*Monitor // Slice to store registered monitors
	mutex         sync.Mutex // Mutex for thread safety
}

var (
	instance Announcement
	once     sync.Once
)

type EnvironmentType int

const (
	Noop EnvironmentType = iota
	Dst
)

func Initialize(envType EnvironmentType) {
	once.Do(func() {
		switch envType {
		case Noop:
			instance = &NoopAnnouncement{}
		case Dst:
			instance = &DstAnnouncement{
				announcements: make([]Event, 0, 100), // Preallocate capacity to prevent frequent reallocations
				monitors:      make([]*Monitor, 0),
			}
		default:
			panic("Invalid environment type.")
		}
	})
}

func GetInstance() Announcement {
	// check if the instance has been initialized
	return instance
}

func (n *NoopAnnouncement) Announce(event *Event) {
	// Do nothing
}

func (d *DstAnnouncement) Announce(event *Event) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.announcements = append(d.announcements, *event)
	// Print the announcement
	fmt.Println("Announcement:", event.Type, event.Data)
	// Apply the event to all registered monitors
	for _, monitor := range d.monitors {
		monitor.Apply(*event)
	}
}

func (n *NoopAnnouncement) Register(monitor *Monitor) {
	// Do nothing
}

func (d *DstAnnouncement) Register(monitor *Monitor) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.monitors = append(d.monitors, monitor)
}

func (d *DstAnnouncement) GetAnnouncements() []Event {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	// Return a copy of the announcements slice to ensure thread safety
	announcementsCopy := make([]Event, len(d.announcements))
	copy(announcementsCopy, d.announcements)
	return announcementsCopy
}
