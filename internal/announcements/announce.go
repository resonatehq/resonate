package announcements

import (
	"fmt"
	"sync"

	"github.com/resonatehq/resonate/internal/util"
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
}

type NoopAnnouncement struct{}

type DstAnnouncement struct {
	announcements []Event
	mutex         sync.Mutex // Mutex for thread safety
}

var (
	instance Announcement
	once     sync.Once
)

type EnvironmentType int

const (
	Nop EnvironmentType = iota
	Dst
)

func Initialize(envType EnvironmentType) {
	once.Do(func() {
		switch envType {
		case Nop:
			instance = &NoopAnnouncement{}
		case Dst:
			instance = &DstAnnouncement{
				announcements: make([]Event, 0, 100), // Preallocate capacity to prevent frequent reallocations
			}
		default:
			panic("Invalid environment type.")
		}
	})
}

func GetInstance() Announcement {
	// check if the instance has been initialized
	util.Assert(instance != nil, "Announcement instance has not been initialized.")
	return instance
}

func (n *NoopAnnouncement) Announce(event *Event) {
	// Do nothing
}

func (d *DstAnnouncement) Announce(event *Event) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.announcements = append(d.announcements, *event)
}
