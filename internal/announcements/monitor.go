package announcements

import (
	"fmt"
)

type MonitorInterface interface {
	Apply(event Event)
	Status() []error
}

type Monitor struct {
	errors []error
}

func NewMonitor() *Monitor {
	return &Monitor{}
}

func (m *Monitor) Apply(event Event) {
	switch event.Type {
	case "HTTPResponse":
		statusCode, err := event.Get("StatusCode")
		if err != nil {
			m.errors = append(m.errors, fmt.Errorf("error getting StatusCode from event: %v", err))
			return
		}
		fmt.Printf("Received HTTP response with status code: %v\n", statusCode)
		// Add more handling logic as needed
	case "Flush":
		fmt.Printf("Received flush event at time: %v\n", event.Data["time"])
		// Add more handling logic as needed
	default:
		m.errors = append(m.errors, fmt.Errorf("unknown event type: %v", event.Type))
	}
}

func (m *Monitor) Status() []error {
	return m.errors
}

func (m *Monitor) Listen() {
	// Get the announcement instance
	announcement := GetInstance()

	// Start an infinite loop to continuously listen to announcements
	for {
		// Check the type of announcement and handle it accordingly
		switch announcement := announcement.(type) {
		case *DstAnnouncement:
			// Process DstAnnouncement events
			events := announcement.GetAnnouncements()
			for _, event := range events {
				m.Apply(event)
			}
		case *NoopAnnouncement:
			// Do nothing for NoopAnnouncement
		default:
			fmt.Println("Unknown announcement type")
		}
	}
}

// handleDstAnnouncement is a helper function to handle DstAnnouncement events.
func (m *Monitor) handleDstAnnouncement(event Event) {
	switch event.Type {
	case "HTTPResponse":
		statusCode, err := event.Get("StatusCode")
		if err != nil {
			fmt.Println("Error getting StatusCode from event:", err)
			return
		}
		fmt.Println("Received HTTP response with status code:", statusCode)
		// Add more handling logic as needed
	case "Flush":
		fmt.Println("Received flush event at time:", event.Data["time"])
	default:
		fmt.Println("Unknown event type:", event.Type)
	}
}
