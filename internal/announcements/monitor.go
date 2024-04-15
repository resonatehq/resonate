package announcements

import (
	"fmt"
)

// Monitor is a simple struct representing an announcement monitor.
type Monitor struct{}

// NewMonitor creates a new instance of Monitor.
func NewMonitor() *Monitor {
	return &Monitor{}
}

// Listen starts listening to announcements and handles them accordingly.
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
				m.handleDstAnnouncement(event)
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
	// Example: Routing logic based on event type
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
