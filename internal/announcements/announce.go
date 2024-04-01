package announcements

import "sync"

type Announcement interface {
	Announce(data map[string]string)
}

// NopAnnouncement does nothing when announcements are made.
type NopAnnouncement struct{}

// DstAnnouncement stores announcements for later verification.
type DstAnnouncement struct {
	announcements []map[string]string
	mu            sync.Mutex // Mutex for thread safety
}

var (
	instance Announcement
	once     sync.Once
)

// EnvironmentType represents different environments.
type EnvironmentType int

// Environment types.
const (
	Nop EnvironmentType = iota
	Dst
)

// Initialize initializes the announcement system based on the environment type.
func Initialize(envType EnvironmentType) {
	once.Do(func() {
		switch envType {
		case Nop:
			instance = &NopAnnouncement{}
		case Dst:
			instance = &DstAnnouncement{}
		default:
			panic("Invalid environment type.")
		}
	})
}

func GetInstance() Announcement {
	return instance
}

func (n *NopAnnouncement) Announce(data map[string]string) {
	// Do nothing
}

func (d *DstAnnouncement) Announce(data map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.announcements = append(d.announcements, data)
}
