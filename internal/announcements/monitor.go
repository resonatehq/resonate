// monitor.go

package announcements

type Monitors interface {
	Apply(event Event)
	Status() []error
}
