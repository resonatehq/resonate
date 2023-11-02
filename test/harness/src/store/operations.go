package store

import "time"

type Status int

const (
	Invoke Status = iota
	Ok
	Fail
)

type API int

const (
	Search API = iota
	Get
	Create
	Cancel
	Resolve
	Reject
)

type Operation struct {
	ID string

	Invoker string      // Process/thread that invoked the op
	API     API         // Function name
	Value   interface{} // Value holds arguments and results to operations, --  nil before reading, writing doesn't matter -- maybe split

	StartTime time.Time // When the operation was started
	EndTime   time.Time // When the operation completed
	Status    Status    // Type of operation [Invoke | Ok | Fail]
}
