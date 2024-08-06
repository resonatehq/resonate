package task

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/message"
)

type Task struct {
	Id          int64            `json:"id"`
	State       State            `json:"state"`
	Message     *message.Message `json:"message"`
	Timeout     int64            `json:"timeout"`
	Counter     int              `json:"counter"`
	Frequency   int              `json:"frequency"`
	Expiration  int64            `json:"expiration"`
	CreatedOn   *int64           `json:"createdOn"`
	CompletedOn *int64           `json:"completedOn"`
}

func (t *Task) String() string {
	return fmt.Sprintf(
		"Task(id=%d, state=%s, message=%s, timeout=%d, counter=%d, frequency=%d, expiration=%d)",
		t.Id,
		t.State,
		t.Message,
		t.Timeout,
		t.Counter,
		t.Frequency,
		t.Expiration,
	)
}

func (t1 *Task) Equals(t2 *Task) bool {
	// for dst only
	return t1.Id == t2.Id && t1.State == t2.State
}

type State int

const (
	Init      State = 1 << iota // 1
	Enqueued                    // 2
	Claimed                     // 4
	Completed                   // 8
	Timedout                    // 16
)

func (s State) String() string {
	switch s {
	case Init:
		return "INIT"
	case Enqueued:
		return "ENQUEUED"
	case Claimed:
		return "CLAIMED"
	case Completed:
		return "COMPLETED"
	case Timedout:
		return "TIMEDOUT"
	default:
		panic("invalid state")
	}
}

func (s State) In(mask State) bool {
	return s&mask != 0
}
