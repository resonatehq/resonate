package task

import (
	"encoding/json"
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
)

type Task struct {
	Id          string          `json:"id"`
	Counter     int             `json:"counter"`
	Timeout     int64           `json:"timeout"`
	ProcessId   *string         `json:"processId,omitempty"`
	State       State           `json:"-"`
	Recv        json.RawMessage `json:"-"`
	Mesg        *message.Mesg   `json:"-"`
	Attempt     int             `json:"-"`
	Frequency   int             `json:"-"`
	Expiration  int64           `json:"-"`
	CreatedOn   *int64          `json:"createdOn,omitempty"`
	CompletedOn *int64          `json:"completedOn,omitempty"`
}

func (t *Task) String() string {
	return fmt.Sprintf(
		"Task(id=%s, processId=%s, state=%s, recv=%s, mesg=%v, timeout=%d, counter=%d, attempt=%d, frequency=%d, expiration=%d, createdOn=%d, completedOn=%d)",
		t.Id,
		util.SafeDeref(t.ProcessId),
		t.State,
		t.Recv,
		t.Mesg,
		t.Timeout,
		t.Counter,
		t.Attempt,
		t.Frequency,
		t.Expiration,
		util.SafeDeref(t.CreatedOn),
		util.SafeDeref(t.CompletedOn),
	)
}

func (t1 *Task) Equals(t2 *Task) bool {
	// for dst only
	return t1.Id == t2.Id && t1.State == t2.State && t1.Counter == t2.Counter
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
