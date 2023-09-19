package promise

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Promise struct {
	Id          string            `json:"id"`
	State       State             `json:"state"`
	Param       Value             `json:"param,omitempty"`
	Value       Value             `json:"value,omitempty"`
	Timeout     int64             `json:"timeout"`
	CreatedOn   *int64            `json:"createdOn,omitempty"`
	CompletedOn *int64            `json:"completedOn,omitempty"`
	Tags        map[string]string `json:"tags"`
}

func (p *Promise) String() string {
	return fmt.Sprintf(
		"Promise(id=%s, state=%s, param=%s, value=%s, timeout=%d)",
		p.Id,
		p.State,
		&p.Param,
		&p.Value,
		p.Timeout,
	)
}

type State int

const (
	Pending State = 1 << iota
	Resolved
	Rejected
	Timedout
	Canceled
)

func (s State) String() string {
	switch s {
	case Pending:
		return "PENDING"
	case Resolved:
		return "RESOLVED"
	case Rejected:
		return "REJECTED"
	case Timedout:
		return "REJECTED_TIMEDOUT"
	case Canceled:
		return "REJECTED_CANCELED"
	default:
		panic("invalid state")
	}
}

func (s *State) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *State) UnmarshalJSON(data []byte) error {
	var state string
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	switch strings.ToUpper(state) {
	case "Pending":
		*s = Pending
	case "RESOLVED":
		*s = Resolved
	case "REJECTED":
		*s = Rejected
	case "REJECTED_TIMEDOUT":
		*s = Timedout
	case "REJECTED_CANCELED":
		*s = Canceled
	default:
		return fmt.Errorf("invalid state '%s'", state)
	}

	return nil
}

func (s State) In(mask State) bool {
	return s&mask != 0
}

type Value struct {
	Headers map[string]string `json:"headers,omitempty"`
	Ikey    *Ikey             `json:"ikey,omitempty"`
	Data    []byte            `json:"data,omitempty"`
}

func (v *Value) String() string {
	return fmt.Sprintf(
		"Value(ikey=%s, data=%s)",
		v.Ikey,
		string(v.Data),
	)
}

type Ikey string

func (i1 *Ikey) Match(i2 *Ikey) bool {
	return i1 != nil && i2 != nil && *i1 == *i2
}

func (i *Ikey) String() string {
	return string(*i)
}
