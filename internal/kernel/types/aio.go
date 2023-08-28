package types

import "fmt"

type AIOKind int

const (
	Echo AIOKind = iota
	Network
	Store
)

type Submission struct {
	Kind    AIOKind
	Echo    *EchoSubmission
	Network *NetworkSubmission
	Store   *StoreSubmission
}

func (s *Submission) String() string {
	switch s.Kind {
	case Echo:
		return fmt.Sprintf("Echo(data=%s)", s.Echo.Data)
	case Network:
		return fmt.Sprintf("Network(url=%s)", s.Network.Http.Url)
	case Store:
		return fmt.Sprintf("Store(commands=%d)", len(s.Store.Transaction.Commands))
	default:
		panic("invalid submission")
	}
}

type Completion struct {
	Kind    AIOKind
	Echo    *EchoCompletion
	Network *NetworkCompletion
	Store   *StoreCompletion
}

func (c *Completion) String() string {
	switch c.Kind {
	case Echo:
		return fmt.Sprintf("Echo(data=%s)", c.Echo.Data)
	case Network:
		return fmt.Sprintf("Network(status=%s)", c.Network.Http.Status)
	case Store:
		return fmt.Sprintf("Store(results=%d)", len(c.Store.Results))
	default:
		panic("invalid completion")
	}
}
