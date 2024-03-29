package t_aio

type Kind int

const (
	Echo Kind = iota
	Network
	Store
	Queuing
)

func (k Kind) String() string {
	switch k {
	case Echo:
		return "echo"
	case Network:
		return "network"
	case Store:
		return "store"
	case Queuing:
		return "queuing"
	default:
		panic("invalid aio")
	}
}

type Submission struct {
	Kind    Kind
	Echo    *EchoSubmission
	Network *NetworkSubmission
	Store   *StoreSubmission
	Queuing *QueuingSubmission
}

func (s *Submission) String() string {
	switch s.Kind {
	case Echo:
		return s.Echo.String()
	case Network:
		return s.Network.String()
	case Store:
		return s.Store.String()
	case Queuing:
		return s.Queuing.String()
	default:
		panic("invalid aio submission")
	}
}

type Completion struct {
	Kind    Kind
	Echo    *EchoCompletion
	Network *NetworkCompletion
	Store   *StoreCompletion
	Queuing *QueuingCompletion
}

func (c *Completion) String() string {
	switch c.Kind {
	case Echo:
		return c.Echo.String()
	case Network:
		return c.Network.String()
	case Store:
		return c.Store.String()
	case Queuing:
		return c.Queuing.String()
	default:
		panic("invalid aio completion")
	}
}
