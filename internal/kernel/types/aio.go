package types

type AIOKind int

const (
	Echo AIOKind = iota
	Network
	Store
)

func (k AIOKind) String() string {
	switch k {
	case Echo:
		return "Echo"
	case Network:
		return "Network"
	case Store:
		return "Store"
	default:
		panic("invalid submission")
	}
}

type Submission struct {
	Kind    AIOKind
	Echo    *EchoSubmission
	Network *NetworkSubmission
	Store   *StoreSubmission
}

func (s *Submission) String() string {
	// TODO: expand
	return s.Kind.String()
}

type Completion struct {
	Kind    AIOKind
	Echo    *EchoCompletion
	Network *NetworkCompletion
	Store   *StoreCompletion
}

func (c *Completion) String() string {
	// TODO: expand
	return c.Kind.String()
}
