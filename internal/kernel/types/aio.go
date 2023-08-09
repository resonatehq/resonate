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
		return "echo"
	case Network:
		return "network"
	case Store:
		return "store"
	default:
		return "io"
	}
}

type Submission struct {
	Kind    AIOKind
	Echo    *EchoSubmission
	Network *NetworkSubmission
	Store   *StoreSubmission
}

type Completion struct {
	Kind    AIOKind
	Echo    *EchoCompletion
	Network *NetworkCompletion
	Store   *StoreCompletion
}
