package t_aio

type Kind int

const (
	Echo Kind = iota
	Network
	Store
)

func (k Kind) String() string {
	switch k {
	case Echo:
		return "echo"
	case Network:
		return "network"
	case Store:
		return "store"
	default:
		panic("invalid aio")
	}
}

type Submission struct {
	Kind    Kind
	Echo    *EchoSubmission
	Network *NetworkSubmission
	Store   *StoreSubmission
}

type Completion struct {
	Kind    Kind
	Echo    *EchoCompletion
	Network *NetworkCompletion
	Store   *StoreCompletion
}
