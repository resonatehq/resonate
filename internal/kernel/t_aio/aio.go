package t_aio

type Kind int

const (
	Echo Kind = iota
	Router
	Sender
	Store
)

func (k Kind) String() string {
	switch k {
	case Echo:
		return "echo"
	case Router:
		return "router"
	case Sender:
		return "sender"
	case Store:
		return "store"
	default:
		panic("invalid aio")
	}
}

type Submission struct {
	Kind Kind
	Tags map[string]string

	Echo   *EchoSubmission
	Router *RouterSubmission
	Sender *SenderSubmission
	Store  *StoreSubmission
}

func (s *Submission) String() string {
	switch s.Kind {
	case Echo:
		return s.Echo.String()
	case Router:
		return s.Router.String()
	case Sender:
		return s.Sender.String()
	case Store:
		return s.Store.String()
	default:
		panic("invalid aio submission")
	}
}

type Completion struct {
	Kind Kind
	Tags map[string]string

	Echo   *EchoCompletion
	Router *RouterCompletion
	Sender *SenderCompletion
	Store  *StoreCompletion
}

func (c *Completion) String() string {
	switch c.Kind {
	case Echo:
		return c.Echo.String()
	case Router:
		return c.Router.String()
	case Sender:
		return c.Sender.String()
	case Store:
		return c.Store.String()
	default:
		panic("invalid aio completion")
	}
}
