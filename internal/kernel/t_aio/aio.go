package t_aio

type Kind int

const (
	Store Kind = iota
	Echo
)

func (k Kind) String() string {
	switch k {
	case Store:
		return "store"
	case Echo:
		return "echo"
	default:
		panic("invalid aio")
	}
}

type Submission struct {
	Kind Kind
	Tags map[string]string

	Store *StoreSubmission
	Echo  *EchoSubmission
}

func (s *Submission) Id() string {
	return s.Tags["request_id"]
}

func (s *Submission) String() string {
	switch s.Kind {
	case Store:
		return s.Store.String()
	case Echo:
		return s.Echo.String()
	default:
		panic("invalid aio submission")
	}
}

type Completion struct {
	Kind Kind
	Tags map[string]string

	Store *StoreCompletion
	Echo  *EchoCompletion
}

func (c *Completion) Id() string {
	return c.Tags["request_id"]
}

func (c *Completion) String() string {
	switch c.Kind {
	case Echo:
		return c.Echo.String()
	case Store:
		return c.Store.String()
	default:
		panic("invalid aio completion")
	}
}
