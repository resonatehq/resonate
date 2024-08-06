package t_aio

type Kind int

const (
	Echo Kind = iota
	Queue
	Store
)

func (k Kind) String() string {
	switch k {
	case Echo:
		return "echo"
	case Queue:
		return "queue"
	case Store:
		return "store"
	default:
		panic("invalid aio")
	}
}

type Submission struct {
	Kind Kind
	Tags map[string]string

	Echo  *EchoSubmission
	Queue *QueueSubmission
	Store *StoreSubmission
}

func (s *Submission) Id() string {
	return s.Tags["request_id"]
}

func (s *Submission) String() string {
	switch s.Kind {
	case Echo:
		return s.Echo.String()
	case Queue:
		return s.Queue.String()
	case Store:
		return s.Store.String()
	default:
		panic("invalid aio submission")
	}
}

type Completion struct {
	Kind Kind
	Tags map[string]string

	Echo  *EchoCompletion
	Queue *QueueCompletion
	Store *StoreCompletion
}

func (c *Completion) Id() string {
	return c.Tags["request_id"]
}

func (c *Completion) String() string {
	switch c.Kind {
	case Echo:
		return c.Echo.String()
	case Queue:
		return c.Queue.String()
	case Store:
		return c.Store.String()
	default:
		panic("invalid aio completion")
	}
}
