package aio

type Message struct {
	Data []byte
	Body []byte
	Done func(bool, error)
}

type Plugin interface {
	String() string
	Type() string
	Start() error
	Stop() error
	Enqueue(*Message) bool
}
