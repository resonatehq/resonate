package aio

import "github.com/resonatehq/resonate/pkg/message"

type Message struct {
	Type message.Type
	Data []byte
	Body []byte
	Done func(bool, error)
}

type Plugin interface {
	String() string
	Type() string
	Start(chan<- error) error
	Stop() error
	Enqueue(*Message) bool
}
