package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/pkg/message"
)

type Message struct {
	Type message.Type
	Addr []byte
	Body []byte
	Done func(*t_aio.SenderCompletion)
}

type Plugin interface {
	String() string
	Type() string
	Start(chan<- error) error
	Stop() error
	Enqueue(*Message) bool
}
