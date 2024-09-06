package aio

type Plugin interface {
	Type() string
	Enqueue([]byte, []byte) (bool, error)
}
