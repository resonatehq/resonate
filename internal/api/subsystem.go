package api

type Subsystem interface {
	String() string
	Kind() string
	Addr() string
	Start(chan<- error)
	Stop() error
}
