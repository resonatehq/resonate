package api

type Subsystem interface {
	String() string
	Start(chan<- error)
	Stop() error
}
