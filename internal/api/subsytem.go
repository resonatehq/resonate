package api

type Subsystem interface {
	String() string
	Start(errors chan<- error)
	Stop() error
}
