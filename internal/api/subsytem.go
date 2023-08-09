package api

type Subsystem interface {
	Start(errors chan<- error)
	Stop() error
}
