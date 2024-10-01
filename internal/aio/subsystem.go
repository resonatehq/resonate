package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type subsystem interface {
	String() string
	Kind() t_aio.Kind
	Start(chan<- error) error
	Stop() error
}

type Subsystem interface {
	subsystem

	Enqueue(*bus.SQE[t_aio.Submission, t_aio.Completion]) bool
	Flush(int64)
}

type SubsystemDST interface {
	subsystem

	Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion]
}
