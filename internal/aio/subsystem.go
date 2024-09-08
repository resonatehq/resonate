package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type subsystem interface {
	String() string
	Kind() t_aio.Kind
	Start() error
	Stop() error
}

type Subsystem interface {
	subsystem

	SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	Flush(int64)
}

type SubsystemDST interface {
	subsystem

	Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion]
}
