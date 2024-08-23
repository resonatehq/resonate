package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type Subsystem interface {
	String() string
	Kind() t_aio.Kind

	Start() error
	Stop() error

	SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	Flush(int64)

	// for dst only
	Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion]
}
