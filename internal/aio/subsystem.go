package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type Subsystem interface {
	String() string
	NewWorker(int) Worker
	Start() error
	Stop() error
	Reset() error
}

type Worker interface {
	Process([]*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion]
}
