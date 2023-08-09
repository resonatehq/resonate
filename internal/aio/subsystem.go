package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type Subsystem interface {
	String() string
	NewWorker(int) Worker
	Start() error
	Stop() error
	Reset() error
}

type Worker interface {
	Process([]*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion]
}
