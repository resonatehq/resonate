package echo

import (
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type Echo struct{}

type EchoDevice struct{}

func New() aio.Subsystem {
	return &Echo{}
}

func (e *Echo) String() string {
	return "echo"
}

func (e *Echo) Start() error {
	return nil
}

func (e *Echo) Stop() error {
	return nil
}

func (e *Echo) Reset() error {
	return nil
}

func (e *Echo) NewWorker(int) aio.Worker {
	return &EchoDevice{}
}

func (d *EchoDevice) Process(sqes []*bus.SQE[types.Submission, types.Completion]) []*bus.CQE[types.Submission, types.Completion] {
	cqes := make([]*bus.CQE[types.Submission, types.Completion], len(sqes))

	for i, sqe := range sqes {
		cqes[i] = &bus.CQE[types.Submission, types.Completion]{
			Kind: sqe.Kind,
			Completion: &types.Completion{
				Echo: &types.EchoCompletion{
					Data: sqe.Submission.Echo.Data,
				},
			},
			Callback: sqe.Callback,
		}
	}

	return cqes
}
