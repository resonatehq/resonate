package echo

import (
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
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

func (d *EchoDevice) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		cqes[i] = &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Tags: sqe.Tags,
			Completion: &t_aio.Completion{
				Echo: &t_aio.EchoCompletion{
					Data: sqe.Submission.Echo.Data,
				},
			},
			Callback: sqe.Callback,
		}
	}

	return cqes
}
