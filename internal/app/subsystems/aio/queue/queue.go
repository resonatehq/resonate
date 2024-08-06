package queue

import (
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type Queue struct{}

type QueueDevice struct{}

func New() aio.Subsystem {
	return &Queue{}
}

func (s *Queue) String() string {
	return "queue"
}

func (s *Queue) Start() error {
	return nil
}

func (s *Queue) Stop() error {
	return nil
}

func (s *Queue) Reset() error {
	return nil
}

func (s *Queue) Close() error {
	return nil
}

func (s *Queue) NewWorker(int) aio.Worker {
	return &QueueDevice{}
}

func (d *QueueDevice) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	panic("not implemented")
}
