package queue

import (
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type QueueDST struct {
	backchannel chan interface{}
}

type QueueDSTDevice struct {
	backchannel chan interface{}
}

func NewDST(backchannel chan interface{}) aio.Subsystem {
	return &QueueDST{
		backchannel: backchannel,
	}
}

func (s *QueueDST) String() string {
	return "queue:dst"
}

func (s *QueueDST) Start() error {
	return nil
}

func (s *QueueDST) Stop() error {
	return nil
}

func (s *QueueDST) Reset() error {
	return nil
}

func (s *QueueDST) Close() error {
	return nil
}

func (s *QueueDST) NewWorker(int) aio.Worker {
	return &QueueDSTDevice{
		backchannel: s.backchannel,
	}
}

func (d *QueueDSTDevice) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		var completion *t_aio.QueueCompletion

		select {
		case d.backchannel <- sqe.Submission.Queue.Task:
			completion = &t_aio.QueueCompletion{
				Success: true,
			}
		default:
			completion = &t_aio.QueueCompletion{
				Success: false,
			}
		}

		cqes[i] = &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Completion: &t_aio.Completion{
				Kind:  t_aio.Queue,
				Tags:  sqe.Submission.Tags, // propagate the tags
				Queue: completion,
			},
			Callback: sqe.Callback,
		}
	}

	return cqes
}
