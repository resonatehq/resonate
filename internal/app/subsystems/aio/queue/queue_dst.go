package queue

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

// Config

type ConfigDST struct{}

// Subsystem

type QueueDST struct {
	config      *ConfigDST
	backchannel chan interface{}
}

func NewDST(backchannel chan interface{}, config *ConfigDST) (*QueueDST, error) {
	return &QueueDST{
		config:      config,
		backchannel: backchannel,
	}, nil
}

func (q *QueueDST) Kind() t_aio.Kind {
	return t_aio.Queue
}

func (q *QueueDST) String() string {
	return "queue:dst"
}

func (q *QueueDST) Start() error {
	return nil
}

func (q *QueueDST) Stop() error {
	return nil
}

func (q *QueueDST) Reset() error {
	return nil
}

func (q *QueueDST) SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion] {
	return nil
}

func (q *QueueDST) Flush(int64) {}

func (q *QueueDST) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		var completion *t_aio.QueueCompletion

		select {
		case q.backchannel <- sqe.Submission.Queue.Task:
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
