package queue

import (
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

// Config

type ConfigDST struct {
	P float64 `flag:"p" desc:"probability of simulated unsuccessful request" default:"0.5" dst:"0:1"`
}

// Subsystem

type QueueDST struct {
	config      *ConfigDST
	r           *rand.Rand
	backchannel chan interface{}
}

func NewDST(r *rand.Rand, backchannel chan interface{}, config *ConfigDST) (*QueueDST, error) {
	return &QueueDST{
		config:      config,
		r:           r,
		backchannel: backchannel,
	}, nil
}

func (q *QueueDST) String() string {
	return "queue:dst"
}

func (q *QueueDST) Kind() t_aio.Kind {
	return t_aio.Queue
}

func (q *QueueDST) Start() error {
	return nil
}

func (q *QueueDST) Stop() error {
	return nil
}

func (q *QueueDST) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		var completion *t_aio.QueueCompletion

		select {
		case q.backchannel <- sqe.Submission.Queue.Task:
			completion = &t_aio.QueueCompletion{
				Success: q.r.Float64() < q.config.P,
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
