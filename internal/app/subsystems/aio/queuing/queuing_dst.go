package queuing

import (
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type (
	ConfigDST struct {
		P     float32
		Queue chan *ConnectionSubmissionDST
	}

	QueuingSubsystemDST struct {
		config *ConfigDST
		r      *rand.Rand
	}

	QueuingWorkerDST struct {
		config *ConfigDST
		r      *rand.Rand
	}

	ConnectionSubmissionDST struct {
		Queue   string `json:"queue"`
		TaskId  string `json:"taskid"`
		Counter int    `json:"counter"`
	}
)

// NewDST is a simple helper functions that wraps New and returns a pre-configured QueuingSubsystem.
// This configurations aligns with the DST tests. Search for: 'id = fmt.Sprintf("/gpu/summarize/%s", id)'
func NewDST(config *ConfigDST, r *rand.Rand) (aio.Subsystem, error) {
	// Only need to configure coroutine router since mocking process function.
	CoroutineRouter().Handle("/gpu/summarize/*", &RouteHandler{
		Connection: "summarize",
		Queue:      "analytics",
	})

	return &QueuingSubsystemDST{
		config: config,
		r:      r,
	}, nil
}

func (q *QueuingSubsystemDST) String() string {
	return "queuing:dst"
}

func (q *QueuingSubsystemDST) Start() error {
	return nil
}

func (q *QueuingSubsystemDST) Stop() error {
	close(q.config.Queue)
	return nil
}

func (q *QueuingSubsystemDST) Reset() error {
	return nil
}

func (q *QueuingSubsystemDST) NewWorker(int) aio.Worker {
	return &QueuingWorkerDST{
		config: q.config,
		r:      q.r,
	}
}

func (w *QueuingWorkerDST) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		util.Assert(sqe.Submission.Queuing != nil, "submission must not be nil")

		w.config.Queue <- &ConnectionSubmissionDST{
			Queue:   "analytics",
			TaskId:  sqe.Submission.Queuing.TaskId,
			Counter: sqe.Submission.Queuing.Counter,
		}

		cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Callback: sqe.Callback,
		}

		cqe.Completion = &t_aio.Completion{
			Kind: t_aio.Queuing,
			Tags: sqe.Submission.Tags, // propagate the tags
			Queuing: &t_aio.QueuingCompletion{
				Result: t_aio.Success,
			},
		}

		cqes[i] = cqe
	}

	return cqes
}
