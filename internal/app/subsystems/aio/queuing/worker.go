package queuing

import (
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/connections/t_conn"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

// QueuingWorker is a worker that dispatches submissions to the appropriate connections.
type QueuingWorker struct {
	// ConnectionRouter is the router to route requests to the appropriate connections.
	ConnectionRouter Router

	// ConnectionsSQ is a map of connection names to their submission queues.
	ConnectionsSQ map[string]chan *t_conn.ConnectionSubmission

	// i is the index of the worker.
	i int
}

// Process dispatches the given submissions to the appropriate connections.
func (w *QueuingWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	var (
		cqes []*bus.CQE[t_aio.Submission, t_aio.Completion]
	)

	// Collect all submissions and dispatch them to the appropriate connections.
	for _, sqe := range sqes {
		util.Assert(sqe.Submission.Queuing != nil, "submission must not be nil")

		matchResult, err := w.ConnectionRouter.Match(sqe.Submission.Queuing.TaskId)
		util.Assert(err == nil, "task id must match a pattern in the router")

		bind, ok := w.ConnectionsSQ[matchResult.Connection]
		if ok {
			bind <- &t_conn.ConnectionSubmission{
				Queue:   matchResult.Queue,
				TaskId:  sqe.Submission.Queuing.TaskId,
				Counter: sqe.Submission.Queuing.Counter,
			}
		}
	}

	// Collect and return all the results as successful.
	for _, sqe := range sqes {
		cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Metadata: sqe.Metadata,
			Callback: sqe.Callback,
		}

		cqe.Completion = &t_aio.Completion{
			Kind: t_aio.Queuing,
			Queuing: &t_aio.QueuingCompletion{
				Result: t_aio.Success,
			},
		}

		cqes = append(cqes, cqe)

	}

	return cqes
}
