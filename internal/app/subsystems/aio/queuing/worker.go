package queuing

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing/bindings/t_bind"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type QueuingWorker struct {
	BindingsSQ map[string]chan *t_bind.BindingSubmission
	i          int
}

// Process dispatches the given submissions to the appropriate connections.
func (w *QueuingWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	var (
		errs []error
		cqes []*bus.CQE[t_aio.Submission, t_aio.Completion]
	)

	// Collect all submissions and dispatch them to the appropriate connections. todo: parallelize this.
	for _, sqe := range sqes {
		util.Assert(sqe.Submission.Queuing != nil, "submission must not be nil")

		var (
			err error
		)

		bind, ok := w.BindingsSQ[sqe.Submission.Queuing.Target]
		if ok {
			bind <- &t_bind.BindingSubmission{
				TaskId:  sqe.Submission.Queuing.TaskId,
				Counter: sqe.Submission.Queuing.Counter,
			}
		} else {
			panic(fmt.Sprintf("binding %s not found", sqe.Submission.Queuing.Target))
			err = fmt.Errorf("binding %s not found", sqe.Submission.Queuing.Target)
		}

		errs = append(errs, err)
	}

	// TODO: comeback cause callback is king here.
	// Collect all results. Still needs to get it back though !
	for i, sqe := range sqes {
		cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Metadata: sqe.Metadata,
			Callback: sqe.Callback,
		}

		if len(errs) > 0 {
			slog.Error("failed queuing execution", "err", errs[i])
			cqe.Error = errs[i]
			continue
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
