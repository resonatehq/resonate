package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func HeartbeatTasks(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.TaskHeartbeatRequest)
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Head,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []t_aio.Command{
					&t_aio.HeartbeatTasksCommand{
						ProcessId: req.ProcessId,
						Time:      c.Time(),
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to heartbeat task", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := t_aio.AsAlterTasks(completion.Store.Results[0])
	util.Assert(result != nil, "result must not be nil")

	return &t_api.Response{
		Status:   t_api.StatusOK,
		Metadata: r.Head,
		Payload: &t_api.TaskHeartbeatResponse{
			TasksAffected: result.RowsAffected,
		},
	}, nil
}
