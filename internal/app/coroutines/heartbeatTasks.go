package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func HeartbeatTasks(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: r.Tags,
		Store: &t_aio.StoreSubmission{
			Transaction: &t_aio.Transaction{
				Commands: []*t_aio.Command{
					{
						Kind: t_aio.HeartbeatTasks,
						HeartbeatTasks: &t_aio.HeartbeatTasksCommand{
							ProcessId: r.HeartbeatTasks.ProcessId,
							Time:      c.Time(),
						},
					},
				},
			},
		},
	})
	if err != nil {
		slog.Error("failed to heartbeat task", "req", r, "err", err)
		return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to heartbeat task", err)
	}

	util.Assert(completion.Store != nil, "completion must not be nil")
	result := completion.Store.Results[0].HeartbeatTasks
	util.Assert(result != nil, "result must not be nil")

	return &t_api.Response{
		Kind: t_api.HeartbeatTasks,
		Tags: r.Tags,
		HeartbeatTasks: &t_api.HeartbeatTasksResponse{
			Status:        t_api.StatusOK,
			TasksAffected: result.RowsAffected,
		},
	}, nil
}
