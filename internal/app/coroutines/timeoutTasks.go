package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func TimeoutTasks(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadTasks,
							ReadTasks: &t_aio.ReadTasksCommand{
								States: []task.State{task.Enqueued, task.Claimed},
								Time:   c.Time(),
								Limit:  5, // TODO
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read tasks", "err", err)
			return nil, t_api.NewResonateError(t_api.ErrAIOStoreFailure, "failed to read tasks", err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := completion.Store.Results[0].ReadTasks
		util.Assert(result != nil, "result must not be nil")

		commands := make([]*t_aio.Command, len(result.Records))

		for i, t := range result.Records {
			util.Assert(t.State.In(task.Enqueued|task.Claimed), "task must be in state enqueued or claimed")

			if c.Time() < t.Timeout {
				commands[i] = &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             t.Id,
						State:          task.Init,
						Counter:        t.Counter + 1,
						Frequency:      0,
						Expiration:     0, // time until reenqueued
						CurrentStates:  []task.State{t.State},
						CurrentCounter: t.Counter,
					},
				}
			} else {
				commands[i] = &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             t.Id,
						State:          task.Timedout,
						Counter:        t.Counter,
						Frequency:      0,
						Expiration:     0,
						CompletedOn:    &t.Timeout,
						CurrentStates:  []task.State{t.State},
						CurrentCounter: t.Counter,
					},
				}
			}
		}

		if len(commands) > 0 {
			_, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: tags,
				Store: &t_aio.StoreSubmission{
					Transaction: &t_aio.Transaction{
						Commands: commands,
					},
				},
			})

			if err != nil {
				slog.Error("failed to update tasks", "err", err)
			}
		}

		return nil, nil
	}
}
