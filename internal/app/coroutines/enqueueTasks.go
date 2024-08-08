package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

func EnqueueTasks(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
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
								States: []task.State{task.Init},
								Time:   c.Time(),
								Limit:  config.TaskBatchSize,
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

		commands := []*t_aio.Command{}
		promises := make([]promise.Awaitable[*t_aio.Completion], len(result.Records))

		for i, t := range result.Records {
			t, err := t.Task()
			if err != nil {
				slog.Error("failed to parse task, skipping", "err", err)
				continue
			}

			if c.Time() < t.Timeout {
				promises[i] = gocoro.Yield(c, &t_aio.Submission{
					Kind: t_aio.Queue,
					Tags: tags,
					Queue: &t_aio.QueueSubmission{
						ClaimUrl: fmt.Sprintf("%s/tasks/claim?id=%d&counter=%d&frequency=%d", config.Url, t.Id, t.Counter, 10000),
						Task:     t,
					},
				})
			} else {
				// go straight to jail, do not collect $200
				commands = append(commands, &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             t.Id,
						State:          task.Timedout,
						Counter:        t.Counter,
						Frequency:      0,
						Expiration:     0,
						CompletedOn:    &t.Timeout,
						CurrentStates:  []task.State{task.Init},
						CurrentCounter: t.Counter,
					},
				})
			}
		}

		for i, t := range result.Records {
			if promises[i] == nil {
				continue
			}

			completion, err := gocoro.Await(c, promises[i])
			if err != nil {
				slog.Error("failed to enqueue task", "err", err)
				continue
			}

			if completion.Queue.Success {
				commands = append(commands, &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             t.Id,
						State:          task.Enqueued,
						Counter:        t.Counter,
						Frequency:      0,
						Expiration:     c.Time() + int64(config.TaskEnqueueDelay), // time to be claimed
						CurrentStates:  []task.State{task.Init},
						CurrentCounter: t.Counter,
					},
				})
			} else {
				commands = append(commands, &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             t.Id,
						State:          task.Init,
						Counter:        t.Counter,
						Frequency:      0,
						Expiration:     c.Time() + int64(config.TaskEnqueueDelay), // time until reenqueued
						CurrentStates:  []task.State{task.Init},
						CurrentCounter: t.Counter,
					},
				})
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
