package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/task"
)

func EnqueueTasks(config *system.Config, tags map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(tags != nil, "tags must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		tasksCompletion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadEnqueueableTasks,
							ReadEnquableTasks: &t_aio.ReadEnqueueableTasksCommand{
								Time:  c.Time(),
								Limit: config.TaskBatchSize,
							},
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to read tasks", "err", err)
			return nil, nil
		}

		util.Assert(tasksCompletion.Store != nil, "completion must not be nil")
		util.Assert(len(tasksCompletion.Store.Results) == 1, "completion must have one result")

		tasksResult := tasksCompletion.Store.Results[0].ReadEnqueueableTasks
		util.Assert(tasksResult != nil, "tasksResult must not be nil")

		if len(tasksResult.Records) == 0 {
			return nil, nil
		}

		promiseCmds := make([]*t_aio.Command, len(tasksResult.Records))
		for i, r := range tasksResult.Records {
			promiseCmds[i] = &t_aio.Command{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: r.RootPromiseId,
				},
			}
		}
		util.Assert(len(promiseCmds) > 0, "there must be more that 0 promiseCmds")

		promisesCompletion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: tags,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: promiseCmds,
				},
			},
		})

		if err != nil {
			slog.Error("failed to read promises", "err", err)
			return nil, nil
		}

		util.Assert(promisesCompletion.Store != nil, "completion must not be nil")
		util.Assert(len(promisesCompletion.Store.Results) == len(promiseCmds), "There must be one result per cmd")

		promisesResults := promisesCompletion.Store.Results

		commands := []*t_aio.Command{}
		awaiting := make([]promise.Awaitable[*t_aio.Completion], len(tasksResult.Records))

		expiresAt := c.Time() + config.TaskEnqueueDelay.Milliseconds()
		for i, r := range tasksResult.Records {
			if c.Time() < r.Timeout {
				t, err := r.Task()
				if err != nil {
					slog.Warn("failed to parse task", "err", err)
					continue
				}

				util.Assert(promisesResults[i].ReadPromise != nil, "ReadPromise must not be nil")

				promise, err := promisesResults[i].ReadPromise.Records[0].Promise()
				if err != nil {
					slog.Warn("failed to parse promise", "err", err)
					continue
				}

				awaiting[i] = gocoro.Yield(c, &t_aio.Submission{
					Kind: t_aio.Sender,
					Tags: tags,
					Sender: &t_aio.SenderSubmission{
						Task: &task.Task{
							Id:            t.Id,
							Counter:       t.Counter,
							Timeout:       t.Timeout,
							ProcessId:     t.ProcessId,
							State:         task.Enqueued,
							RootPromiseId: t.RootPromiseId,
							Recv:          t.Recv,
							Mesg:          t.Mesg,
							Attempt:       t.Attempt,
							Ttl:           t.Ttl,
							ExpiresAt:     expiresAt,
							CreatedOn:     t.CreatedOn,
							CompletedOn:   t.CompletedOn,
						},
						Promise:       promise,
						ClaimHref:     fmt.Sprintf("%s/tasks/claim/%s/%d", config.Url, t.Id, t.Counter),
						CompleteHref:  fmt.Sprintf("%s/tasks/complete/%s/%d", config.Url, t.Id, t.Counter),
						HeartbeatHref: fmt.Sprintf("%s/tasks/heartbeat/%s/%d", config.Url, t.Id, t.Counter),
					},
				})
			} else {
				// go straight to jail, do not collect $200
				commands = append(commands, &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             r.Id,
						ProcessId:      nil,
						State:          task.Timedout,
						Counter:        r.Counter,
						Attempt:        r.Attempt,
						Ttl:            0,
						ExpiresAt:      0,
						CompletedOn:    &r.Timeout,
						CurrentStates:  []task.State{task.Init},
						CurrentCounter: r.Counter,
					},
				})
			}
		}

		for i, t := range tasksResult.Records {
			if awaiting[i] == nil {
				continue
			}

			completion, err := gocoro.Await(c, awaiting[i])
			if err != nil {
				slog.Warn("failed to enqueue task", "err", err)
			}

			if err == nil && completion.Sender.Success {

				decodedT, decodeErr := t.Task()
				if decodeErr != nil {
					slog.Warn("error decoding task", "err", decodeErr)
				}

				nextState := task.Enqueued
				if decodedT.Mesg.Type == message.Notify {
					// When a task message is of type notify we do a
					// best effort delivery.
					nextState = task.Completed
				}

				commands = append(commands, &t_aio.Command{
					Kind: t_aio.UpdateTask,
					UpdateTask: &t_aio.UpdateTaskCommand{
						Id:             t.Id,
						ProcessId:      nil,
						State:          nextState,
						Counter:        t.Counter,
						Attempt:        t.Attempt,
						Ttl:            0,
						ExpiresAt:      expiresAt, // time to be claimed
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
						Attempt:        t.Attempt + 1,
						Ttl:            0,
						ExpiresAt:      expiresAt, // time until reenqueued
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
