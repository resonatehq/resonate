package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func EnqueueTasks(config *system.Config, metadata map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
	util.Assert(metadata != nil, "metadata must be set")

	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
		tasksCompletion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: metadata,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.ReadEnqueueableTasksCommand{
							Time:  c.Time(),
							Limit: config.TaskBatchSize,
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

		tasksResult := t_aio.AsQueryTasks(tasksCompletion.Store.Results[0])
		util.Assert(tasksResult != nil, "tasksResult must not be nil")

		if len(tasksResult.Records) == 0 {
			return nil, nil
		}

		promiseCmds := make([]t_aio.Command, len(tasksResult.Records))
		for i, r := range tasksResult.Records {
			promiseCmds[i] = &t_aio.ReadPromiseCommand{Id: r.RootPromiseId}
		}
		util.Assert(len(promiseCmds) > 0, "there must be more that 0 promiseCmds")

		promisesCompletion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: metadata,
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

		commands := []t_aio.Command{}
		awaiting := make([]gocoroPromise.Awaitable[*t_aio.Completion], len(tasksResult.Records))

		for i, r := range tasksResult.Records {
			if c.Time() < r.Timeout {
				t, err := r.Task()
				if err != nil {
					slog.Warn("failed to parse task", "err", err)
					continue
				}

				pResult := t_aio.AsQueryPromises(promisesResults[i])

				var promise *promise.Promise
				if pResult.RowsReturned > 0 {
					promise, err = pResult.Records[0].Promise()
					if err != nil {
						slog.Warn("failed to parse promise", "err", err)
						continue
					}
				}

				awaiting[i] = gocoro.Yield(c, &t_aio.Submission{
					Kind: t_aio.Sender,
					Tags: metadata,
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
							ExpiresAt:     t.ExpiresAt,
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
				commands = append(commands, &t_aio.UpdateTaskCommand{
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
				})
			}
		}

		for i, t := range tasksResult.Records {
			if awaiting[i] == nil {
				continue
			}

			decodedT, decodeErr := t.Task()
			if decodeErr != nil {
				slog.Warn("error decoding task, continuing", "err", decodeErr)
				continue
			}

			completion, err := gocoro.Await(c, awaiting[i])
			if err != nil && decodedT.Mesg.Type != message.Notify {
				slog.Warn("failed to send task", "err", err)
			}
			if err == nil && completion.Sender.Error != nil && decodedT.Mesg.Type != message.Notify {
				slog.Warn("failed to send task", "err", completion.Sender.Error)
			}

			if decodedT.Mesg.Type == message.Notify {
				commands = append(commands, &t_aio.UpdateTaskCommand{
					Id:             t.Id,
					ProcessId:      nil,
					State:          task.Completed,
					Counter:        t.Counter,
					Attempt:        0,
					Ttl:            0,
					ExpiresAt:      0,
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: t.Counter,
				})
			} else if err == nil && completion.Sender.Success {
				var expiresAt int64
				if completion.Sender.TimeToClaim > 0 {
					expiresAt = c.Time() + completion.Sender.TimeToClaim
				} else {
					expiresAt = 0
				}

				commands = append(commands, &t_aio.UpdateTaskCommand{
					Id:             t.Id,
					ProcessId:      nil,
					State:          task.Enqueued,
					Counter:        t.Counter,
					Attempt:        t.Attempt,
					Ttl:            0,
					ExpiresAt:      expiresAt, // time to claim
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: t.Counter,
				})
			} else {
				var expiresAt int64
				if err != nil {
					expiresAt = c.Time() + 15000 // fallback to 15s
				} else if completion.Sender.TimeToRetry > 0 {
					expiresAt = c.Time() + completion.Sender.TimeToRetry
				} else {
					expiresAt = 0
				}

				commands = append(commands, &t_aio.UpdateTaskCommand{
					Id:             t.Id,
					State:          task.Init,
					Counter:        t.Counter,
					Attempt:        t.Attempt + 1,
					Ttl:            0,
					ExpiresAt:      expiresAt, // time to retry enqueueing
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: t.Counter,
				})
			}
		}

		if len(commands) > 0 {
			_, err = gocoro.YieldAndAwait(c, &t_aio.Submission{
				Kind: t_aio.Store,
				Tags: metadata,
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
