package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func EnqueueTasks(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], m map[string]string) (any, error) {
	util.Assert(m != nil, "metadata must be set")

	config := c.Get("config").(*system.Config)
	metrics := c.Get("metrics").(*metrics.Metrics)

	tasksCompletion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
		Kind: t_aio.Store,
		Tags: m,
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
		Tags: m,
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

	commands := []t_aio.Command{}
	awaiting := make([]gocoroPromise.Awaitable[*t_aio.Completion], len(tasksResult.Records))

	for i, r := range tasksResult.Records {
		var p *promise.Promise
		promiseResult := t_aio.AsQueryPromises(promisesCompletion.Store.Results[i])

		if promiseResult.RowsReturned > 0 {
			p, err = promiseResult.Records[0].Promise()
			if err != nil {
				slog.Warn("failed to parse promise", "err", err)
				continue
			}
		}

		t, err := r.Task()
		if err != nil {
			slog.Warn("failed to parse task", "err", err)
			continue
		}

		if c.Time() >= t.Timeout {
			// go straight to jail, do not collect $200
			commands = append(commands, &t_aio.UpdateTaskCommand{
				Id:             t.Id,
				ProcessId:      nil,
				State:          task.Timedout,
				Counter:        t.Counter,
				Attempt:        t.Attempt,
				Ttl:            0,
				ExpiresAt:      0,
				CompletedOn:    &t.Timeout,
				CurrentStates:  []task.State{task.Init},
				CurrentCounter: t.Counter,
			})
		} else if p != nil && p.State != promise.Pending && t.Mesg.Type != message.Notify {
			completedOn := c.Time()

			// also go straight to jail, do not collect $200
			commands = append(commands, &t_aio.UpdateTaskCommand{
				Id:             t.Id,
				ProcessId:      nil,
				State:          task.Completed,
				Counter:        t.Counter,
				Attempt:        t.Attempt,
				Ttl:            0,
				ExpiresAt:      0,
				CompletedOn:    &completedOn,
				CurrentStates:  []task.State{task.Init},
				CurrentCounter: t.Counter,
			})
		} else {
			awaiting[i] = gocoro.Yield(c, &t_aio.Submission{
				Kind: t_aio.Sender,
				Tags: m,
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
					Promise:       p,
					BaseHref:      config.Url,
					ClaimHref:     fmt.Sprintf("%s/tasks/claim/%s/%d", config.Url, t.Id, t.Counter),
					CompleteHref:  fmt.Sprintf("%s/tasks/complete/%s/%d", config.Url, t.Id, t.Counter),
					HeartbeatHref: fmt.Sprintf("%s/tasks/heartbeat/%s/%d", config.Url, t.Id, t.Counter),
				},
			})
		}
	}

	for i, r := range tasksResult.Records {
		if awaiting[i] == nil {
			continue
		}

		t, err := r.Task()
		if err != nil {
			slog.Warn("failed to parse task", "err", err)
			continue
		}

		completion, err := gocoro.Await(c, awaiting[i])
		if err != nil && t.Mesg.Type != message.Notify {
			slog.Warn("failed to send task", "err", err)
		}

		if t.Mesg.Type == message.Notify {
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
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: m,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: commands,
				},
			},
		})

		if err != nil {
			slog.Error("failed to update tasks", "err", err)
			return nil, nil
		}

		util.Assert(len(completion.Store.Results) == len(commands), "must have same number of results as commands")

		for i, r := range completion.Store.Results {
			result := t_aio.AsAlterTasks(r)
			command := commands[i].(*t_aio.UpdateTaskCommand)
			util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

			if command.State == task.Completed && result.RowsAffected == 1 {
				metrics.TasksTotal.WithLabelValues("completed").Inc()
			}
			if command.State == task.Timedout && result.RowsAffected == 1 {
				metrics.TasksTotal.WithLabelValues("timedout").Inc()
			}
		}
	}

	return nil, nil
}
