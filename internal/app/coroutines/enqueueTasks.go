package coroutines

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/task"
)

var tasksInflight = inflight{}

func EnqueueTasks(t int64, config *system.Config) *Coroutine {
	metadata := metadata.New(fmt.Sprintf("tick:%d:enqueueTasks", t))
	metadata.Tags.Set("name", "enqueue-tasks")

	return scheduler.NewCoroutine(metadata, func(c *Coroutine) {

		// Read tasks from the store that are pending, but
		// filter out the ones that are already inflight
		// (e.g., tasks that are already claimed by a process))
		// and that have not been globally timed out yet.

		completion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.ReadTasks,
							ReadTasks: &t_aio.ReadTasksCommand{
								IsCompleted: false,
								RunTime:     c.Time(),
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to read tasks", "err", err)
			return
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		records := completion.Store.Results[0].ReadTasks.Records

		// Schedule child coroutine for all the pending tasks returned.

		for _, record := range records {
			task, err := record.Task()
			if err != nil {
				slog.Warn("failed to parse task", "err", err)
				continue
			}

			if !tasksInflight.get(taskId(task)) {
				c.Scheduler.Add(enqueueTask(metadata.TransactionId, task))
			}
		}
	})
}

// enqueueTask is a coroutine that enqueues a single task to the queuing subsystem.
func enqueueTask(tid string, task *task.Task) *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission] {
	metadata := metadata.New(tid)
	metadata.Tags.Set("name", "enqueue-task")

	return scheduler.NewCoroutine(metadata, func(c *scheduler.Coroutine[*t_aio.Completion, *t_aio.Submission]) {

		// Handle inflight cache.

		tasksInflight.add(taskId(task))
		c.OnDone(func() { tasksInflight.remove(taskId(task)) })

		// Update counter for the task before enqueuing.

		dbCompletion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Store,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []*t_aio.Command{
						{
							Kind: t_aio.UpdateTask,
							UpdateTask: &t_aio.UpdateTaskCommand{
								Id:              task.Id,
								Counter:         task.Counter + 1,
								ClaimTimeout:    task.ClaimTimeout + 10_000, // 10 seconds, like the claim timeout in createPromise coroutine it is arbitrary for now.
								CompleteTimeout: task.CompleteTimeout,
								CompletedOn:     task.CompletedOn,
								IsCompleted:     task.IsCompleted,
							},
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to update task", "err", err)
			return
		}

		util.Assert(dbCompletion.Store != nil, "completion must not be nil")
		dbWriteResult := dbCompletion.Store.Results[0].UpdateTask
		util.Assert(dbWriteResult.RowsAffected == 1, "result must return 0 or 1 rows")

		// Enqueue the task.

		queueCompletion, err := c.Yield(&t_aio.Submission{
			Kind: t_aio.Queuing,
			Queuing: &t_aio.QueuingSubmission{
				TaskId:  task.Id,
				Counter: task.Counter + 1,
			},
		})
		if err != nil {
			slog.Error("failed to enqueue task", "err", err)
			return
		}

		util.Assert(queueCompletion.Queuing != nil, "completion must not be nil")
	})
}

func taskId(task *task.Task) string {
	return task.Id
}
