package coroutines

import (
	"log/slog"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func TaskFulfill(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.TaskFulfillRequest)
	metrics := c.Get("metrics").(*metrics.Metrics)

	t, err := gocoro.SpawnAndAwait(c, readTask(r.Head, req.Id))
	if err != nil {
		slog.Error("failed to read task", "req", r, "err", err)
		return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
	}

	if t == nil {
		return &t_api.Response{
			Status: t_api.StatusTaskNotFound,
			Head:   r.Head,
			Data:   &t_api.TaskFulfillResponse{},
		}, nil
	}

	if t.State.In(task.Init | task.Enqueued | task.Completed | task.Timedout) {
		return &t_api.Response{
			Status: t_api.StatusTaskNotClaimed,
			Head:   r.Head,
			Data:   &t_api.TaskFulfillResponse{},
		}, nil
	}

	if t.Counter != req.Version {
		return &t_api.Response{
			Status: t_api.StatusTaskInvalidVersion,
			Head:   r.Head,
			Data:   &t_api.TaskFulfillResponse{},
		}, nil
	}

	p, err := gocoro.SpawnAndAwait(c, readPromise(r.Head, req.Action.Id))
	if err != nil {
		return nil, err
	}

	// TODO: TaskV2, assertion promise must exists
	if p == nil {
		return &t_api.Response{
			Status: t_api.StatusPromiseNotFound,
			Head:   r.Head,
			Data:   &t_api.TaskFulfillResponse{},
		}, nil
	}

	completedOn := c.Time()
	if p.State == promise.Pending {
		// if the promise is pending complete the promise which completes the task
		// TODO(avillega): Revisits when tasksV2 is implemented
		cmd := &t_aio.UpdatePromiseCommand{
			Id:          req.Id,
			State:       req.Action.State,
			Value:       req.Action.Value,
			CompletedOn: completedOn,
		}
		ok, err := gocoro.SpawnAndAwait(c, completePromise(r.Head, cmd))
		if err != nil {
			return nil, err
		}

		// NOTE: if the the completion of the task was not applied it means the task
		// got moved from acquired to another state while doing the current operation
		// return 409

		if !ok {
			slog.Debug("Couldn't fulfill task and settle promise", "taskId", t.Id, "version", t.Counter, "promiseId", p.Id)
			// Assume the task somehow got acquire by another process that completed the promise
			return &t_api.Response{
				Status: t_api.StatusTaskNotClaimed,
				Head:   r.Head,
				Data:   &t_api.TaskFulfillResponse{},
			}, nil
		}

		metrics.TasksTotal.WithLabelValues("completed").Inc()
		return &t_api.Response{
			Status: t_api.StatusOK,
			Head:   r.Head,
			Data: &t_api.TaskFulfillResponse{
				Promise: &promise.Promise{
					Id:          p.Id,
					State:       req.Action.State,
					Param:       p.Param,
					Value:       req.Action.Value,
					Timeout:     p.Timeout,
					Tags:        p.Tags,
					CreatedOn:   p.CreatedOn,
					CompletedOn: &completedOn,
				},
			},
		}, nil

	} else {
		// If the promise was already completed only complete the task
		completed, err := gocoro.SpawnAndAwait(c, completeTask(r.Head, t.Id, t.Counter, completedOn))
		if err != nil {
			return nil, err
		}
		if !completed {
			return &t_api.Response{
				Status: t_api.StatusTaskNotClaimed,
				Head:   r.Head,
				Data:   &t_api.TaskFulfillResponse{},
			}, nil
		}

		metrics.TasksTotal.WithLabelValues("completed").Inc()
		return &t_api.Response{
			Status: t_api.StatusOK,
			Head:   r.Head,
			Data: &t_api.TaskFulfillResponse{
				Promise: p,
			},
		}, nil
	}
}

func readTask(head map[string]string, id string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, *task.Task] {
	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, *task.Task]) (*task.Task, error) {

		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: head,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.ReadTaskCommand{
							Id: id,
						},
					},
				},
			},
		})
		if err != nil {
			slog.Error("failed to read task", "id", id, "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := t_aio.AsQueryTasks(completion.Store.Results[0])
		util.Assert(result.RowsReturned == 0 || result.RowsReturned == 1, "result must return 0 or 1 rows")

		if result.RowsReturned == 0 {
			return nil, nil
		}

		t, err := result.Records[0].Task()
		if err != nil {
			slog.Error("failed to parse task", "record", result.Records[0], "err", err)
			return nil, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		return t, nil

	}
}

func completeTask(head map[string]string, id string, version int, completedOn int64) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: head,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.UpdateTaskCommand{
							Id:             id,
							ProcessId:      nil,
							State:          task.Completed,
							Counter:        version,
							Attempt:        0,
							Ttl:            0,
							ExpiresAt:      0,
							CompletedOn:    util.ToPointer(completedOn),
							CurrentStates:  []task.State{task.Claimed},
							CurrentCounter: version,
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to complete task", "id", id, "version", version, "err", err)
			return false, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		result := t_aio.AsAlterTasks(completion.Store.Results[0])
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		return result.RowsAffected == 1, nil
	}
}
