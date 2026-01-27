package coroutines

import (
	"errors"
	"log/slog"

	"github.com/resonatehq/gocoro"
	gocoroPromise "github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

func TaskSuspend(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], r *t_api.Request) (*t_api.Response, error) {
	req := r.Data.(*t_api.TaskSuspendRequest)
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
			Data:   &t_api.TaskSuspendResponse{},
		}, nil
	}

	if t.State.In(task.Init | task.Enqueued | task.Completed | task.Timedout) {
		return &t_api.Response{
			Status: t_api.StatusTaskNotClaimed,
			Head:   r.Head,
			Data:   &t_api.TaskSuspendResponse{},
		}, nil
	}

	if t.Counter != req.Version {
		return &t_api.Response{
			Status: t_api.StatusTaskInvalidVersion,
			Head:   r.Head,
			Data:   &t_api.TaskSuspendResponse{},
		}, nil
	}

	util.Assert(len(req.Actions) > 0, "must have at least one action")
	awaiter, err := gocoro.SpawnAndAwait(c, readPromise(r.Head, req.Actions[0].Awaiter))
	if awaiter == nil {
		return &t_api.Response{
			Status: t_api.StatusPromiseNotFound,
			Head:   r.Head,
			Data:   &t_api.TaskSuspendResponse{},
		}, nil
	}

	if awaiter.State != promise.Pending {
		return &t_api.Response{
			Status: t_api.StatusKeepGoing,
			Head:   r.Head,
			Data:   &t_api.TaskSuspendResponse{},
		}, nil
	}

	recv, exists := awaiter.Tags["resonate:invoke"]
	if !exists {
		slog.Debug("Awaiter promise missing recv tag", "tags", awaiter.Tags)
		return &t_api.Response{
			Status: t_api.StatusPromiseRecvNotFound,
			Head:   r.Head,
			Data:   &t_api.TaskSuspendResponse{},
		}, nil
	}

	awaiting := make([]gocoroPromise.Awaitable[bool], len(req.Actions))
	for i, action := range req.Actions {
		awaiting[i] = gocoro.Spawn(c, promiseRegister(r.Head, awaiter, recv, action.Awaited))
	}

	for _, p := range awaiting {
		created, err := gocoro.Await(c, p)
		var error *t_api.Error
		if err != nil {
			if !errors.As(err, &error) && error.Code() != t_api.StatusPromiseNotFound {
				return nil, err
			}
			return &t_api.Response{
				Status: t_api.StatusPromiseNotFound,
				Head:   r.Head,
				Data:   &t_api.TaskSuspendResponse{},
			}, nil
		}

		if !created {
			// If atleast one of the callbacks was not created the task must keep going
			return &t_api.Response{
				Status: t_api.StatusKeepGoing,
				Head:   r.Head,
				Data:   &t_api.TaskSuspendResponse{},
			}, nil
		}
	}

	// All callbacks created
	ok, err := gocoro.SpawnAndAwait(c, completeTask(r.Head, req.Id, req.Version, c.Time()))
	if err != nil {
		return nil, err
	}

	if !ok {
		return TaskSuspend(c, r) // We could lose a task, go from the top
	}

	metrics.TasksTotal.WithLabelValues("completed").Inc()
	return &t_api.Response{
		Status: t_api.StatusOK,
		Head:   r.Head,
		Data:   &t_api.TaskSuspendResponse{},
	}, nil

}

func promiseRegister(head map[string]string, awaiter *promise.Promise, recv string, awaitedId string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, bool] {
	return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, bool]) (bool, error) {
		awaited, err := gocoro.SpawnAndAwait(c, readPromise(head, awaitedId))
		if err != nil {
			return false, err
		}

		if awaited == nil {
			return false, t_api.NewError(t_api.StatusPromiseNotFound, errors.New("Promise not found"))
		}

		if awaited.State != promise.Pending {
			return false, nil
		}

		createdOn := c.Time()
		completion, err := gocoro.YieldAndAwait(c, &t_aio.Submission{
			Kind: t_aio.Store,
			Tags: head,
			Store: &t_aio.StoreSubmission{
				Transaction: &t_aio.Transaction{
					Commands: []t_aio.Command{
						&t_aio.CreateCallbackCommand{
							Id:        util.ResumeId(awaiter.Id, awaited.Id),
							PromiseId: awaited.Id,
							Recv:      []byte(recv),
							Mesg: &message.Mesg{
								Type: "resume",
								Head: map[string]string{},
								Root: awaiter.Id,
								Leaf: awaited.Id,
							},
							Timeout:   awaiter.Timeout,
							CreatedOn: createdOn,
						},
					},
				},
			},
		})

		if err != nil {
			slog.Error("failed to create callback", "awaiter", awaiter.Id, "awaited", awaited.Id, "err", err)
			return false, t_api.NewError(t_api.StatusAIOStoreError, err)
		}

		if !completion.Store.Valid {
			return false, t_api.NewError(t_api.StatusTaskPreconditionFailed, errors.New("the specified task is not valid"))
		}

		util.Assert(completion.Store != nil, "completion must not be nil")
		util.Assert(len(completion.Store.Results) == 1, "completion must have one result")

		result := t_aio.AsAlterCallbacks(completion.Store.Results[0])
		util.Assert(result.RowsAffected == 0 || result.RowsAffected == 1, "result must return 0 or 1 rows")

		return true, nil
	}

}
