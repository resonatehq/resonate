package test

import (
	"testing"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/task"

	"github.com/resonatehq/resonate/pkg/timeout"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name     string
	panic    bool
	commands []*t_aio.Command
	expected []*t_aio.Result
}

func (c *testCase) Run(t *testing.T, subsystem aio.Subsystem) {
	t.Run(c.name, func(t *testing.T) {
		// assert panic occurs
		if c.panic {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("The function did not panic as expected")
				}
			}()
		}

		sqes := []*bus.SQE[t_aio.Submission, t_aio.Completion]{
			{
				Submission: &t_aio.Submission{
					Kind: t_aio.Store,
					Store: &t_aio.StoreSubmission{
						Transaction: &t_aio.Transaction{
							Commands: c.commands,
						},
					},
				},
			},
		}

		for _, cqe := range subsystem.NewWorker(0).Process(sqes) {
			if cqe.Error != nil {
				t.Fatal(cqe.Error)
			}

			// normalize results
			for _, result := range cqe.Completion.Store.Results {
				switch result.Kind {
				case t_aio.ReadPromise:
					for _, record := range result.ReadPromise.Records {
						record.ParamHeaders = normalizeJSON(record.ParamHeaders)
						record.ValueHeaders = normalizeJSON(record.ValueHeaders)
						record.Tags = normalizeJSON(record.Tags)
					}
				case t_aio.SearchPromises:
					for _, record := range result.SearchPromises.Records {
						record.ParamHeaders = normalizeJSON(record.ParamHeaders)
						record.ValueHeaders = normalizeJSON(record.ValueHeaders)
						record.Tags = normalizeJSON(record.Tags)
					}
				case t_aio.ReadSchedule:
					for _, record := range result.ReadSchedule.Records {
						record.Tags = normalizeJSON(record.Tags)
						record.PromiseParamHeaders = normalizeJSON(record.PromiseParamHeaders)
					}
				case t_aio.ReadSchedules:
					for _, record := range result.ReadSchedules.Records {
						record.Tags = normalizeJSON(record.Tags)
						record.PromiseParamHeaders = normalizeJSON(record.PromiseParamHeaders)
					}
				case t_aio.SearchSchedules:
					for _, record := range result.SearchSchedules.Records {
						record.Tags = normalizeJSON(record.Tags)
						record.PromiseParamHeaders = normalizeJSON(record.PromiseParamHeaders)
					}
				}
			}

			assert.Equal(t, c.expected, cqe.Completion.Store.Results)
		}
	})
}

func (c *testCase) Panic() bool {
	return c.panic
}

var TestCases = []*testCase{

	// TASKS
	{
		name: "Basic task lifecyle",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Id:              "taskId",
					Counter:         0,
					PromiseId:       "promiseId",
					ClaimTimeout:    10_000,
					CompleteTimeout: 10_000,
					PromiseTimeout:  30_000,
					CreatedOn:       5_000,
					CompletedOn:     0,
					IsCompleted:     false,
				},
			},
			{
				Kind: t_aio.ReadTasks,
				ReadTasks: &t_aio.ReadTasksCommand{
					IsCompleted: false,
					RunTime:     20_000,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:              "taskId",
					Counter:         0,
					ClaimTimeout:    10_000,
					CompleteTimeout: 10_000,
					CompletedOn:     25_000,
					IsCompleted:     true,
				},
			},
			{
				Kind: t_aio.ReadTask,
				ReadTask: &t_aio.ReadTaskCommand{
					Id: "taskId",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadTasks,
				ReadTasks: &t_aio.QueryTasksResult{
					RowsReturned: 1,
					Records: []*task.TaskRecord{
						{
							Id:              "taskId",
							Counter:         0,
							PromiseId:       "promiseId",
							ClaimTimeout:    10_000,
							CompleteTimeout: 10_000,
							PromiseTimeout:  30_000,
							CreatedOn:       5_000,
							CompletedOn:     0,
							IsCompleted:     false,
						},
					},
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadTask,
				ReadTask: &t_aio.QueryTasksResult{
					RowsReturned: 1,
					Records: []*task.TaskRecord{
						{
							Id:              "taskId",
							Counter:         0,
							PromiseId:       "promiseId",
							ClaimTimeout:    10_000,
							CompleteTimeout: 10_000,
							PromiseTimeout:  30_000,
							CreatedOn:       5_000,
							CompletedOn:     25_000,
							IsCompleted:     true,
						},
					},
				},
			},
		},
	},

	// LOCKS
	{
		name: "AcquireLock: duplicate requests",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Timeout:     1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Timeout:     1736571600000,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "AcquireLock: reacquire lock with same execution id, but different process id",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "barUpdated",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571700000,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "AcquireLock: fail to acquire lock",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz1",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz2",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "HeartbeatLock",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo-1",
					ProcessId:            "a",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo-2",
					ProcessId:            "a",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo-3",
					ProcessId:            "b",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.HeartbeatLocks,
				HeartbeatLocks: &t_aio.HeartbeatLocksCommand{
					ProcessId: "a",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.HeartbeatLocks,
				HeartbeatLocks: &t_aio.AlterLocksResult{
					RowsAffected: 2,
				},
			},
		},
	},
	{
		name: "ReleaseLock: success",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.ReadLock,
				ReadLock: &t_aio.ReadLockCommand{
					ResourceId: "foo",
				},
			},
			{
				Kind: t_aio.ReleaseLock,
				ReleaseLock: &t_aio.ReleaseLockCommand{
					ResourceId:  "foo",
					ExecutionId: "baz",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadLock,
				ReadLock: &t_aio.QueryLocksResult{
					RowsReturned: 1,
					Records: []*lock.LockRecord{{
						ResourceId:           "foo",
						ProcessId:            "bar",
						ExecutionId:          "baz",
						ExpiryInMilliseconds: 10_000,
						Timeout:              1736571600000,
					}},
				},
			},
			{
				Kind: t_aio.ReleaseLock,
				ReleaseLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "ReleaseLock: lock exists, but not owned by execution id or does not exist at all (no-op)",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.ReleaseLock,
				ReleaseLock: &t_aio.ReleaseLockCommand{
					ResourceId:  "foo",
					ExecutionId: "bazOther",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReleaseLock,
				ReleaseLock: &t_aio.AlterLocksResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "TimeoutLocks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:           "foo",
					ProcessId:            "bar",
					ExecutionId:          "baz",
					ExpiryInMilliseconds: 10_000,
					Timeout:              1736571600000,
				},
			},
			{
				Kind: t_aio.TimeoutLocks,
				TimeoutLocks: &t_aio.TimeoutLocksCommand{
					Timeout: 1736571700000,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.TimeoutLocks,
				TimeoutLocks: &t_aio.AlterLocksResult{
					RowsAffected: 1,
				},
			},
		},
	},

	// PROMISES

	{
		name: "CreatePromise",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        1,
						ParamHeaders: []byte("{}"),
						ParamData:    []byte{},
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    util.ToPointer(int64(1)),
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKey",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "bar",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Timeout:        2,
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					Tags:           map[string]string{},
					CreatedOn:      1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                      "bar",
						State:                   1,
						ParamHeaders:            []byte("{}"),
						IdempotencyKeyForCreate: util.ToPointer(idempotency.Key("bar")),
						ParamData:               []byte{},
						Timeout:                 2,
						Tags:                    []byte("{}"),
						CreatedOn:               util.ToPointer(int64(1)),
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKeyAndParam",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "baz",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("baz"),
					},
					Timeout:        3,
					IdempotencyKey: util.ToPointer(idempotency.Key("baz")),
					Tags:           map[string]string{},
					CreatedOn:      1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "baz",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                      "baz",
						State:                   1,
						ParamHeaders:            []byte("{}"),
						IdempotencyKeyForCreate: util.ToPointer(idempotency.Key("baz")),
						ParamData:               []byte("baz"),
						Timeout:                 3,
						Tags:                    []byte("{}"),
						CreatedOn:               util.ToPointer(int64(1)),
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKeyAndParamAndHeaders",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "baz",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Data: []byte("baz"),
					},
					Timeout:        3,
					IdempotencyKey: util.ToPointer(idempotency.Key("baz")),
					Tags:           map[string]string{},
					CreatedOn:      1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "baz",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                      "baz",
						State:                   1,
						ParamHeaders:            []byte(`{"a":"a","b":"b","c":"c"}`),
						IdempotencyKeyForCreate: util.ToPointer(idempotency.Key("baz")),
						ParamData:               []byte("baz"),
						Timeout:                 3,
						Tags:                    []byte("{}"),
						CreatedOn:               util.ToPointer(int64(1)),
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKeyAndParamAndHeadersAndTags",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "baz",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Data: []byte("baz"),
					},
					Timeout:        3,
					IdempotencyKey: util.ToPointer(idempotency.Key("baz")),
					Tags: map[string]string{
						"x": "x",
						"y": "y",
						"z": "z",
					},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "baz",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                      "baz",
						State:                   1,
						ParamHeaders:            []byte(`{"a":"a","b":"b","c":"c"}`),
						IdempotencyKeyForCreate: util.ToPointer(idempotency.Key("baz")),
						ParamData:               []byte("baz"),
						Timeout:                 3,
						Tags:                    []byte(`{"x":"x","y":"y","z":"z"}`),
						CreatedOn:               util.ToPointer(int64(1)),
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseTwice",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "UpdatePromise",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        2,
						ParamHeaders: []byte("{}"),
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueData:    []byte{},
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    util.ToPointer(int64(1)),
						CompletedOn:  util.ToPointer(int64(2)),
					}},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "bar",
						State:        4,
						ParamHeaders: []byte("{}"),
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueData:    []byte{},
						Timeout:      2,
						Tags:         []byte("{}"),
						CreatedOn:    util.ToPointer(int64(1)),
						CompletedOn:  util.ToPointer(int64(2)),
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseWithIdKey",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					IdempotencyKey: util.ToPointer(idempotency.Key("foo")),
					CompletedOn:    2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					CompletedOn:    2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                        "foo",
						State:                     2,
						ParamHeaders:              []byte("{}"),
						ParamData:                 []byte{},
						ValueHeaders:              []byte("{}"),
						ValueData:                 []byte{},
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("foo")),
						Timeout:                   1,
						Tags:                      []byte("{}"),
						CreatedOn:                 util.ToPointer(int64(1)),
						CompletedOn:               util.ToPointer(int64(2)),
					}},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                        "bar",
						State:                     4,
						ParamHeaders:              []byte("{}"),
						ParamData:                 []byte{},
						ValueHeaders:              []byte("{}"),
						ValueData:                 []byte{},
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
						Timeout:                   2,
						Tags:                      []byte("{}"),
						CreatedOn:                 util.ToPointer(int64(1)),
						CompletedOn:               util.ToPointer(int64(2)),
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseWithIdKeyAndValue",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("foo"),
					},
					IdempotencyKey: util.ToPointer(idempotency.Key("foo")),
					CompletedOn:    2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("bar"),
					},
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					CompletedOn:    2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                        "foo",
						State:                     2,
						ParamHeaders:              []byte("{}"),
						ParamData:                 []byte{},
						ValueHeaders:              []byte("{}"),
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("foo")),
						ValueData:                 []byte("foo"),
						Timeout:                   1,
						Tags:                      []byte("{}"),
						CreatedOn:                 util.ToPointer(int64(1)),
						CompletedOn:               util.ToPointer(int64(2)),
					}},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                        "bar",
						State:                     4,
						ParamHeaders:              []byte("{}"),
						ParamData:                 []byte{},
						ValueHeaders:              []byte("{}"),
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
						ValueData:                 []byte("bar"),
						Timeout:                   2,
						Tags:                      []byte("{}"),
						CreatedOn:                 util.ToPointer(int64(1)),
						CompletedOn:               util.ToPointer(int64(2)),
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseWithIdKeyAndValueAndHeaders",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Data: []byte("foo"),
					},
					IdempotencyKey: util.ToPointer(idempotency.Key("foo")),
					CompletedOn:    2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Data: []byte("bar"),
					},
					IdempotencyKey: util.ToPointer(idempotency.Key("bar")),
					CompletedOn:    2,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                        "foo",
						State:                     2,
						ParamHeaders:              []byte("{}"),
						ParamData:                 []byte{},
						ValueHeaders:              []byte(`{"a":"a","b":"b","c":"c"}`),
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("foo")),
						ValueData:                 []byte("foo"),
						Timeout:                   1,
						Tags:                      []byte("{}"),
						CreatedOn:                 util.ToPointer(int64(1)),
						CompletedOn:               util.ToPointer(int64(2)),
					}},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:                        "bar",
						State:                     4,
						ParamHeaders:              []byte("{}"),
						ParamData:                 []byte{},
						ValueHeaders:              []byte(`{"a":"a","b":"b","c":"c"}`),
						IdempotencyKeyForComplete: util.ToPointer(idempotency.Key("bar")),
						ValueData:                 []byte("bar"),
						Timeout:                   2,
						Tags:                      []byte("{}"),
						CreatedOn:                 util.ToPointer(int64(1)),
						CompletedOn:               util.ToPointer(int64(2)),
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseTwice",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "bar",
					State: promise.Pending,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "UpdatePromiseBeforeCreatePromise",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "ReadPromiseThatDoesNotExist",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 0,
				},
			},
		},
	},
	{
		name: "SearchPromisesById",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo.a",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo.b",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "a.bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "b.bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "foo.*",
					States: []promise.State{
						promise.Pending,
					},
					Tags:  map[string]string{},
					Limit: 2,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*.bar",
					States: []promise.State{
						promise.Pending,
					},
					Tags:  map[string]string{},
					Limit: 2,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags:  map[string]string{},
					Limit: 2,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags:   map[string]string{},
					Limit:  2,
					SortId: util.ToPointer(int64(3)),
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo.b",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo.a",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "b.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       4,
						},
						{
							Id:           "a.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "b.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       4,
						},
						{
							Id:           "a.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo.b",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo.a",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
		},
	},
	{
		name: "SearchPromisesByState",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "baz",
					State:   promise.Pending,
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "baz",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "qux",
					State:   promise.Pending,
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "qux",
					State: promise.Timedout,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "quy",
					State:   promise.Pending,
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "quy",
					State: promise.Canceled,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags:  map[string]string{},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Resolved,
					},
					Tags:  map[string]string{},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Tags:  map[string]string{},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
						promise.Resolved,
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Tags:  map[string]string{},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
						promise.Resolved,
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Tags:   map[string]string{},
					SortId: util.ToPointer(int64(3)),
					Limit:  3,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					LastSortId:   2,
					Records: []*promise.PromiseRecord{
						{
							Id:           "bar",
							State:        2,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       2,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 3,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "quy",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(3)),
							Tags:         []byte("{}"),
							SortId:       5,
						},
						{
							Id:           "qux",
							State:        16,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       4,
						},
						{
							Id:           "baz",
							State:        4,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 3,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "quy",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(3)),
							Tags:         []byte("{}"),
							SortId:       5,
						},
						{
							Id:           "qux",
							State:        16,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       4,
						},
						{
							Id:           "baz",
							State:        4,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "bar",
							State:        2,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      3,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
		},
	},
	{
		name: "SearchPromisesByTag",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags: map[string]string{
						"resonate:invocation": "true",
						"foo":                 "foo",
					},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags: map[string]string{
						"bar": "bar",
					},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "baz",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags: map[string]string{
						"baz": "baz",
					},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags:  map[string]string{},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags: map[string]string{
						"resonate:invocation": "true",
					},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id: "*",
					States: []promise.State{
						promise.Pending,
					},
					Tags: map[string]string{
						"bar": "bar",
					},
					Limit: 3,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 3,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "baz",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{\"baz\":\"baz\"}"),
							SortId:       3,
						},
						{
							Id:           "bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{\"bar\":\"bar\"}"),
							SortId:       2,
						},
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{\"foo\":\"foo\",\"resonate:invocation\":\"true\"}"),
							SortId:       1,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{\"foo\":\"foo\",\"resonate:invocation\":\"true\"}"),
							SortId:       1,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					LastSortId:   2,
					Records: []*promise.PromiseRecord{
						{
							Id:           "bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							Tags:         []byte("{\"bar\":\"bar\"}"),
							SortId:       2,
						},
					},
				},
			},
		},
	},

	// SCHEDULES

	{
		name: "CreateUpdateDeleteSchedule",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "foo",
					Description:    "this is a test schedule",
					Cron:           "* * * * *",
					Tags:           map[string]string{},
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
					PromiseParam: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("Created Durable Promise"),
					},
					PromiseTags:    map[string]string{},
					NextRunTime:    1000,
					IdempotencyKey: nil,
					CreatedOn:      500,
				},
			},
			{
				Kind: t_aio.UpdateSchedule,
				UpdateSchedule: &t_aio.UpdateScheduleCommand{
					Id:          "foo",
					LastRunTime: util.ToPointer[int64](1000),
					NextRunTime: 1500,
				},
			},
			{
				Kind: t_aio.ReadSchedule,
				ReadSchedule: &t_aio.ReadScheduleCommand{
					Id: "foo",
				},
			},
			{
				Kind: t_aio.DeleteSchedule,
				DeleteSchedule: &t_aio.DeleteScheduleCommand{
					Id: "foo",
				},
			},
			{
				Kind: t_aio.ReadSchedule,
				ReadSchedule: &t_aio.ReadScheduleCommand{
					Id: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateSchedule,
				UpdateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadSchedule,
				ReadSchedule: &t_aio.QuerySchedulesResult{
					RowsReturned: 1,
					Records: []*schedule.ScheduleRecord{{
						Id:                  "foo",
						Description:         "this is a test schedule",
						Cron:                "* * * * *",
						Tags:                []byte("{}"),
						PromiseId:           "foo.{{.timestamp}}",
						PromiseTimeout:      1000000,
						PromiseParamHeaders: []byte("{}"),
						PromiseParamData:    []byte("Created Durable Promise"),
						PromiseTags:         []byte("{}"),
						LastRunTime:         util.ToPointer[int64](1000),
						NextRunTime:         1500,
						CreatedOn:           500,
						IdempotencyKey:      nil,
					}},
				},
			},
			{
				Kind: t_aio.DeleteSchedule,
				DeleteSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadSchedule,
				ReadSchedule: &t_aio.QuerySchedulesResult{
					RowsReturned: 0,
					Records:      nil,
				},
			},
		},
	},
	{
		name: "ReadSchedules",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "foo-1",
					Description:    "this is a test schedule",
					Cron:           "* * * * *",
					Tags:           map[string]string{},
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
					PromiseParam: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("Created Durable Promise"),
					},
					PromiseTags:    map[string]string{},
					NextRunTime:    1000,
					IdempotencyKey: nil,
					CreatedOn:      500,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "foo-2",
					Description:    "this is a test schedule",
					Cron:           "* * * * *",
					Tags:           map[string]string{},
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
					PromiseParam: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("Created Durable Promise"),
					},
					PromiseTags:    map[string]string{},
					NextRunTime:    2000,
					IdempotencyKey: nil,
					CreatedOn:      500,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "foo-3",
					Description:    "this is a test schedule",
					Cron:           "* * * * *",
					Tags:           map[string]string{},
					PromiseId:      "foo.{{.timestamp}}",
					PromiseTimeout: 1000000,
					PromiseParam: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("Created Durable Promise"),
					},
					PromiseTags:    map[string]string{},
					NextRunTime:    3000,
					IdempotencyKey: nil,
					CreatedOn:      500,
				},
			},
			{
				Kind: t_aio.ReadSchedules,
				ReadSchedules: &t_aio.ReadSchedulesCommand{
					NextRunTime: 2500,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadSchedules,
				ReadSchedules: &t_aio.QuerySchedulesResult{
					RowsReturned: 2,
					Records: []*schedule.ScheduleRecord{
						{
							Id:                  "foo-1",
							Cron:                "* * * * *",
							PromiseId:           "foo.{{.timestamp}}",
							PromiseTimeout:      1000000,
							PromiseParamHeaders: []byte("{}"),
							PromiseParamData:    []byte("Created Durable Promise"),
							PromiseTags:         []byte("{}"),
							LastRunTime:         nil,
							NextRunTime:         1000,
						},
						{
							Id:                  "foo-2",
							Cron:                "* * * * *",
							PromiseId:           "foo.{{.timestamp}}",
							PromiseTimeout:      1000000,
							PromiseParamHeaders: []byte("{}"),
							PromiseParamData:    []byte("Created Durable Promise"),
							PromiseTags:         []byte("{}"),
							LastRunTime:         nil,
							NextRunTime:         2000,
						},
					},
				},
			},
		},
	},
	{
		name: "SearchSchedules",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "foo",
					Description:    "",
					Cron:           "* * * * *",
					Tags:           map[string]string{"foo": "foo"},
					NextRunTime:    1,
					IdempotencyKey: nil,
					CreatedOn:      1,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "bar",
					Description:    "",
					Cron:           "* * * * *",
					Tags:           map[string]string{"bar": "bar"},
					NextRunTime:    2,
					IdempotencyKey: nil,
					CreatedOn:      2,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.CreateScheduleCommand{
					Id:             "baz",
					Description:    "",
					Cron:           "* * * * *",
					Tags:           map[string]string{"baz": "baz"},
					NextRunTime:    3,
					IdempotencyKey: nil,
					CreatedOn:      3,
				},
			},
			{
				Kind: t_aio.SearchSchedules,
				SearchSchedules: &t_aio.SearchSchedulesCommand{
					Id:    "*",
					Tags:  map[string]string{},
					Limit: 3,
				},
			},
			{
				Kind: t_aio.SearchSchedules,
				SearchSchedules: &t_aio.SearchSchedulesCommand{
					Id:    "*",
					Tags:  map[string]string{"foo": "foo"},
					Limit: 3,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSchedule,
				CreateSchedule: &t_aio.AlterSchedulesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.SearchSchedules,
				SearchSchedules: &t_aio.QuerySchedulesResult{
					RowsReturned: 3,
					LastSortId:   1,
					Records: []*schedule.ScheduleRecord{
						{
							Id:          "baz",
							Description: "",
							Cron:        "* * * * *",
							Tags:        []byte("{\"baz\":\"baz\"}"),
							LastRunTime: nil,
							NextRunTime: 3,
							CreatedOn:   3,
							SortId:      3,
						},
						{
							Id:          "bar",
							Description: "",
							Cron:        "* * * * *",
							Tags:        []byte("{\"bar\":\"bar\"}"),
							LastRunTime: nil,
							NextRunTime: 2,
							CreatedOn:   2,
							SortId:      2,
						},
						{
							Id:          "foo",
							Description: "",
							Cron:        "* * * * *",
							Tags:        []byte("{\"foo\":\"foo\"}"),
							LastRunTime: nil,
							NextRunTime: 1,
							CreatedOn:   1,
							SortId:      1,
						},
					},
				},
			},
			{
				Kind: t_aio.SearchSchedules,
				SearchSchedules: &t_aio.QuerySchedulesResult{
					RowsReturned: 1,
					LastSortId:   1,
					Records: []*schedule.ScheduleRecord{
						{
							Id:          "foo",
							Description: "",
							Cron:        "* * * * *",
							Tags:        []byte("{\"foo\":\"foo\"}"),
							LastRunTime: nil,
							NextRunTime: 1,
							CreatedOn:   1,
							SortId:      1,
						},
					},
				},
			},
		},
	},

	// TIMEOUTS

	{
		name: "CreateTimeout",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "CreateTimeoutTwice",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "foo",
					Time: 1,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "ReadNTimeout",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "bar",
					Time: 1,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "baz",
					Time: 2,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "qux",
					Time: 3,
				},
			},
			{
				Kind: t_aio.ReadTimeouts,
				ReadTimeouts: &t_aio.ReadTimeoutsCommand{
					N: 3,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadTimeouts,
				ReadTimeouts: &t_aio.QueryTimeoutsResult{
					RowsReturned: 3,
					Records: []*timeout.TimeoutRecord{
						{Id: "foo", Time: 0},
						{Id: "bar", Time: 1},
						{Id: "baz", Time: 2},
					},
				},
			},
		},
	},
	{
		name: "ReadNTimeoutNoResults",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.ReadTimeouts,
				ReadTimeouts: &t_aio.ReadTimeoutsCommand{
					N: 3,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.ReadTimeouts,
				ReadTimeouts: &t_aio.QueryTimeoutsResult{
					RowsReturned: 0,
				},
			},
		},
	},
	{
		name: "DeleteTimeout",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
			{
				Kind: t_aio.DeleteTimeout,
				DeleteTimeout: &t_aio.DeleteTimeoutCommand{
					Id: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateTimeout,
				CreateTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.DeleteTimeout,
				DeleteTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "DeleteTimeoutThatDoesNotExist",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.DeleteTimeout,
				DeleteTimeout: &t_aio.DeleteTimeoutCommand{
					Id: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.DeleteTimeout,
				DeleteTimeout: &t_aio.AlterTimeoutsResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "CreateSubscription",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "CreateSubscriptionTwice",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   2,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "DeleteSubscription",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.DeleteSubscription,
				DeleteSubscription: &t_aio.DeleteSubscriptionCommand{
					Id:        "a",
					PromiseId: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.DeleteSubscription,
				DeleteSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "DeleteSubscriptions",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "foo",
					Url:         "https://foo.com/b",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "c",
					PromiseId:   "foo",
					Url:         "https://foo.com/c",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.DeleteSubscriptions,
				DeleteSubscriptions: &t_aio.DeleteSubscriptionsCommand{
					PromiseId: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.DeleteSubscriptions,
				DeleteSubscriptions: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 3,
				},
			},
		},
	},
	{
		name: "ReadSubscription",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.ReadSubscription,
				ReadSubscription: &t_aio.ReadSubscriptionCommand{
					Id:        "a",
					PromiseId: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadSubscription,
				ReadSubscription: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 1,
					Records: []*subscription.SubscriptionRecord{
						{
							Id:          "a",
							PromiseId:   "foo",
							Url:         "https://foo.com/a",
							RetryPolicy: []byte("{\"delay\":1,\"attempts\":1}"),
							CreatedOn:   1,
						},
					},
				},
			},
		},
	},
	{
		name: "ReadSubscriptions",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "foo",
					Url:         "https://foo.com/b",
					RetryPolicy: &subscription.RetryPolicy{Delay: 2, Attempts: 2},
					CreatedOn:   2,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "bar",
					Url:         "https://bar.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 3, Attempts: 3},
					CreatedOn:   3,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "bar",
					Url:         "https://bar.com/b",
					RetryPolicy: &subscription.RetryPolicy{Delay: 4, Attempts: 4},
					CreatedOn:   4,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
					PromiseId: "foo",
					Limit:     4,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
					PromiseId: "bar",
					Limit:     1,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
					PromiseId: "bar",
					Limit:     1,
					SortId:    util.ToPointer(int64(4)),
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 2,
					LastSortId:   1,
					Records: []*subscription.SubscriptionRecord{
						{
							Id:          "b",
							PromiseId:   "foo",
							Url:         "https://foo.com/b",
							RetryPolicy: []byte("{\"delay\":2,\"attempts\":2}"),
							CreatedOn:   2,
							SortId:      2,
						},
						{
							Id:          "a",
							PromiseId:   "foo",
							Url:         "https://foo.com/a",
							RetryPolicy: []byte("{\"delay\":1,\"attempts\":1}"),
							CreatedOn:   1,
							SortId:      1,
						},
					},
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 1,
					LastSortId:   4,
					Records: []*subscription.SubscriptionRecord{
						{
							Id:          "b",
							PromiseId:   "bar",
							Url:         "https://bar.com/b",
							RetryPolicy: []byte("{\"delay\":4,\"attempts\":4}"),
							CreatedOn:   4,
							SortId:      4,
						},
					},
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 1,
					LastSortId:   3,
					Records: []*subscription.SubscriptionRecord{
						{
							Id:          "a",
							PromiseId:   "bar",
							Url:         "https://bar.com/a",
							RetryPolicy: []byte("{\"delay\":3,\"attempts\":3}"),
							CreatedOn:   3,
							SortId:      3,
						},
					},
				},
			},
		},
	},
	{
		name: "TimeoutPromises",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags: map[string]string{
						"resonate:timeout": "true",
					},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "bar",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "bar",
					Url:         "https://bar.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 2, Attempts: 2},
					CreatedOn:   2,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "baz",
					State:   promise.Pending,
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "baz",
					Url:         "https://baz.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 3, Attempts: 3},
					CreatedOn:   3,
				},
			},
			{
				Kind: t_aio.TimeoutCreateNotifications,
				TimeoutCreateNotifications: &t_aio.TimeoutCreateNotificationsCommand{
					Time: 2,
				},
			},
			{
				Kind: t_aio.TimeoutDeleteSubscriptions,
				TimeoutDeleteSubscriptions: &t_aio.TimeoutDeleteSubscriptionsCommand{
					Time: 2,
				},
			},
			{
				Kind: t_aio.TimeoutPromises,
				TimeoutPromises: &t_aio.TimeoutPromisesCommand{
					Time: 2,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.ReadNotificationsCommand{
					N: 5,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
					PromiseId: "foo",
					Limit:     3,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
					PromiseId: "bar",
					Limit:     3,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.ReadSubscriptionsCommand{
					PromiseId: "baz",
					Limit:     3,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.SearchPromisesCommand{
					Id:     "*",
					States: []promise.State{promise.Timedout},
					Tags:   map[string]string{},
					Limit:  5,
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.ReadPromiseCommand{
					Id: "foo",
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.TimeoutCreateNotifications,
				TimeoutCreateNotifications: &t_aio.AlterNotificationsResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.TimeoutDeleteSubscriptions,
				TimeoutDeleteSubscriptions: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.TimeoutPromises,
				TimeoutPromises: &t_aio.AlterPromisesResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.QueryNotificationsResult{
					RowsReturned: 3,
					Records: []*notification.NotificationRecord{
						{
							Id:          "a",
							PromiseId:   "bar",
							Url:         "https://bar.com/a",
							RetryPolicy: []byte("{\"delay\":2,\"attempts\":2}"),
							Time:        2,
							Attempt:     0,
						},
						{
							Id:          "a",
							PromiseId:   "baz",
							Url:         "https://baz.com/a",
							RetryPolicy: []byte("{\"delay\":3,\"attempts\":3}"),
							Time:        2,
							Attempt:     0,
						},
						{
							Id:          "a",
							PromiseId:   "foo",
							Url:         "https://foo.com/a",
							RetryPolicy: []byte("{\"delay\":1,\"attempts\":1}"),
							Time:        2,
							Attempt:     0,
						},
					},
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 0,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 0,
				},
			},
			{
				Kind: t_aio.ReadSubscriptions,
				ReadSubscriptions: &t_aio.QuerySubscriptionsResult{
					RowsReturned: 0,
				},
			},
			{
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   2,
					Records: []*promise.PromiseRecord{
						{
							Id:           "baz",
							State:        16,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       3,
						},
						{
							Id:           "bar",
							State:        16,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    util.ToPointer(int64(1)),
							CompletedOn:  util.ToPointer(int64(2)),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						// foo is not here because it was resolved.
					},
				},
			},
			{
				Kind: t_aio.ReadPromise,
				ReadPromise: &t_aio.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        2,
						ParamHeaders: []byte("{}"),
						ParamData:    []byte{},
						Timeout:      2,
						Tags:         []byte("{\"resonate:timeout\":\"true\"}"),
						CreatedOn:    util.ToPointer(int64(1)),
						CompletedOn:  util.ToPointer(int64(2)),
					}},
				},
			},
		},
	},
	{
		name: "CreateNotifications",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "foo",
					Url:         "https://foo.com/b",
					RetryPolicy: &subscription.RetryPolicy{Delay: 2, Attempts: 2},
					CreatedOn:   2,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "c",
					PromiseId:   "foo",
					Url:         "https://foo.com/c",
					RetryPolicy: &subscription.RetryPolicy{Delay: 3, Attempts: 3},
					CreatedOn:   3,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreateNotifications,
				CreateNotifications: &t_aio.CreateNotificationsCommand{
					PromiseId: "foo",
					Time:      2,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.ReadNotificationsCommand{
					N: 3,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateNotifications,
				CreateNotifications: &t_aio.AlterNotificationsResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.QueryNotificationsResult{
					RowsReturned: 3,
					Records: []*notification.NotificationRecord{
						{
							Id:          "a",
							PromiseId:   "foo",
							Url:         "https://foo.com/a",
							RetryPolicy: []byte("{\"delay\":1,\"attempts\":1}"),
							Time:        2,
							Attempt:     0,
						},
						{
							Id:          "b",
							PromiseId:   "foo",
							Url:         "https://foo.com/b",
							RetryPolicy: []byte("{\"delay\":2,\"attempts\":2}"),
							Time:        2,
							Attempt:     0,
						},
						{
							Id:          "c",
							PromiseId:   "foo",
							Url:         "https://foo.com/c",
							RetryPolicy: []byte("{\"delay\":3,\"attempts\":3}"),
							Time:        2,
							Attempt:     0,
						},
					},
				},
			},
		},
	},
	{
		name: "UpdateNotification",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreateNotifications,
				CreateNotifications: &t_aio.CreateNotificationsCommand{
					PromiseId: "foo",
					Time:      2,
				},
			},
			{
				Kind: t_aio.UpdateNotification,
				UpdateNotification: &t_aio.UpdateNotificationCommand{
					Id:        "a",
					PromiseId: "foo",
					Time:      4,
					Attempt:   1,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.ReadNotificationsCommand{
					N: 1,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateNotifications,
				CreateNotifications: &t_aio.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateNotification,
				UpdateNotification: &t_aio.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.QueryNotificationsResult{
					RowsReturned: 1,
					Records: []*notification.NotificationRecord{
						{
							Id:          "a",
							PromiseId:   "foo",
							Url:         "https://foo.com/a",
							RetryPolicy: []byte("{\"delay\":1,\"attempts\":1}"),
							Time:        4,
							Attempt:     1,
						},
					},
				},
			},
		},
	},
	{
		name: "DeleteNotification",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:      "foo",
					State:   promise.Pending,
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: t_aio.CreateNotifications,
				CreateNotifications: &t_aio.CreateNotificationsCommand{
					PromiseId: "foo",
					Time:      2,
				},
			},
			{
				Kind: t_aio.DeleteNotification,
				DeleteNotification: &t_aio.DeleteNotificationCommand{
					Id:        "a",
					PromiseId: "foo",
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.ReadNotificationsCommand{
					N: 1,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateSubscription,
				CreateSubscription: &t_aio.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.CreateNotifications,
				CreateNotifications: &t_aio.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.DeleteNotification,
				DeleteNotification: &t_aio.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.ReadNotifications,
				ReadNotifications: &t_aio.QueryNotificationsResult{
					RowsReturned: 0,
				},
			},
		},
	},
	{
		name:     "PanicsWhenNoCommands",
		panic:    true,
		commands: []*t_aio.Command{},
	},
	{
		name:     "PanicsWhenInvalidCommand",
		panic:    true,
		commands: []*t_aio.Command{{}},
	},
	{
		name:  "PanicsWhenUpdatePromiseCommandInvalidState",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.UpdatePromise,
			UpdatePromise: &t_aio.UpdatePromiseCommand{
				Id:    "foo",
				State: 1,
				Value: promise.Value{
					Headers: map[string]string{},
				},
			},
		}},
	},
	{
		name:  "PanicsWhenUpdatePromiseCommandInvalidState",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.UpdatePromise,
			UpdatePromise: &t_aio.UpdatePromiseCommand{
				Id:    "foo",
				State: 32,
				Value: promise.Value{
					Headers: map[string]string{},
				},
			},
		}},
	},
	{
		name:  "PanicsWhenCreatePromiseParamHeadersNil",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.CreatePromise,
			CreatePromise: &t_aio.CreatePromiseCommand{
				Id:    "foo",
				State: promise.Pending,
				Param: promise.Value{
					Headers: nil,
					Data:    []byte{},
				},
				Tags: map[string]string{},
			},
		}},
	},
	{
		name:  "PanicsWhenCreatePromiseParamDataNil",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.CreatePromise,
			CreatePromise: &t_aio.CreatePromiseCommand{
				Id:    "foo",
				State: promise.Pending,
				Param: promise.Value{
					Headers: map[string]string{},
					Data:    nil,
				},
				Tags: map[string]string{},
			},
		}},
	},
	{
		name:  "PanicsWhenCreatePromiseTagsNil",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.CreatePromise,
			CreatePromise: &t_aio.CreatePromiseCommand{
				Id:    "foo",
				State: promise.Pending,
				Param: promise.Value{
					Headers: map[string]string{},
					Data:    []byte{},
				},
				Tags: nil,
			},
		}},
	},
	{
		name:  "PanicsWhenUpdatePromiseValueHeadersNil",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.UpdatePromise,
			UpdatePromise: &t_aio.UpdatePromiseCommand{
				Id:    "foo",
				State: promise.Resolved,
				Value: promise.Value{
					Headers: nil,
					Data:    []byte{},
				},
			},
		}},
	},
	{
		name:  "PanicsWhenUpdatePromiseValueDataNil",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.UpdatePromise,
			UpdatePromise: &t_aio.UpdatePromiseCommand{
				Id:    "foo",
				State: promise.Resolved,
				Value: promise.Value{
					Headers: map[string]string{},
					Data:    nil,
				},
			},
		}},
	},
	{
		name:  "PanicsWhenCreateTimeoutCommandNegativeTime",
		panic: true,
		commands: []*t_aio.Command{{
			Kind: t_aio.CreateTimeout,
			CreateTimeout: &t_aio.CreateTimeoutCommand{
				Id:   "foo",
				Time: -1,
			},
		}},
	},
}
