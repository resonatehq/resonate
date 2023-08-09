package test

import (
	"testing"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"

	"github.com/resonatehq/resonate/pkg/timeout"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name     string
	panic    bool
	commands []*types.Command
	expected []*types.Result
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

		sqes := []*bus.SQE[types.Submission, types.Completion]{
			{
				Submission: &types.Submission{
					Kind: types.Store,
					Store: &types.StoreSubmission{
						Transaction: &types.Transaction{
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

			assert.Equal(t, c.expected, cqe.Completion.Store.Results)
		}
	})
}

var TestCases = []*testCase{
	{
		name: "CreatePromise",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        1,
						ParamHeaders: []byte("{}"),
						Timeout:      1,
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKey",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
						Ikey:    ikey("bar"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "bar",
						State:        1,
						ParamHeaders: []byte("{}"),
						ParamIkey:    ikey("bar"),
						Timeout:      2,
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKeyAndParam",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "baz",
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{},
						Ikey:    ikey("baz"),
						Data:    []byte("baz"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "baz",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "baz",
						State:        1,
						ParamHeaders: []byte("{}"),
						ParamIkey:    ikey("baz"),
						ParamData:    []byte("baz"),
						Timeout:      3,
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKeyAndParamAndHeaders",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "baz",
					Timeout: 3,
					Param: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Ikey: ikey("baz"),
						Data: []byte("baz"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "baz",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "baz",
						State:        1,
						ParamHeaders: []byte(`{"a":"a","b":"b","c":"c"}`),
						ParamIkey:    ikey("baz"),
						ParamData:    []byte("baz"),
						Timeout:      3,
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseTwice",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id: "foo",
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id: "foo",
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "UpdatePromise",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        2,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte("{}"),
						Timeout:      1,
					}},
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "bar",
						State:        4,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte("{}"),
						Timeout:      2,
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseWithIdKey",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Ikey:    ikey("foo"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Ikey:    ikey("bar"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        2,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte("{}"),
						ValueIkey:    ikey("foo"),
						Timeout:      1,
					}},
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "bar",
						State:        4,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte("{}"),
						ValueIkey:    ikey("bar"),
						Timeout:      2,
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseWithIdKeyAndValue",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
						Ikey:    ikey("foo"),
						Data:    []byte("foo"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
						Ikey:    ikey("bar"),
						Data:    []byte("bar"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        2,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte("{}"),
						ValueIkey:    ikey("foo"),
						ValueData:    []byte("foo"),
						Timeout:      1,
					}},
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "bar",
						State:        4,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte("{}"),
						ValueIkey:    ikey("bar"),
						ValueData:    []byte("bar"),
						Timeout:      2,
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseWithIdKeyAndValueAndHeaders",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Ikey: ikey("foo"),
						Data: []byte("foo"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
					Timeout: 2,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Ikey: ikey("bar"),
						Data: []byte("bar"),
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "bar",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        2,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte(`{"a":"a","b":"b","c":"c"}`),
						ValueIkey:    ikey("foo"),
						ValueData:    []byte("foo"),
						Timeout:      1,
					}},
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "bar",
						State:        4,
						ParamHeaders: []byte("{}"),
						ValueHeaders: []byte(`{"a":"a","b":"b","c":"c"}`),
						ValueIkey:    ikey("bar"),
						ValueData:    []byte("bar"),
						Timeout:      2,
					}},
				},
			},
		},
	},
	{
		name: "UpdatePromiseTwice",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id: "foo",
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id: "bar",
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "UpdatePromiseBeforeCreatePromise",
		commands: []*types.Command{
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "foo",
					State: 2,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "bar",
					State: 4,
					Value: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.AlterPromisesResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "ReadPromise",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
					Timeout: 1,
					Param: promise.Value{
						Headers: map[string]string{},
					},
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 1,
					Records: []*promise.PromiseRecord{{
						Id:           "foo",
						State:        1,
						ParamHeaders: []byte("{}"),
						Timeout:      1,
					}},
				},
			},
		},
	},
	{
		name: "ReadPromiseThatDoesNotExist",
		commands: []*types.Command{
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.ReadPromiseCommand{
					Id: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreReadPromise,
				ReadPromise: &types.QueryPromisesResult{
					RowsReturned: 0,
				},
			},
		},
	},
	{
		name: "CreateTimeout",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "CreateTimeoutTwice",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "foo",
					Time: 1,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "ReadNTimeout",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "bar",
					Time: 1,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "baz",
					Time: 2,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "qux",
					Time: 3,
				},
			},
			{
				Kind: types.StoreReadTimeouts,
				ReadTimeouts: &types.ReadTimeoutsCommand{
					N: 3,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadTimeouts,
				ReadTimeouts: &types.QueryTimeoutsResult{
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
		commands: []*types.Command{
			{
				Kind: types.StoreReadTimeouts,
				ReadTimeouts: &types.ReadTimeoutsCommand{
					N: 3,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreReadTimeouts,
				ReadTimeouts: &types.QueryTimeoutsResult{
					RowsReturned: 0,
				},
			},
		},
	},
	{
		name: "DeleteTimeout",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.CreateTimeoutCommand{
					Id:   "foo",
					Time: 0,
				},
			},
			{
				Kind: types.StoreDeleteTimeout,
				DeleteTimeout: &types.DeleteTimeoutCommand{
					Id: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateTimeout,
				CreateTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreDeleteTimeout,
				DeleteTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "DeleteTimeoutThatDoesNotExist",
		commands: []*types.Command{
			{
				Kind: types.StoreDeleteTimeout,
				DeleteTimeout: &types.DeleteTimeoutCommand{
					Id: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreDeleteTimeout,
				DeleteTimeout: &types.AlterTimeoutsResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "CreateSubscription",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
		},
	},
	{
		name: "CreateSubscriptionTwice",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 0,
					LastInsertId: 0,
				},
			},
		},
	},
	{
		name: "DeleteSubscription",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
				},
			},
			{
				Kind: types.StoreDeleteSubscription,
				DeleteSubscription: &types.DeleteSubscriptionCommand{
					PromiseId: "foo",
					Id:        1,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreDeleteSubscription,
				DeleteSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "ReadSubscriptions",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "bar",
					Url:       "https://bar.com",
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					PromiseId: "baz",
					Url:       "https://baz.com",
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseIds: []string{"foo", "bar", "baz"},
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
					LastInsertId: 2,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionResult{
					RowsAffected: 1,
					LastInsertId: 3,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
					RowsReturned: 3,
					Records: []*subscription.SubscriptionRecord{
						{
							PromiseId: "bar",
							Id:        2,
							Url:       "https://bar.com",
						},
						{
							PromiseId: "baz",
							Id:        3,
							Url:       "https://baz.com",
						},
						{
							PromiseId: "foo",
							Id:        1,
							Url:       "https://foo.com",
						},
					},
				},
			},
		},
	},
	{
		name: "CreateNotification",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
					Time:      0,
					Attempt:   0,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
		},
	},
	{
		name: "CreateNotificationTwice",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
					Time:      0,
					Attempt:   0,
				},
			},
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
					Time:      1,
					Attempt:   1,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "UpdateNotification",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
					Time:      0,
					Attempt:   0,
				},
			},
			{
				Kind: types.StoreUpdateNotification,
				UpdateNotification: &types.UpdateNotificationCommand{
					Id:      1,
					Time:    1,
					Attempt: 1,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreUpdateNotification,
				UpdateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "DeleteNotification",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
					Time:      0,
					Attempt:   0,
				},
			},
			{
				Kind: types.StoreDeleteNotification,
				DeleteNotification: &types.DeleteNotificationCommand{
					Id: 1,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreDeleteNotification,
				DeleteNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "ReadNotification",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "foo",
					Url:       "https://foo.com",
					Time:      0,
					Attempt:   0,
				},
			},
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "bar",
					Url:       "https://bar.com",
					Time:      1,
					Attempt:   1,
				},
			},
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.CreateNotificationCommand{
					PromiseId: "baz",
					Url:       "https://baz.com",
					Time:      2,
					Attempt:   2,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.ReadNotificationsCommand{
					N: 3,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 1,
				},
			},
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 2,
				},
			},
			{
				Kind: types.StoreCreateNotification,
				CreateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
					LastInsertId: 3,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.QueryNotificationsResult{
					RowsReturned: 3,
					Records: []*notification.NotificationRecord{
						{
							PromiseId: "foo",
							Id:        1,
							Url:       "https://foo.com",
							Time:      0,
							Attempt:   0,
						},
						{
							PromiseId: "bar",
							Id:        2,
							Url:       "https://bar.com",
							Time:      1,
							Attempt:   1,
						},
						{
							PromiseId: "baz",
							Id:        3,
							Url:       "https://baz.com",
							Time:      2,
							Attempt:   2,
						},
					},
				},
			},
		},
	},
	{
		name:     "PanicsWhenNoCommands",
		panic:    true,
		commands: []*types.Command{},
	},
	{
		name:     "PanicsWhenInvalidCommand",
		panic:    true,
		commands: []*types.Command{{}},
	},
	{
		name:  "PanicsWhenUpdatePromiseCommandInvalidState",
		panic: true,
		commands: []*types.Command{{
			Kind: types.StoreUpdatePromise,
			UpdatePromise: &types.UpdatePromiseCommand{
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
		commands: []*types.Command{{
			Kind: types.StoreUpdatePromise,
			UpdatePromise: &types.UpdatePromiseCommand{
				Id:    "foo",
				State: 32,
				Value: promise.Value{
					Headers: map[string]string{},
				},
			},
		}},
	},
	{
		name:  "PanicsWhenCreatePromiseHeadersNil",
		panic: true,
		commands: []*types.Command{{
			Kind: types.StoreCreatePromise,
			CreatePromise: &types.CreatePromiseCommand{
				Id: "foo",
				Param: promise.Value{
					Headers: nil,
				},
			},
		}},
	},
	{
		name:  "PanicsWhenUpdatePromiseHeadersNil",
		panic: true,
		commands: []*types.Command{{
			Kind: types.StoreUpdatePromise,
			UpdatePromise: &types.UpdatePromiseCommand{
				Id:    "foo",
				State: promise.Resolved,
				Value: promise.Value{
					Headers: nil,
				},
			},
		}},
	},
	{
		name:  "PanicsWhenCreateTimeoutCommandNegativeTime",
		panic: true,
		commands: []*types.Command{{
			Kind: types.StoreCreateTimeout,
			CreateTimeout: &types.CreateTimeoutCommand{
				Id:   "foo",
				Time: -1,
			},
		}},
	},
}

func ikey(s string) *promise.Ikey {
	ikey := promise.Ikey(s)
	return &ikey
}
