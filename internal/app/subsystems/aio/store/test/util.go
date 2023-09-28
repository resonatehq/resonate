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

func (c *testCase) Panic() bool {
	return c.panic
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						ParamData:    []byte{},
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						ParamData:    []byte{},
						Timeout:      2,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
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
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
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
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
					}},
				},
			},
		},
	},
	{
		name: "CreatePromiseWithIdKeyAndParamAndHeadersAndTags",
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
					Tags: map[string]string{
						"x": "x",
						"y": "y",
						"z": "z",
					},
					CreatedOn: 1,
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
						Tags:         []byte(`{"x":"x","y":"y","z":"z"}`),
						CreatedOn:    int64ToPointer(1),
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id: "foo",
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueData:    []byte{},
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueData:    []byte{},
						Timeout:      2,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						Data:    []byte{},
					},
					CompletedOn: 2,
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
						Data:    []byte{},
					},
					CompletedOn: 2,
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
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueData:    []byte{},
						ValueIkey:    ikey("foo"),
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueData:    []byte{},
						ValueIkey:    ikey("bar"),
						Timeout:      2,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
					CompletedOn: 2,
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
					CompletedOn: 2,
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
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueIkey:    ikey("foo"),
						ValueData:    []byte("foo"),
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						ParamData:    []byte{},
						ValueHeaders: []byte("{}"),
						ValueIkey:    ikey("bar"),
						ValueData:    []byte("bar"),
						Timeout:      2,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
					CompletedOn: 2,
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
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
					CompletedOn: 2,
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
						ParamData:    []byte{},
						ValueHeaders: []byte(`{"a":"a","b":"b","c":"c"}`),
						ValueIkey:    ikey("foo"),
						ValueData:    []byte("foo"),
						Timeout:      1,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						ParamData:    []byte{},
						ValueHeaders: []byte(`{"a":"a","b":"b","c":"c"}`),
						ValueIkey:    ikey("bar"),
						ValueData:    []byte("bar"),
						Timeout:      2,
						Tags:         []byte("{}"),
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id: "bar",
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Tags:      map[string]string{},
					CreatedOn: 1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
		name: "SearchPromisesById",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo.a",
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo.b",
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "a.bar",
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "b.bar",
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
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "foo.*",
					States: []promise.State{
						promise.Pending,
					},
					Limit: 2,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*.bar",
					States: []promise.State{
						promise.Pending,
					},
					Limit: 2,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Pending,
					},
					Limit: 2,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Pending,
					},
					Limit:  2,
					SortId: int64ToPointer(3),
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
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo.b",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo.a",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "b.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       4,
						},
						{
							Id:           "a.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "b.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       4,
						},
						{
							Id:           "a.bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo.b",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo.a",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
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
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
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
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "baz",
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
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "qux",
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
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "qux",
					State: 8,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 2,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "quy",
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
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
					Id:    "quy",
					State: 16,
					Value: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					CompletedOn: 3,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Pending,
					},
					Limit: 3,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Resolved,
					},
					Limit: 3,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Limit: 3,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Pending,
						promise.Resolved,
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					Limit: 3,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q: "*",
					States: []promise.State{
						promise.Pending,
						promise.Resolved,
						promise.Rejected,
						promise.Timedout,
						promise.Canceled,
					},
					SortId: int64ToPointer(3),
					Limit:  3,
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
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 1,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      3,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
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
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       2,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 3,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "quy",
							State:        16,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(3),
							Tags:         []byte("{}"),
							SortId:       5,
						},
						{
							Id:           "qux",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
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
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 3,
					LastSortId:   3,
					Records: []*promise.PromiseRecord{
						{
							Id:           "quy",
							State:        16,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(3),
							Tags:         []byte("{}"),
							SortId:       5,
						},
						{
							Id:           "qux",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							ValueHeaders: []byte("{}"),
							ValueData:    []byte{},
							Timeout:      3,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
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
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       3,
						},
					},
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
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
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      3,
							CreatedOn:    int64ToPointer(1),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
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
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
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
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   2,
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 0,
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
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreDeleteSubscription,
				DeleteSubscription: &types.DeleteSubscriptionCommand{
					Id:        "a",
					PromiseId: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreDeleteSubscription,
				DeleteSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
		},
	},
	{
		name: "ReadSubscription",
		commands: []*types.Command{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreReadSubscription,
				ReadSubscription: &types.ReadSubscriptionCommand{
					Id:        "a",
					PromiseId: "foo",
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadSubscription,
				ReadSubscription: &types.QuerySubscriptionsResult{
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
		commands: []*types.Command{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "foo",
					Url:         "https://foo.com/b",
					RetryPolicy: &subscription.RetryPolicy{Delay: 2, Attempts: 2},
					CreatedOn:   2,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "bar",
					Url:         "https://bar.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 3, Attempts: 3},
					CreatedOn:   3,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "bar",
					Url:         "https://bar.com/b",
					RetryPolicy: &subscription.RetryPolicy{Delay: 4, Attempts: 4},
					CreatedOn:   4,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseId: "foo",
					Limit:     4,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseId: "bar",
					Limit:     1,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseId: "bar",
					Limit:     1,
					SortId:    int64ToPointer(4),
				},
			},
		},
		expected: []*types.Result{
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
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
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
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
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
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
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "bar",
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "bar",
					Url:         "https://bar.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 2, Attempts: 2},
					CreatedOn:   2,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "baz",
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "baz",
					Url:         "https://baz.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 3, Attempts: 3},
					CreatedOn:   3,
				},
			},
			{
				Kind: types.StoreTimeoutCreateNotifications,
				TimeoutCreateNotifications: &types.TimeoutCreateNotificationsCommand{
					Time: 2,
				},
			},
			{
				Kind: types.StoreTimeoutDeleteSubscriptions,
				TimeoutDeleteSubscriptions: &types.TimeoutDeleteSubscriptionsCommand{
					Time: 2,
				},
			},
			{
				Kind: types.StoreTimeoutPromises,
				TimeoutPromises: &types.TimeoutPromisesCommand{
					Time: 2,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.ReadNotificationsCommand{
					N: 5,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseId: "foo",
					Limit:     3,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseId: "bar",
					Limit:     3,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.ReadSubscriptionsCommand{
					PromiseId: "baz",
					Limit:     3,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.SearchPromisesCommand{
					Q:      "*",
					States: []promise.State{promise.Timedout},
					Limit:  5,
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreTimeoutCreateNotifications,
				TimeoutCreateNotifications: &types.AlterNotificationsResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: types.StoreTimeoutDeleteSubscriptions,
				TimeoutDeleteSubscriptions: &types.AlterSubscriptionsResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: types.StoreTimeoutPromises,
				TimeoutPromises: &types.AlterPromisesResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.QueryNotificationsResult{
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
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
					RowsReturned: 0,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
					RowsReturned: 0,
				},
			},
			{
				Kind: types.StoreReadSubscriptions,
				ReadSubscriptions: &types.QuerySubscriptionsResult{
					RowsReturned: 0,
				},
			},
			{
				Kind: types.StoreSearchPromises,
				SearchPromises: &types.QueryPromisesResult{
					RowsReturned: 3,
					LastSortId:   1,
					Records: []*promise.PromiseRecord{
						{
							Id:           "baz",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       3,
						},
						{
							Id:           "bar",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       2,
						},
						{
							Id:           "foo",
							State:        8,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
							Tags:         []byte("{}"),
							SortId:       1,
						},
					},
				},
			},
		},
	},
	{
		name: "CreateNotifications",
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "b",
					PromiseId:   "foo",
					Url:         "https://foo.com/b",
					RetryPolicy: &subscription.RetryPolicy{Delay: 2, Attempts: 2},
					CreatedOn:   2,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "c",
					PromiseId:   "foo",
					Url:         "https://foo.com/c",
					RetryPolicy: &subscription.RetryPolicy{Delay: 3, Attempts: 3},
					CreatedOn:   3,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreCreateNotifications,
				CreateNotifications: &types.CreateNotificationsCommand{
					PromiseId: "foo",
					Time:      2,
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
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.AlterPromisesResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
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
				Kind: types.StoreCreateNotifications,
				CreateNotifications: &types.AlterNotificationsResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.QueryNotificationsResult{
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
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreCreateNotifications,
				CreateNotifications: &types.CreateNotificationsCommand{
					PromiseId: "foo",
					Time:      2,
				},
			},
			{
				Kind: types.StoreUpdateNotification,
				UpdateNotification: &types.UpdateNotificationCommand{
					Id:        "a",
					PromiseId: "foo",
					Time:      4,
					Attempt:   1,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.ReadNotificationsCommand{
					N: 1,
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
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
				Kind: types.StoreCreateNotifications,
				CreateNotifications: &types.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreUpdateNotification,
				UpdateNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.QueryNotificationsResult{
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
		commands: []*types.Command{
			{
				Kind: types.StoreCreatePromise,
				CreatePromise: &types.CreatePromiseCommand{
					Id:      "foo",
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.CreateSubscriptionCommand{
					Id:          "a",
					PromiseId:   "foo",
					Url:         "https://foo.com/a",
					RetryPolicy: &subscription.RetryPolicy{Delay: 1, Attempts: 1},
					CreatedOn:   1,
				},
			},
			{
				Kind: types.StoreUpdatePromise,
				UpdatePromise: &types.UpdatePromiseCommand{
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
				Kind: types.StoreCreateNotifications,
				CreateNotifications: &types.CreateNotificationsCommand{
					PromiseId: "foo",
					Time:      2,
				},
			},
			{
				Kind: types.StoreDeleteNotification,
				DeleteNotification: &types.DeleteNotificationCommand{
					Id:        "a",
					PromiseId: "foo",
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.ReadNotificationsCommand{
					N: 1,
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
				Kind: types.StoreCreateSubscription,
				CreateSubscription: &types.AlterSubscriptionsResult{
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
				Kind: types.StoreCreateNotifications,
				CreateNotifications: &types.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreDeleteNotification,
				DeleteNotification: &types.AlterNotificationsResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: types.StoreReadNotifications,
				ReadNotifications: &types.QueryNotificationsResult{
					RowsReturned: 0,
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
		name:  "PanicsWhenCreatePromiseParamHeadersNil",
		panic: true,
		commands: []*types.Command{{
			Kind: types.StoreCreatePromise,
			CreatePromise: &types.CreatePromiseCommand{
				Id: "foo",
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
		commands: []*types.Command{{
			Kind: types.StoreCreatePromise,
			CreatePromise: &types.CreatePromiseCommand{
				Id: "foo",
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
		commands: []*types.Command{{
			Kind: types.StoreCreatePromise,
			CreatePromise: &types.CreatePromiseCommand{
				Id: "foo",
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
		commands: []*types.Command{{
			Kind: types.StoreUpdatePromise,
			UpdatePromise: &types.UpdatePromiseCommand{
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
		commands: []*types.Command{{
			Kind: types.StoreUpdatePromise,
			UpdatePromise: &types.UpdatePromiseCommand{
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

func int64ToPointer(i int64) *int64 {
	return &i
}
