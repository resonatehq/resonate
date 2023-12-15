package test

import (
	"testing"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/pkg/notification"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"

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
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
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
						CreatedOn:    int64ToPointer(1),
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
					Id: "bar",
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte{},
					},
					Timeout:        2,
					IdempotencyKey: idempotencyKeyToPointer("bar"),
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
						IdempotencyKeyForCreate: idempotencyKeyToPointer("bar"),
						ParamData:               []byte{},
						Timeout:                 2,
						Tags:                    []byte("{}"),
						CreatedOn:               int64ToPointer(1),
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
					Id: "baz",
					Param: promise.Value{
						Headers: map[string]string{},
						Data:    []byte("baz"),
					},
					Timeout:        3,
					IdempotencyKey: idempotencyKeyToPointer("baz"),
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
						IdempotencyKeyForCreate: idempotencyKeyToPointer("baz"),
						ParamData:               []byte("baz"),
						Timeout:                 3,
						Tags:                    []byte("{}"),
						CreatedOn:               int64ToPointer(1),
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
					Id: "baz",
					Param: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Data: []byte("baz"),
					},
					Timeout:        3,
					IdempotencyKey: idempotencyKeyToPointer("baz"),
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
						IdempotencyKeyForCreate: idempotencyKeyToPointer("baz"),
						ParamData:               []byte("baz"),
						Timeout:                 3,
						Tags:                    []byte("{}"),
						CreatedOn:               int64ToPointer(1),
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
					Id: "baz",
					Param: promise.Value{
						Headers: map[string]string{
							"a": "a",
							"b": "b",
							"c": "c",
						},
						Data: []byte("baz"),
					},
					Timeout:        3,
					IdempotencyKey: idempotencyKeyToPointer("baz"),
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
						IdempotencyKeyForCreate: idempotencyKeyToPointer("baz"),
						ParamData:               []byte("baz"),
						Timeout:                 3,
						Tags:                    []byte(`{"x":"x","y":"y","z":"z"}`),
						CreatedOn:               int64ToPointer(1),
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
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
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
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
						CreatedOn:    int64ToPointer(1),
						CompletedOn:  int64ToPointer(2),
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
					IdempotencyKey: idempotencyKeyToPointer("foo"),
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
					IdempotencyKey: idempotencyKeyToPointer("bar"),
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
						IdempotencyKeyForComplete: idempotencyKeyToPointer("foo"),
						Timeout:                   1,
						Tags:                      []byte("{}"),
						CreatedOn:                 int64ToPointer(1),
						CompletedOn:               int64ToPointer(2),
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
						IdempotencyKeyForComplete: idempotencyKeyToPointer("bar"),
						Timeout:                   2,
						Tags:                      []byte("{}"),
						CreatedOn:                 int64ToPointer(1),
						CompletedOn:               int64ToPointer(2),
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
					IdempotencyKey: idempotencyKeyToPointer("foo"),
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
					IdempotencyKey: idempotencyKeyToPointer("bar"),
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
						IdempotencyKeyForComplete: idempotencyKeyToPointer("foo"),
						ValueData:                 []byte("foo"),
						Timeout:                   1,
						Tags:                      []byte("{}"),
						CreatedOn:                 int64ToPointer(1),
						CompletedOn:               int64ToPointer(2),
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
						IdempotencyKeyForComplete: idempotencyKeyToPointer("bar"),
						ValueData:                 []byte("bar"),
						Timeout:                   2,
						Tags:                      []byte("{}"),
						CreatedOn:                 int64ToPointer(1),
						CompletedOn:               int64ToPointer(2),
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
					IdempotencyKey: idempotencyKeyToPointer("foo"),
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
					IdempotencyKey: idempotencyKeyToPointer("bar"),
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
						IdempotencyKeyForComplete: idempotencyKeyToPointer("foo"),
						ValueData:                 []byte("foo"),
						Timeout:                   1,
						Tags:                      []byte("{}"),
						CreatedOn:                 int64ToPointer(1),
						CompletedOn:               int64ToPointer(2),
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
						IdempotencyKeyForComplete: idempotencyKeyToPointer("bar"),
						ValueData:                 []byte("bar"),
						Timeout:                   2,
						Tags:                      []byte("{}"),
						CreatedOn:                 int64ToPointer(1),
						CompletedOn:               int64ToPointer(2),
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
					Limit:  2,
					SortId: int64ToPointer(3),
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
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
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
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
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
					State: 8,
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
					State: 16,
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
					SortId: int64ToPointer(3),
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
							CreatedOn:    int64ToPointer(1),
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
							CreatedOn:    int64ToPointer(1),
							CompletedOn:  int64ToPointer(2),
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
				Kind: t_aio.SearchPromises,
				SearchPromises: &t_aio.QueryPromisesResult{
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
					SortId:    int64ToPointer(4),
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
					Limit:  5,
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
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
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
		commands: []*t_aio.Command{{
			Kind: t_aio.CreatePromise,
			CreatePromise: &t_aio.CreatePromiseCommand{
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
		commands: []*t_aio.Command{{
			Kind: t_aio.CreatePromise,
			CreatePromise: &t_aio.CreatePromiseCommand{
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
