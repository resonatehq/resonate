package test

import (
	"testing"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/task"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name     string
	panic    bool
	commands []*t_aio.Command
	expected []*t_aio.Result
}

func (c *testCase) Run(t *testing.T, store store.Store) {
	t.Run(c.name, func(t *testing.T) {
		// assert panic occurs
		if c.panic {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("The function did not panic as expected")
				}
			}()
		}

		results, err := store.Execute([]*t_aio.Transaction{{Commands: c.commands}})
		if err != nil {
			t.Fatal(err)
		}

		assert.Len(t, results, 1)

		// normalize results
		for _, result := range results[0] {
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
					record.PromiseParamHeaders = normalizeJSON(record.PromiseParamHeaders)
					record.Tags = normalizeJSON(record.Tags)
				}
			case t_aio.ReadSchedules:
				for _, record := range result.ReadSchedules.Records {
					record.PromiseParamHeaders = normalizeJSON(record.PromiseParamHeaders)
					record.Tags = normalizeJSON(record.Tags)
				}
			case t_aio.SearchSchedules:
				for _, record := range result.SearchSchedules.Records {
					record.PromiseParamHeaders = normalizeJSON(record.PromiseParamHeaders)
					record.Tags = normalizeJSON(record.Tags)
				}
			}
		}

		assert.Equal(t, c.expected, results[0])
	})
}

func (c *testCase) Panic() bool {
	return c.panic
}

var TestCases = []*testCase{
	// PROMISES
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
					Id: "bar",
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
					Id: "baz",
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
		name: "ReadPromisesTimedout",
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
				Kind: t_aio.ReadPromises,
				ReadPromises: &t_aio.ReadPromisesCommand{
					Time:  2,
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
				Kind: t_aio.ReadPromises,
				ReadPromises: &t_aio.QueryPromisesResult{
					RowsReturned: 2,
					LastSortId:   2,
					Records: []*promise.PromiseRecord{
						{
							Id:           "foo",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      1,
							Tags:         []byte("{}"),
							CreatedOn:    util.ToPointer[int64](1),
							CompletedOn:  nil,
							SortId:       1,
						},
						{
							Id:           "bar",
							State:        1,
							ParamHeaders: []byte("{}"),
							ParamData:    []byte{},
							Timeout:      2,
							Tags:         []byte("{}"),
							CreatedOn:    util.ToPointer[int64](1),
							CompletedOn:  nil,
							SortId:       2,
						},
					},
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

	// CALLBACKS
	{
		name: "CreateCallback",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
		},
	},
	{
		name: "CreateCallbackNoPromise",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "CreateCallbackCompletedPromise",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "foo",
					State: promise.Resolved,
					Value: promise.Value{Headers: map[string]string{}, Data: []byte{}},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "bar",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "bar",
					State: promise.Rejected,
					Value: promise.Value{Headers: map[string]string{}, Data: []byte{}},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "bar",
					Recv:      []byte("bar"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "bar", Leaf: "bar"},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "baz",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "baz",
					State: promise.Canceled,
					Value: promise.Value{Headers: map[string]string{}, Data: []byte{}},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "baz",
					Recv:      []byte("baz"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "baz", Leaf: "baz"},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "qux",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.UpdatePromise,
				UpdatePromise: &t_aio.UpdatePromiseCommand{
					Id:    "qux",
					State: promise.Timedout,
					Value: promise.Value{Headers: map[string]string{}, Data: []byte{}},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "qux",
					Recv:      []byte("qux"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "qux", Leaf: "qux"},
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 0,
				},
			},
		},
	},
	{
		name: "DeleteCallbacks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "bar",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo1"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo2"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "bar",
					Recv:      []byte("bar1"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "bar", Leaf: "bar"},
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.DeleteCallbacksCommand{
					PromiseId: "foo",
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.DeleteCallbacksCommand{
					PromiseId: "bar",
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.DeleteCallbacksCommand{
					PromiseId: "baz",
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "2",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "3",
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.AlterCallbacksResult{
					RowsAffected: 2,
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.DeleteCallbacks,
				DeleteCallbacks: &t_aio.AlterCallbacksResult{
					RowsAffected: 0,
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
					Limit:       2,
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

	// TASKS
	{
		name: "CreateTask",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Invoke, Root: "foo", Leaf: "foo"},
					Timeout:   1,
					State:     task.Init,
					CreatedOn: 1,
				},
			},
			{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.CreateTaskCommand{
					Recv:      []byte("bar"),
					Mesg:      &message.Mesg{Type: message.Invoke, Root: "bar", Leaf: "bar"},
					Timeout:   2,
					State:     task.Init,
					CreatedOn: 2,
				},
			},
			{
				Kind: t_aio.ReadTasks,
				ReadTasks: &t_aio.ReadTasksCommand{
					States: []task.State{task.Init},
					Limit:  10,
				},
			},
		},
		expected: []*t_aio.Result{
			{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
			{
				Kind: t_aio.CreateTask,
				CreateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
					LastInsertId: "2",
				},
			},
			{
				Kind: t_aio.ReadTasks,
				ReadTasks: &t_aio.QueryTasksResult{
					RowsReturned: 2,
					Records: []*task.TaskRecord{
						{
							Id:            "2",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "bar",
							Recv:          []byte("bar"),
							Mesg:          []byte(`{"type":"invoke","root":"bar","leaf":"bar"}`),
							Timeout:       2,
							CreatedOn:     util.ToPointer[int64](2),
						},
						{
							Id:            "1",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "foo",
							Recv:          []byte("foo"),
							Mesg:          []byte(`{"type":"invoke","root":"foo","leaf":"foo"}`),
							Timeout:       1,
							CreatedOn:     util.ToPointer[int64](1),
						},
					},
				},
			},
		},
	},
	{
		name: "CreateTasks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("bar"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "bar", Leaf: "bar"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("baz"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "baz", Leaf: "baz"},
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.CreateTasksCommand{
					PromiseId: "foo",
				},
			},
			{
				Kind: t_aio.ReadTasks,
				ReadTasks: &t_aio.ReadTasksCommand{
					States: []task.State{task.Init},
					Limit:  10,
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "2",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "3",
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.AlterTasksResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.ReadTasks,
				ReadTasks: &t_aio.QueryTasksResult{
					RowsReturned: 3,
					Records: []*task.TaskRecord{
						{
							Id:            "2",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "bar",
							Recv:          []byte("bar"),
							Mesg:          []byte(`{"type":"resume","root":"bar","leaf":"bar"}`),
							CreatedOn:     util.ToPointer[int64](0),
						},
						{
							Id:            "3",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "baz",
							Recv:          []byte("baz"),
							Mesg:          []byte(`{"type":"resume","root":"baz","leaf":"baz"}`),
							CreatedOn:     util.ToPointer[int64](0),
						},
						{
							Id:            "1",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "foo",
							Recv:          []byte("foo"),
							Mesg:          []byte(`{"type":"resume","root":"foo","leaf":"foo"}`),
							CreatedOn:     util.ToPointer[int64](0),
						},
					},
				},
			},
		},
	},
	{
		name: "ReadEnquableTasks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "pbar",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("bar"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "bar"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("baz"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "baz"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "pbar",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "pbar", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "pbar",
					Recv:      []byte("bar"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "pbar", Leaf: "bar"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "pbar",
					Recv:      []byte("baz"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "pbar", Leaf: "baz"},
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.CreateTasksCommand{
					PromiseId: "foo",
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.CreateTasksCommand{
					PromiseId: "pbar",
				},
			},
			{
				Kind: t_aio.ReadEnqueableTasks,
				ReadEnquableTasks: &t_aio.ReadEnqueableTasksCommand{
					States: []task.State{task.Init},
					Limit:  10,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Enqueued,
					Counter:        2,
					Attempt:        1,
					Ttl:            1,
					ExpiresAt:      1,
					CompletedOn:    util.ToPointer[int64](1),
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: 1,
				},
			},
			{
				Kind: t_aio.ReadEnqueableTasks,
				ReadEnquableTasks: &t_aio.ReadEnqueableTasksCommand{
					States: []task.State{task.Init},
					Limit:  10,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "5",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Enqueued,
					Counter:        2,
					Attempt:        1,
					Ttl:            1,
					ExpiresAt:      1,
					CompletedOn:    util.ToPointer[int64](1),
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: 1,
				},
			},
			{
				Kind: t_aio.ReadEnqueableTasks,
				ReadEnquableTasks: &t_aio.ReadEnqueableTasksCommand{
					States: []task.State{task.Init},
					Limit:  10,
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "2",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "3",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "4",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "5",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "6",
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.AlterTasksResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.AlterTasksResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.ReadEnqueableTasks,
				ReadEnquableTasks: &t_aio.QueryTasksResult{
					RowsReturned: 2,
					Records: []*task.TaskRecord{
						{
							Id:            "1",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "foo",
							Recv:          []byte("foo"),
							Mesg:          []byte(`{"type":"resume","root":"foo","leaf":"foo"}`),
							CreatedOn:     util.ToPointer[int64](0),
						},
						{
							Id:            "4",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "pbar",
							Recv:          []byte("foo"),
							Mesg:          []byte(`{"type":"resume","root":"pbar","leaf":"foo"}`),
							CreatedOn:     util.ToPointer[int64](0),
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
				Kind: t_aio.ReadEnqueableTasks,
				ReadEnquableTasks: &t_aio.QueryTasksResult{
					RowsReturned: 1,
					Records: []*task.TaskRecord{
						{
							Id:            "4",
							Counter:       1,
							State:         task.Init,
							RootPromiseId: "pbar",
							Recv:          []byte("foo"),
							Mesg:          []byte(`{"type":"resume","root":"pbar","leaf":"foo"}`),
							CreatedOn:     util.ToPointer[int64](0),
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
				Kind: t_aio.ReadEnqueableTasks,
				ReadEnquableTasks: &t_aio.QueryTasksResult{
					RowsReturned: 0,
				},
			},
		},
	},
	{
		name: "UpdateTask",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.CreateTasksCommand{
					PromiseId: "foo",
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Enqueued,
					Counter:        2,
					Attempt:        1,
					Ttl:            1,
					ExpiresAt:      1,
					CompletedOn:    util.ToPointer[int64](1),
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Claimed,
					Counter:        3,
					Attempt:        2,
					Ttl:            2,
					ExpiresAt:      2,
					CompletedOn:    util.ToPointer[int64](2),
					CurrentStates:  []task.State{task.Enqueued},
					CurrentCounter: 1, // mismatch
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Claimed,
					Counter:        4,
					Attempt:        3,
					Ttl:            3,
					ExpiresAt:      3,
					CompletedOn:    util.ToPointer[int64](3),
					CurrentStates:  []task.State{task.Enqueued},
					CurrentCounter: 2,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Completed,
					Counter:        5,
					Attempt:        4,
					Ttl:            4,
					ExpiresAt:      4,
					CompletedOn:    util.ToPointer[int64](4),
					CurrentStates:  []task.State{task.Enqueued}, // mismatch
					CurrentCounter: 4,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("pid"),
					State:          task.Completed,
					Counter:        6,
					Attempt:        5,
					Ttl:            5,
					ExpiresAt:      5,
					CompletedOn:    util.ToPointer[int64](5),
					CurrentStates:  []task.State{task.Claimed},
					CurrentCounter: 4,
				},
			},
			{
				Kind: t_aio.ReadTask,
				ReadTask: &t_aio.ReadTaskCommand{
					Id: "1",
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 0,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 0,
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
							Id:            "1",
							ProcessId:     util.ToPointer("pid"),
							State:         task.Completed,
							RootPromiseId: "foo",
							Recv:          []byte("foo"),
							Mesg:          []byte(`{"type":"resume","root":"foo","leaf":"foo"}`),
							Counter:       6,
							Attempt:       5,
							Ttl:           5,
							ExpiresAt:     5,
							CreatedOn:     util.ToPointer[int64](0),
							CompletedOn:   util.ToPointer[int64](5),
						},
					},
				},
			},
		},
	},
	{
		name: "HeartbeatTasks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.CreatePromise,
				CreatePromise: &t_aio.CreatePromiseCommand{
					Id:    "foo",
					Param: promise.Value{Headers: map[string]string{}, Data: []byte{}},
					Tags:  map[string]string{},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo1"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo2"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.CreateCallbackCommand{
					PromiseId: "foo",
					Recv:      []byte("foo3"),
					Mesg:      &message.Mesg{Type: message.Resume, Root: "foo", Leaf: "foo"},
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.CreateTasksCommand{
					PromiseId: "foo",
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "1",
					ProcessId:      util.ToPointer("bar"),
					State:          task.Claimed,
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "2",
					ProcessId:      util.ToPointer("bar"),
					State:          task.Claimed,
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.UpdateTaskCommand{
					Id:             "3",
					ProcessId:      util.ToPointer("bar"),
					State:          task.Completed,
					CurrentStates:  []task.State{task.Init},
					CurrentCounter: 1,
				},
			},
			{
				Kind: t_aio.HeartbeatTasks,
				HeartbeatTasks: &t_aio.HeartbeatTasksCommand{
					ProcessId: "bar",
				},
			},
			{
				Kind: t_aio.HeartbeatTasks,
				HeartbeatTasks: &t_aio.HeartbeatTasksCommand{
					ProcessId: "baz",
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
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "1",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "2",
				},
			},
			{
				Kind: t_aio.CreateCallback,
				CreateCallback: &t_aio.AlterCallbacksResult{
					RowsAffected: 1,
					LastInsertId: "3",
				},
			},
			{
				Kind: t_aio.CreateTasks,
				CreateTasks: &t_aio.AlterTasksResult{
					RowsAffected: 3,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.UpdateTask,
				UpdateTask: &t_aio.AlterTasksResult{
					RowsAffected: 1,
				},
			},
			{
				Kind: t_aio.HeartbeatTasks,
				HeartbeatTasks: &t_aio.AlterTasksResult{
					RowsAffected: 2,
				},
			},
			{
				Kind: t_aio.HeartbeatTasks,
				HeartbeatTasks: &t_aio.AlterTasksResult{
					RowsAffected: 0,
				},
			},
		},
	},

	// LOCKS
	{
		name: "AcquireLock",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					ExpiresAt:   1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					ExpiresAt:   1736571600000,
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
		name: "AcquireLockDifferentProcessId",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "barUpdated",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571700000,
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
		name: "AcquireLockFail",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz1",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz2",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
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
		name: "ReleaseLock",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
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
						ResourceId:  "foo",
						ProcessId:   "bar",
						ExecutionId: "baz",
						Ttl:         10_000,
						ExpiresAt:   1736571600000,
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
		name: "ReleaseLockNoop",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
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
		name: "HeartbeatLocks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo-1",
					ProcessId:   "a",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo-2",
					ProcessId:   "a",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
				},
			},
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo-3",
					ProcessId:   "b",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
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
		name: "TimeoutLocks",
		commands: []*t_aio.Command{
			{
				Kind: t_aio.AcquireLock,
				AcquireLock: &t_aio.AcquireLockCommand{
					ResourceId:  "foo",
					ProcessId:   "bar",
					ExecutionId: "baz",
					Ttl:         10_000,
					ExpiresAt:   1736571600000,
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

	// OTHER
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
}
