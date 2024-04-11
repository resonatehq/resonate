package dst

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/stretchr/testify/assert"
)

func TestModelStep(t *testing.T) {
	tcs := []struct {
		name    string
		m       *Model
		req     *t_api.Request
		res     *t_api.Response
		err     error
		wantErr bool
	}{
		{
			name:    "api submission queue full error",
			m:       &Model{},
			req:     &t_api.Request{},
			res:     &t_api.Response{},
			err:     t_api.NewResonateError(t_api.ErrAPISubmissionQueueFull, "", nil),
			wantErr: false,
		},
		{
			name:    "aio submission queue full error",
			m:       &Model{},
			req:     &t_api.Request{},
			res:     &t_api.Response{},
			err:     t_api.NewResonateError(t_api.ErrAIOSubmissionQueueFull, "", nil),
			wantErr: false,
		},
		{
			name:    "unexpected resonate error",
			m:       &Model{},
			req:     &t_api.Request{},
			res:     &t_api.Response{},
			err:     t_api.NewResonateError(-1, "", nil),
			wantErr: true,
		},
		{
			name:    "unexpected non-resonate error",
			m:       &Model{},
			req:     &t_api.Request{},
			res:     &t_api.Response{},
			err:     errors.New("unexpected non-resonate error"),
			wantErr: true,
		},
		{
			name:    "error with fault injection",
			m:       &Model{scenario: &Scenario{Kind: FaultInjection, FaultInjection: &FaultInjectionScenario{P: 1}}},
			req:     &t_api.Request{},
			res:     &t_api.Response{},
			err:     errors.New("error with fault injection"),
			wantErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.Step(0, 0, tc.req, tc.res, tc.err)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestModelValidateReadSchedule(t *testing.T) {
	// Creating a schedule model with a mock schedule
	mockSchedule := schedule.Schedule{
		Id:          "mockScheduleId",
		NextRunTime: 1000, // set according to your test case
		LastRunTime: nil,  // set according to your test case
	}
	sm := &ScheduleModel{
		id:       "mockScheduleId",
		schedule: &mockSchedule,
	}

	// Creating the model with the schedules
	model := &Model{
		scenario:  &Scenario{},                // Assuming you have initialized the scenario struct
		promises:  map[string]*PromiseModel{}, // Assuming you have initialized the promises map
		schedules: map[string]*ScheduleModel{},
		locks:     map[string]*LockModel{}, // Assuming you have initialized the locks map
		tasks:     map[string]*TaskModel{}, // Assuming you have initialized the tasks map
	}

	model.schedules["mockScheduleId"] = sm

	// Test case 1: Successful validation
	request := &t_api.Request{
		ReadSchedule: &t_api.ReadScheduleRequest{
			Id: "mockScheduleId",
		},
	}
	response := &t_api.Response{
		ReadSchedule: &t_api.ReadScheduleResponse{
			Status: t_api.StatusOK,
			Schedule: &schedule.Schedule{
				Id:          "mockScheduleId",
				NextRunTime: 1500, // set according to your test case
				LastRunTime: nil,  // set according to your test case
			},
		},
	}
	err := model.ValidateReadSchedule(0, 0, request, response)
	assert.NoError(t, err)

	// Test case 2: Schedule not found
	response.ReadSchedule.Status = t_api.StatusScheduleNotFound
	err = model.ValidateReadSchedule(0, 0, request, response)
	assert.NoError(t, err) // Expecting no error for schedule not found

	// Test case 3: Unexpected response status
	response.ReadSchedule.Status = t_api.StatusNoContent
	err = model.ValidateReadSchedule(0, 0, request, response)
	assert.Error(t, err) // Expecting error for unexpected response status
}
