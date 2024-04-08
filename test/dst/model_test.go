package dst

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
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
			err := tc.m.Step(0, tc.req, tc.res, tc.err)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
