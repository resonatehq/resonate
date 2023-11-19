package dst

import (
	"errors"
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/stretchr/testify/require"
)

func TestModelStep(t *testing.T) {
	tests := []struct {
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
			err:     errors.New("unexpected resonate error"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.m.Step(tt.req, tt.res, tt.err)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
