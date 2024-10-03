package promises

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestCreatePromiseCmd(t *testing.T) {
	// Set Gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	res := &v1.CreatePromiseResponse{HTTPResponse: &http.Response{StatusCode: 201}}

	// Set test cases
	tcs := []struct {
		name       string
		args       []string
		expect     func(*v1.MockClientWithResponsesInterface)
		wantStdout string
		wantStderr string
	}{
		{
			name: "CreatePromise",
			args: []string{"foo", "--timeout", "1s"},
			expect: func(mock *v1.MockClientWithResponsesInterface) {
				mock.
					EXPECT().
					CreatePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(res, nil).
					Times(1)
			},
			wantStdout: "Created promise: foo\n",
		},
		{
			name: "CreatePromiseWithData",
			args: []string{"bar", "--timeout", "1m", "--data", `{"foo": "bar"}`},
			expect: func(mock *v1.MockClientWithResponsesInterface) {
				mock.
					EXPECT().
					CreatePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(res, nil).
					Times(1)
			},
			wantStdout: "Created promise: bar\n",
		},
		{
			name: "CreatePromiseWithDataAndHeaders",
			args: []string{"baz", "--timeout", "1h", "--data", `{"foo": "bar"}`, "--header", "foo=bar"},
			expect: func(mock *v1.MockClientWithResponsesInterface) {
				mock.
					EXPECT().
					CreatePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(res, nil).
					Times(1)
			},
			wantStdout: "Created promise: baz\n",
		},
		{
			name:       "MissingFirstArgument",
			args:       []string{"--timeout", "24h"},
			expect:     func(*v1.MockClientWithResponsesInterface) {},
			wantStderr: "Error: must specify an id\n",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Create buffer writer
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}

			// Create mock client
			mock := v1.NewMockClientWithResponsesInterface(ctrl)
			tc.expect(mock)

			// Create commands in test
			cmd := CreatePromiseCmd(client.MockClient(mock))

			// Set streams for command
			cmd.SetOut(stdout)
			cmd.SetErr(stderr)

			// Set args for command
			cmd.SetArgs(tc.args)

			// Execute command
			if err := cmd.Execute(); err != nil {
				assert.Equal(t, tc.wantStderr, stderr.String())
			} else {
				assert.Equal(t, tc.wantStdout, stdout.String())
			}
		})
	}
}
