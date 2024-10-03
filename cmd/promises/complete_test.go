package promises

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/resonatehq/resonate/pkg/client"
	v1 "github.com/resonatehq/resonate/pkg/client/v1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestCompletePromiseCmd(t *testing.T) {
	// Set Gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	res := &v1.CompletePromiseResponse{HTTPResponse: &http.Response{StatusCode: 201}}

	// Set test cases
	tcs := []struct {
		name       string
		args       []string
		expect     func(*v1.MockClientWithResponsesInterface)
		wantStdout string
		wantStderr string
	}{
		{
			name: "ResolvePromise",
			expect: func(mock *v1.MockClientWithResponsesInterface) {
				mock.
					EXPECT().
					CompletePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(res, nil).
					Times(1)
			},
			args:       []string{"resolve", "foo", "--data", `{"foo": "bar"}`, "--header", "foo=bar"},
			wantStdout: "Resolved promise: foo\n",
		},
		{
			name: "reject a promise",
			args: []string{"reject", "bar", "--data", `{"foo": "bar"}`},
			expect: func(mock *v1.MockClientWithResponsesInterface) {
				mock.
					EXPECT().
					CompletePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(res, nil).
					Times(1)
			},
			wantStdout: "Rejected promise: bar\n",
		},
		{
			name: "cancel a promise",
			args: []string{"cancel", "baz"},
			expect: func(mock *v1.MockClientWithResponsesInterface) {
				mock.
					EXPECT().
					CompletePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(res, nil).
					Times(1)
			},
			wantStdout: "Canceled promise: baz\n",
		},
		{
			name:       "MissingFirstArgument",
			args:       []string{"resolve"},
			expect:     func(mock *v1.MockClientWithResponsesInterface) {},
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
			cmds := CompletePromiseCmds(client.MockClient(mock))

			// Find the appropriate command based on the first argument
			var cmd *cobra.Command
			for _, c := range cmds {
				if c.Name() == tc.args[0] {
					cmd = c
					break
				}
			}

			// Set streams for command
			cmd.SetOut(stdout)
			cmd.SetErr(stderr)

			// Set args for command
			cmd.SetArgs(tc.args[1:])

			// Execute command
			if err := cmd.Execute(); err != nil {
				assert.Equal(t, tc.wantStderr, stderr.String())
			} else {
				assert.Equal(t, tc.wantStdout, stdout.String())
			}
		})
	}
}
