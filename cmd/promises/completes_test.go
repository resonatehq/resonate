package promises

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

var (
	patchPromiseResponse = &promises.PatchPromisesIdResponse{
		HTTPResponse: &http.Response{
			StatusCode: 201,
			Body:       io.NopCloser(strings.NewReader("")),
		},
	}
)

func TestCompletePromiseCmd(t *testing.T) {
	// Set Gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Set test cases
	tcs := []struct {
		name              string
		mockPromiseClient promises.ClientWithResponsesInterface
		args              []string
		wantStdout        string
		wantStderr        string
	}{
		{
			name: "resolve a promise",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				mock.
					EXPECT().
					PatchPromisesIdWithResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(patchPromiseResponse, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"resolve", "foo", "--data", `{"foo": "bar"}`, "--header", "foo=bar"},
			wantStdout: "Resolved promise: foo\n",
		},
		{
			name: "reject a promise",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				mock.
					EXPECT().
					PatchPromisesIdWithResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(patchPromiseResponse, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"reject", "bar", "--data", `{"foo": "bar"}`},
			wantStdout: "Rejected promise: bar\n",
		},
		{
			name: "cancel a promise",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				mock.
					EXPECT().
					PatchPromisesIdWithResponse(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(patchPromiseResponse, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"cancel", "baz"},
			wantStdout: "Canceled promise: baz\n",
		},
		{
			name: "Missing ID arg",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				return mock
			}(),
			args:       []string{"resolve"},
			wantStderr: "Must specify promise id\n",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Create buffer writer
			stdout := &bytes.Buffer{}
			stderr := &bytes.Buffer{}

			// Wire up client set
			clientSet := &client.ClientSet{}
			clientSet.SetPromisesV1Alpha1(tc.mockPromiseClient)

			// Create commands in test
			cmds := CompletePromiseCmds(clientSet)

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
				t.Fatalf("Received unexpected error: %v", err)
			}

			assert.Equal(t, tc.wantStdout, stdout.String())
			assert.Equal(t, tc.wantStderr, stderr.String())
		})
	}
}
