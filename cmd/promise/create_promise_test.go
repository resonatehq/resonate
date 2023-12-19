package promise

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/resonatehq/resonate/pkg/client"
	"github.com/resonatehq/resonate/pkg/client/promises"
)

func TestCreatePromiseCmd(t *testing.T) {
	// Set Gomock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Set test cases
	tcs := []struct {
		name              string
		mockPromiseClient promises.ClientInterface
		args              []string
		wantStdout        string
	}{
		{
			name: "create a minimal promise",
			mockPromiseClient: func() *promises.MockClientInterface {
				mock := promises.NewMockClientInterface(ctrl)
				mock.
					EXPECT().
					CreatePromise(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(""))}, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"my-promise", "--timeout", "2524608000000"},
			wantStdout: "Created promise: my-promise\n",
		},
		{
			name: "create a promise with a data param",
			mockPromiseClient: func() *promises.MockClientInterface {
				mock := promises.NewMockClientInterface(ctrl)
				mock.
					EXPECT().
					CreatePromise(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(""))}, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"my-promise", "--timeout", "2524608000000", "--data", `{"foo": "bar"}`},
			wantStdout: "Created promise: my-promise\n",
		},
		{
			name: "create a promise with a data param and headers",
			mockPromiseClient: func() *promises.MockClientInterface {
				mock := promises.NewMockClientInterface(ctrl)
				mock.
					EXPECT().
					CreatePromise(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&http.Response{StatusCode: 201, Body: io.NopCloser(strings.NewReader(""))}, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"my-promise", "--timeout", "2524608000000", "--data", `{"foo": "bar"}`, "--headers", "Content-Type=application/json"},
			wantStdout: "Created promise: my-promise\n",
		},
		{
			name: "Missing ID arg",
			mockPromiseClient: func() *promises.MockClientInterface {
				mock := promises.NewMockClientInterface(ctrl)
				return mock
			}(),
			args:       []string{"--timeout", "2524608000000"},
			wantStdout: "Error: must specify ID\n",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Create buffer writer
			stdout := &bytes.Buffer{}

			// Wire up client set
			clientSet := &client.ClientSet{}
			clientSet.SetPromisesV1Alpha1(tc.mockPromiseClient)

			// Create command in test
			cmd := NewCmdCreatePromise(clientSet, stdout)

			// Set streams for command
			cmd.SetOut(stdout)

			// Set args for command
			cmd.SetArgs(tc.args)

			// Execute command
			err := cmd.Execute()
			if err != nil {
				t.Fatalf("Received unexpected error: %v", err)
			}

			// Check command output (what the user sees)
			if stdout.String() != tc.wantStdout {
				t.Fatalf("Expected stdout to be %q, got %q", tc.wantStdout, stdout.String())
			}
		})
	}
}
