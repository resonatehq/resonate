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
	"github.com/stretchr/testify/assert"
)

var (
	createPromiseResponse = &promises.CreatePromiseResponse{
		HTTPResponse: &http.Response{
			StatusCode: 201,
			Body:       io.NopCloser(strings.NewReader("")),
		},
	}
)

func TestCreatePromiseCmd(t *testing.T) {
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
			name: "create a minimal promise",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				mock.
					EXPECT().
					CreatePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(createPromiseResponse, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"foo", "--timeout", "1s"},
			wantStdout: "Created promise: foo\n",
		},
		{
			name: "create a promise with a data param",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				mock.
					EXPECT().
					CreatePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(createPromiseResponse, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"bar", "--timeout", "1m", "--data", `{"foo": "bar"}`},
			wantStdout: "Created promise: bar\n",
		},
		{
			name: "create a promise with a data param and headers",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				mock.
					EXPECT().
					CreatePromiseWithResponse(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(createPromiseResponse, nil).
					Times(1)
				return mock
			}(),
			args:       []string{"baz", "--timeout", "1h", "--data", `{"foo": "bar"}`, "--header", "foo=bar"},
			wantStdout: "Created promise: baz\n",
		},
		{
			name: "Missing ID arg",
			mockPromiseClient: func() *promises.MockClientWithResponsesInterface {
				mock := promises.NewMockClientWithResponsesInterface(ctrl)
				return mock
			}(),
			args:       []string{"--timeout", "24h"},
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

			// Create command in test
			cmd := CreatePromiseCmd(clientSet)

			// Set streams for command
			cmd.SetOut(stdout)
			cmd.SetErr(stderr)

			// Set args for command
			cmd.SetArgs(tc.args)

			// Execute command
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Received unexpected error: %v", err)
			}

			assert.Equal(t, tc.wantStdout, stdout.String())
			assert.Equal(t, tc.wantStderr, stderr.String())
		})
	}
}
