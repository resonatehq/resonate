package queue

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/task"
	"github.com/stretchr/testify/assert"
)

func TestEcho(t *testing.T) {
	// create a backchannel
	ch := make(chan *req, 1)

	// start a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		var req *req
		if err := json.Unmarshal(b, &req); err != nil {
			t.Fatal(err)
		}
		ch <- req

		switch r.URL.Path {
		case "/no":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))

	defer server.Close()

	for _, tc := range []struct {
		id      string
		counter int
		recv    string
		success bool
	}{
		{"foo", 0, fmt.Sprintf("%s/ok", server.URL), true},
		{"foo", 1, fmt.Sprintf("%s/no", server.URL), false},
		{"bar", 0, fmt.Sprintf("%s/ok", server.URL), true},
		{"bar", 1, fmt.Sprintf("%s/no", server.URL), false},
	} {
		t.Run(fmt.Sprintf("%s/%d", tc.id, tc.counter), func(t *testing.T) {
			sqe := &bus.SQE[t_aio.Submission, t_aio.Completion]{
				Submission: &t_aio.Submission{
					Kind: t_aio.Queue,
					Queue: &t_aio.QueueSubmission{
						Task: &task.Task{
							Id:      tc.id,
							Counter: tc.counter,
							Message: &message.Message{Recv: tc.recv},
						},
					},
				},
			}

			worker := New().NewWorker(0)
			cqes := worker.Process([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe})

			assert.Len(t, cqes, 1)
			assert.Equal(t, tc.success, cqes[0].Completion.Queue.Success)
			assert.Equal(t, &req{tc.id, tc.counter}, <-ch)
		})
	}
}
