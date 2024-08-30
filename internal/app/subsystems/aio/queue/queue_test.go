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
		case "/ok":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))

	defer server.Close()

	okUrl := fmt.Sprintf("%s/ok", server.URL)
	koUrl := fmt.Sprintf("%s/ko", server.URL)

	for _, tc := range []struct {
		id      string
		counter int
		recv    *message.Recv
		success bool
	}{
		{"foo", 0, httpRecv(okUrl), true},
		{"foo", 1, httpRecv(koUrl), false},
		{"bar", 0, httpRecv(okUrl), true},
		{"bar", 1, httpRecv(koUrl), false},
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

			queue, err := New(nil, &Config{Workers: 1})
			assert.Nil(t, err)
			assert.Len(t, queue.workers, 1)

			cqes := queue.workers[0].Process([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe})
			assert.Len(t, cqes, 1)
			assert.Equal(t, tc.success, cqes[0].Completion.Queue.Success)
			assert.Equal(t, &req{tc.id, tc.counter}, <-ch)
		})
	}
}

// Helper functions

func httpRecv(url string) *message.Recv {
	return &message.Recv{
		Type: "http",
		Data: map[string]interface{}{"url": url},
	}
}
