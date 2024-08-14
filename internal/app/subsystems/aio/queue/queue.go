package queue

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

type Queue struct{}

type QueueDevice struct {
	client *http.Client
}

func New() aio.Subsystem {
	return &Queue{}
}

func (s *Queue) String() string {
	return "queue"
}

func (s *Queue) Start() error {
	return nil
}

func (s *Queue) Stop() error {
	return nil
}

func (s *Queue) Reset() error {
	return nil
}

func (s *Queue) Close() error {
	return nil
}

func (s *Queue) NewWorker(int) aio.Worker {
	return &QueueDevice{
		client: &http.Client{Timeout: 1 * time.Second},
	}
}

type req struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (d *QueueDevice) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		util.Assert(sqe.Submission.Queue != nil, "queue submission must not be nil")
		util.Assert(sqe.Submission.Queue.Task != nil, "task must not be nil")
		util.Assert(sqe.Submission.Queue.Task.Message != nil, "message must not be nil")

		cqes[i] = &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Completion: &t_aio.Completion{
				Kind:  t_aio.Queue,
				Tags:  sqe.Submission.Tags, // propagate the tags
				Queue: &t_aio.QueueCompletion{Success: false},
			},
			Callback: sqe.Callback,
		}

		var buf bytes.Buffer

		// by default golang escapes html in json, for queue requests we
		// need to disable this
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)

		if err := enc.Encode(&req{
			Id:      sqe.Submission.Queue.Task.Id,
			Counter: sqe.Submission.Queue.Task.Counter,
		}); err != nil {
			slog.Warn("json marshal failed", "err", err)
			continue
		}

		req, err := http.NewRequest("POST", sqe.Submission.Queue.Task.Message.Recv, &buf)
		if err != nil {
			slog.Warn("http request failed", "err", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		res, err := d.client.Do(req)
		if err != nil {
			slog.Warn("http request failed", "err", err)
			continue
		}

		// set success accordingly
		cqes[i].Completion.Queue.Success = res.StatusCode == http.StatusOK
	}

	return cqes
}
