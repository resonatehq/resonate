package queue

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

// Config

type Config struct {
	Size      int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	BatchSize int           `flag:"batch-size" desc:"max submissions processed per iteration" default:"100"`
	Workers   int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout   time.Duration `flag:"timeout" desc:"http request timeout" default:"5s"`
}

// Subsystem

type Queue struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*QueueWorker
}

func New(cq chan *bus.CQE[t_aio.Submission, t_aio.Completion], config *Config) (*Queue, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*QueueWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &QueueWorker{
			config: config,
			client: &http.Client{Timeout: config.Timeout},
			sq:     sq,
			cq:     cq,
			flush:  make(chan int64, 1),
		}
	}

	return &Queue{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (q *Queue) Kind() t_aio.Kind {
	return t_aio.Queue
}

func (q *Queue) String() string {
	return "queue"
}

func (q *Queue) Start() error {
	for _, worker := range q.workers {
		go worker.Start()
	}
	return nil
}

func (q *Queue) Stop() error {
	return nil
}

func (q *Queue) Reset() error {
	return nil
}

func (q *Queue) SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion] {
	return q.sq
}

func (q *Queue) Flush(t int64) {
	for _, worker := range q.workers {
		worker.Flush(t)
	}
}

func (q *Queue) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(q.workers) > 0, "must be at least one worker")
	return q.workers[0].Process(sqes)
}

// Worker

type QueueWorker struct {
	config *Config
	client *http.Client
	sq     <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	cq     chan<- *bus.CQE[t_aio.Submission, t_aio.Completion]
	flush  chan int64
}

func (w *QueueWorker) Start() {
	for {
		sqes, ok := util.Collect(w.sq, w.flush, w.config.BatchSize)
		if len(sqes) > 0 {
			for _, cqe := range w.Process(sqes) {
				w.cq <- cqe
			}
		}
		if !ok {
			return
		}
	}
}

func (w *QueueWorker) Flush(t int64) {
	// ignore case where flush channel is full,
	// this means the flush is waiting on the cq
	select {
	case w.flush <- t:
	default:
	}
}

type req struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (w *QueueWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
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
		res, err := w.client.Do(req)
		if err != nil {
			slog.Warn("http request failed", "err", err)
			continue
		}

		// set success accordingly
		cqes[i].Completion.Queue.Success = res.StatusCode == http.StatusOK
	}

	return cqes
}
