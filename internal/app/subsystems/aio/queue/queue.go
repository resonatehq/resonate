package queue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
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

func New(aio aio.AIO, config *Config) (*Queue, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*QueueWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &QueueWorker{
			config: config,
			client: &http.Client{Timeout: config.Timeout},
			sq:     sq,
			aio:    aio,
			flush:  make(chan int64, 1),
		}
	}

	return &Queue{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (q *Queue) String() string {
	return "queue"
}

func (q *Queue) Kind() t_aio.Kind {
	return t_aio.Queue
}

func (q *Queue) Start() error {
	for _, worker := range q.workers {
		go worker.Start()
	}
	return nil
}

func (q *Queue) Stop() error {
	close(q.sq)
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

// Worker

type QueueWorker struct {
	config *Config
	client *http.Client
	sq     <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	aio    aio.AIO
	flush  chan int64
}

func (w *QueueWorker) Start() {
	for {
		sqes, ok := util.Collect(w.sq, w.flush, w.config.BatchSize)
		if len(sqes) > 0 {
			for _, cqe := range w.Process(sqes) {
				w.aio.Enqueue(cqe)
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

		// instantiate cqe
		cqes[i] = &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Callback: sqe.Callback,
		}

		body, err := json.Marshal(&req{
			Id:      sqe.Submission.Queue.Task.Id,
			Counter: sqe.Submission.Queue.Task.Counter,
		})
		if err != nil {
			cqes[i].Error = err
			continue
		}

		switch t := sqe.Submission.Queue.Task.Message.Recv.Type; t {
		case "http":
			success, err := w.processHttp(sqe.Submission.Queue.Task.Message.Recv, body)
			if err != nil {
				cqes[i].Error = err
			} else {
				cqes[i].Completion = &t_aio.Completion{
					Kind:  t_aio.Queue,
					Tags:  sqe.Submission.Tags,
					Queue: &t_aio.QueueCompletion{Success: success},
				}
			}
		default:
			cqes[i].Error = fmt.Errorf("unsupported type %s", t)
		}
	}

	return cqes
}

// TODO: move to a plugin

type httpData struct {
	Url     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

func (w *QueueWorker) processHttp(recv *message.Recv, body []byte) (bool, error) {
	util.Assert(recv.Type == "http", "type must be http")

	fmt.Println(recv.Data)

	// unmarshal
	var data httpData
	if err := recv.As(&data); err != nil {
		return false, err
	}

	req, err := http.NewRequest("POST", data.Url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}

	req.Header.Set("Content-Type", "application/json")
	res, err := w.client.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == http.StatusOK, nil
}
