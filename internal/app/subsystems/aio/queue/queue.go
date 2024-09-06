package queue

import (
	"encoding/json"
	"fmt"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/http"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

// Config

type Config struct {
	Size    int          `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers int          `flag:"workers" desc:"number of workers" default:"1"`
	Plugins PluginConfig `flag:"plugin"`
}

type PluginConfig struct {
	Http EnabledPlugin[http.Config] `flag:"http"`
}

type EnabledPlugin[T any] struct {
	Enabled bool `flag:"enable" desc:"enable plugin" default:"true"`
	Config  T    `flag:"-"`
}

type DisabledPlugin[T any] struct {
	Enabled bool `flag:"enable" desc:"enable plugin" default:"false"`
	Config  T    `flag:"-"`
}

func (c *PluginConfig) Instantiate() ([]aio.Plugin, error) {
	plugins := []aio.Plugin{}
	if c.Http.Enabled {
		plugin, err := http.New(&c.Http.Config)
		if err != nil {
			return nil, err
		}

		plugins = append(plugins, plugin)
	}

	return plugins, nil
}

// Subsystem

type Queue struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*QueueWorker
}

func New(a aio.AIO, config *Config) (*Queue, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*QueueWorker, config.Workers)

	p, err := config.Plugins.Instantiate()
	if err != nil {
		return nil, err
	}

	plugins := map[string]aio.Plugin{}
	for _, plugin := range p {
		plugins[plugin.Type()] = plugin
	}

	for i := 0; i < config.Workers; i++ {
		workers[i] = &QueueWorker{
			config:  config,
			sq:      sq,
			aio:     a,
			plugins: plugins,
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

func (q *Queue) Flush(t int64) {}

// Worker

type QueueWorker struct {
	config  *Config
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	aio     aio.AIO
	plugins map[string]aio.Plugin
}

func (w *QueueWorker) Start() {
	for {
		sqe, ok := <-w.sq
		if !ok {
			return
		}

		// process one at a time
		w.aio.Enqueue(w.Process(sqe))
	}
}

type req struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (w *QueueWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(sqe.Submission.Queue != nil, "queue submission must not be nil")
	util.Assert(sqe.Submission.Queue.Task != nil, "task must not be nil")
	util.Assert(sqe.Submission.Queue.Task.Message != nil, "message must not be nil")

	// instantiate cqe
	cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Id:       sqe.Id,
		Callback: sqe.Callback,
	}

	body, err := json.Marshal(&req{
		Id:      sqe.Submission.Queue.Task.Id,
		Counter: sqe.Submission.Queue.Task.Counter,
	})
	if err != nil {
		cqe.Error = err
		return cqe
	}

	if plugin, ok := w.plugins[sqe.Submission.Queue.Task.Recv.Type]; ok {
		success, err := plugin.Enqueue(sqe.Submission.Queue.Task.Recv.Data, body)
		if err != nil {
			cqe.Error = err
		} else {
			cqe.Completion = &t_aio.Completion{
				Kind:  t_aio.Queue,
				Tags:  sqe.Submission.Tags,
				Queue: &t_aio.QueueCompletion{Success: success},
			}
		}
	} else {
		cqe.Error = fmt.Errorf("unsupported type %s", sqe.Submission.Queue.Task.Recv.Type)
	}

	return cqe
}
