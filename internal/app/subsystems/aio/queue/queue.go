package queue

import (
	"encoding/json"
	"fmt"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/http"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/receiver"
)

// Config

type Config struct {
	Size         int                 `flag:"size" desc:"submission buffered channel size" default:"100"`
	Plugins      PluginConfig        `flag:"plugin"`
	Destinations []DestinationConfig `flag:"destinations" desc:"destination configurations"`
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

func (c *PluginConfig) Instantiate(a aio.AIO, metrics *metrics.Metrics) ([]aio.Plugin, error) {
	plugins := []aio.Plugin{}
	if c.Http.Enabled {
		plugin, err := http.New(a, metrics, &c.Http.Config)
		if err != nil {
			return nil, err
		}

		plugins = append(plugins, plugin)
	}

	return plugins, nil
}

type DestinationConfig struct {
	Name string
	Type string
	Data json.RawMessage
}

// Subsystem

type Queue struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	plugins []aio.Plugin
	worker  *QueueWorker
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Queue, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)

	plugins, err := config.Plugins.Instantiate(a, metrics)
	if err != nil {
		return nil, err
	}

	destinations := map[string]*receiver.Recv{}
	for _, dest := range config.Destinations {
		destinations[dest.Name] = &receiver.Recv{Type: dest.Type, Data: dest.Data}
	}

	worker := &QueueWorker{
		sq:           sq,
		plugins:      map[string]aio.Plugin{},
		destinations: destinations,
		aio:          a,
		metrics:      metrics,
	}

	for _, plugin := range plugins {
		worker.AddPlugin(plugin)
	}

	return &Queue{
		config:  config,
		sq:      sq,
		worker:  worker,
		plugins: plugins,
	}, nil
}

func (q *Queue) String() string {
	return t_aio.Queue.String()
}

func (q *Queue) Kind() t_aio.Kind {
	return t_aio.Queue
}

func (q *Queue) Start() error {
	// start plugins
	for _, plugin := range q.plugins {
		if err := plugin.Start(); err != nil {
			return err
		}
	}

	// start worker
	go q.worker.Start()

	return nil
}

func (q *Queue) Stop() error {
	// first close sq
	close(q.sq)

	// then stop plugins
	for _, plugin := range q.plugins {
		if err := plugin.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (q *Queue) SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion] {
	return q.sq
}

func (q *Queue) Flush(t int64) {}

// Worker

type QueueWorker struct {
	sq           <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	plugins      map[string]aio.Plugin
	destinations map[string]*receiver.Recv
	aio          aio.AIO
	metrics      *metrics.Metrics
}

func (w *QueueWorker) String() string {
	return t_aio.Queue.String()
}

func (w *QueueWorker) AddPlugin(plugin aio.Plugin) {
	w.plugins[plugin.Type()] = plugin
}

func (w *QueueWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), "0")
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		sqe, ok := <-w.sq
		if !ok {
			return
		}

		// process one at a time
		counter.Inc()
		w.Process(sqe)
		counter.Dec()
	}
}

type body struct {
	Id      string `json:"id"`
	Counter int    `json:"counter"`
}

func (w *QueueWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	util.Assert(sqe.Submission.Queue != nil, "queue submission must not be nil")
	util.Assert(sqe.Submission.Queue.Task != nil, "recv must not be nil")
	util.Assert(sqe.Submission.Queue.Body != nil, "body must not be nil")

	// instantiate cqe
	cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Id:       sqe.Id,
		Callback: sqe.Callback,
	}

	var logicalRecv *string
	var physicalRecv *receiver.Recv
	if err := util.UnmarshalChain(sqe.Submission.Queue.Task.Recv, &logicalRecv, &physicalRecv); err != nil {
		cqe.Error = err
		w.aio.Enqueue(cqe)
		return
	}

	util.Assert((logicalRecv != nil) != (physicalRecv != nil), "one of logical or physical recv must be nil, but not both")

	var recv *receiver.Recv
	if logicalRecv != nil {
		recv = w.destinations[*logicalRecv]
	} else {
		recv = physicalRecv
	}

	if recv == nil {
		cqe.Error = fmt.Errorf("unknown receiver %s", *logicalRecv)
		w.aio.Enqueue(cqe)
		return
	}

	plugin := w.plugins[recv.Type]
	if plugin == nil {
		cqe.Error = fmt.Errorf("unknown plugin %s", recv.Type)
		w.aio.Enqueue(cqe)
		return
	}

	counter := w.metrics.AioInFlight.WithLabelValues(plugin.String())

	ok := plugin.Enqueue(&aio.Message{
		Data: recv.Data,
		Body: sqe.Submission.Queue.Body,
		Done: func(success bool, err error) {
			if err != nil {
				cqe.Error = err
			} else {
				cqe.Completion = &t_aio.Completion{
					Kind:  t_aio.Queue,
					Tags:  sqe.Submission.Tags,
					Queue: &t_aio.QueueCompletion{Success: success},
				}
			}

			w.aio.Enqueue(cqe)

			counter.Dec()
			w.metrics.AioTotal.WithLabelValues(plugin.String(), boolToStatus(success)).Inc()
		},
	})

	if ok {
		counter.Inc()
	} else {
		cqe.Error = fmt.Errorf("aio:%s:%s submission queue full", w, recv.Type)
		w.aio.Enqueue(cqe)
	}
}

func boolToStatus(b bool) string {
	switch b {
	case true:
		return "success"
	default:
		return "failure"
	}
}
