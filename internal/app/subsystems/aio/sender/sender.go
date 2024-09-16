package sender

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
	Size    int            `flag:"size" desc:"submission buffered channel size" default:"100"`
	Plugins PluginConfig   `flag:"plugin"`
	Targets []TargetConfig `flag:"targets" desc:"target config"`
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

type TargetConfig struct {
	Name string
	Type string
	Data json.RawMessage
}

// Subsystem

type Sender struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	plugins []aio.Plugin
	worker  *SenderWorker
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Sender, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)

	plugins, err := config.Plugins.Instantiate(a, metrics)
	if err != nil {
		return nil, err
	}

	targets := map[string]*receiver.Recv{}
	for _, target := range config.Targets {
		targets[target.Name] = &receiver.Recv{Type: target.Type, Data: target.Data}
	}

	worker := &SenderWorker{
		sq:      sq,
		plugins: map[string]aio.Plugin{},
		targets: targets,
		aio:     a,
		metrics: metrics,
	}

	for _, plugin := range plugins {
		worker.AddPlugin(plugin)
	}

	return &Sender{
		config:  config,
		sq:      sq,
		worker:  worker,
		plugins: plugins,
	}, nil
}

func (s *Sender) String() string {
	return t_aio.Sender.String()
}

func (s *Sender) Kind() t_aio.Kind {
	return t_aio.Sender
}

func (s *Sender) Start() error {
	// start plugins
	for _, plugin := range s.plugins {
		if err := plugin.Start(); err != nil {
			return err
		}
	}

	// start worker
	go s.worker.Start()

	return nil
}

func (s *Sender) Stop() error {
	// first close sq
	close(s.sq)

	// then stop plugins
	for _, plugin := range s.plugins {
		if err := plugin.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sender) SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion] {
	return s.sq
}

func (s *Sender) Flush(t int64) {}

// Worker

type SenderWorker struct {
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	plugins map[string]aio.Plugin
	targets map[string]*receiver.Recv
	aio     aio.AIO
	metrics *metrics.Metrics
}

func (w *SenderWorker) String() string {
	return t_aio.Sender.String()
}

func (w *SenderWorker) AddPlugin(plugin aio.Plugin) {
	w.plugins[plugin.Type()] = plugin
}

func (w *SenderWorker) Start() {
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

func (w *SenderWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	util.Assert(sqe.Submission.Sender != nil, "sender submission must not be nil")
	util.Assert(sqe.Submission.Sender.Task != nil, "recv must not be nil")

	// instantiate cqe
	cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Id:       sqe.Id,
		Callback: sqe.Callback,
	}

	var logicalRecv *string
	var physicalRecv *receiver.Recv
	if err := util.UnmarshalChain(sqe.Submission.Sender.Task.Recv, &logicalRecv, &physicalRecv); err != nil {
		cqe.Error = err
		w.aio.Enqueue(cqe)
		return
	}

	util.Assert((logicalRecv != nil) != (physicalRecv != nil), "one of logical or physical recv must be nil, but not both")

	var recv *receiver.Recv
	if logicalRecv != nil {
		recv = w.targets[*logicalRecv]
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

	body, err := json.Marshal(map[string]interface{}{
		"id":      sqe.Submission.Sender.Task.Id,
		"counter": sqe.Submission.Sender.Task.Counter,
	})
	if err != nil {
		cqe.Error = err
		w.aio.Enqueue(cqe)
		return
	}

	counter := w.metrics.AioInFlight.WithLabelValues(plugin.String())

	ok := plugin.Enqueue(&aio.Message{
		Data: recv.Data,
		Body: body,
		Done: func(success bool, err error) {
			if err != nil {
				cqe.Error = err
			} else {
				cqe.Completion = &t_aio.Completion{
					Kind:   t_aio.Sender,
					Tags:   sqe.Submission.Tags,
					Sender: &t_aio.SenderCompletion{Success: success},
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
