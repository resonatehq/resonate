package sender

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/http"
	"github.com/resonatehq/resonate/internal/app/plugins/poll"
	"github.com/resonatehq/resonate/internal/app/plugins/pubsub"
	"github.com/resonatehq/resonate/internal/app/plugins/sqs"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/receiver"
)

// Config

type Config struct {
	Size    int            `flag:"size" desc:"submission buffered channel size" default:"100"`
	Plugins PluginConfig   `flag:"plugin"`
	Targets []TargetConfig `flag:"targets" desc:"target config"`
}

type PluginConfig struct {
	Http   EnabledPlugin[http.Config]    `flag:"http"`
	Poll   EnabledPlugin[poll.Config]    `flag:"poll"`
	SQS    DisabledPlugin[sqs.Config]    `flag:"sqs"`
	PubSub DisabledPlugin[pubsub.Config] `flag:"pubsub"`
}

type EnabledPlugin[T any] struct {
	Enabled bool `flag:"enable" desc:"enable plugin" default:"true"`
	Config  T    `flag:"-"`
}

type DisabledPlugin[T any] struct {
	Enabled bool `flag:"enable" desc:"enable plugin" default:"false"`
	Config  T    `flag:"-"`
}

type TargetConfig struct {
	Name string
	Type string
	Data json.RawMessage
}

// Subsystem

type Sender struct {
	config  *Config
	sq      chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	plugins []aio.Plugin
	worker  *SenderWorker
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config, plugins []aio.Plugin) (*Sender, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)

	targets := map[string]*receiver.Recv{}
	for _, target := range config.Targets {
		targets[target.Name] = &receiver.Recv{Type: target.Type, Data: target.Data}
	}

	if _, ok := targets["default"]; !ok {
		// add default target if none
		targets["default"] = &receiver.Recv{Type: "poll", Data: []byte(`{"cast":"any","group":"default"}`)}
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

func (s *Sender) Start(errors chan<- error) error {
	// start plugins
	for _, plugin := range s.plugins {
		if err := plugin.Start(errors); err != nil {
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

func (s *Sender) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) bool {
	select {
	case s.sq <- sqe:
		return true
	default:
		return false
	}
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

		slog.Debug("api:sqe:dequeue", "id", sqe.Id, "sqe", sqe)

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
		w.aio.EnqueueCQE(cqe)
		return
	}

	util.Assert((logicalRecv != nil) != (physicalRecv != nil), "one of logical or physical recv must be nil, but not both")

	var recv *receiver.Recv

	if logicalRecv != nil {
		recv = w.targets[*logicalRecv]
		if recv == nil {
			recv, _ = schemeToRecv(*logicalRecv)
		}
	} else {
		recv = physicalRecv
	}

	if recv == nil {
		cqe.Error = fmt.Errorf("unknown receiver %s", *logicalRecv)
		w.aio.EnqueueCQE(cqe)
		return
	}

	plugin := w.plugins[recv.Type]
	if plugin == nil {
		cqe.Error = fmt.Errorf("unknown plugin %s", recv.Type)
		w.aio.EnqueueCQE(cqe)
		return
	}

	var body []byte
	var err error

	if sqe.Submission.Sender.Task.Mesg.Type == message.Notify {
		util.Assert(sqe.Submission.Sender.Promise != nil, "promise must not be nil for a notify message")
		body, err = json.Marshal(map[string]interface{}{
			"type":    sqe.Submission.Sender.Task.Mesg.Type,
			"promise": sqe.Submission.Sender.Promise,
		})
	} else {
		body, err = json.Marshal(map[string]interface{}{
			"type": sqe.Submission.Sender.Task.Mesg.Type,
			"task": sqe.Submission.Sender.Task,
			"href": map[string]string{
				"base":      sqe.Submission.Sender.BaseHref,
				"claim":     sqe.Submission.Sender.ClaimHref,
				"complete":  sqe.Submission.Sender.CompleteHref,
				"heartbeat": sqe.Submission.Sender.HeartbeatHref,
			},
		})
	}

	if err != nil {
		cqe.Error = err
		w.aio.EnqueueCQE(cqe)
		return
	}

	counter := w.metrics.AioInFlight.WithLabelValues(plugin.String())

	ok := plugin.Enqueue(&aio.Message{
		Type: sqe.Submission.Sender.Task.Mesg.Type,
		Addr: recv.Data,
		Head: sqe.Submission.Sender.Task.Mesg.Head,
		Body: body,
		Done: func(completion *t_aio.SenderCompletion) {
			cqe.Completion = &t_aio.Completion{
				Kind:   t_aio.Sender,
				Tags:   sqe.Submission.Tags,
				Sender: completion,
			}

			w.aio.EnqueueCQE(cqe)

			counter.Dec()
			w.metrics.AioTotal.WithLabelValues(plugin.String(), boolToStatus(completion.Success)).Inc()
		},
	})

	if ok {
		counter.Inc()
	} else {
		cqe.Error = fmt.Errorf("aio:%s:%s submission queue full", w, recv.Type)
		w.aio.EnqueueCQE(cqe)
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

func schemeToRecv(v string) (*receiver.Recv, bool) {
	u, err := url.Parse(v)
	if err != nil {
		return nil, false
	}

	switch u.Scheme {
	case "http", "https":
		data, err := json.Marshal(map[string]string{"url": u.String()})
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "http", Data: data}, true

	case "poll":
		cast := "any"
		if u.User != nil {
			if cast = strings.ToLower(u.User.Username()); cast != "uni" && cast != "any" {
				return nil, false
			}
		}

		addr := map[string]string{"cast": cast, "group": u.Host}
		if id := strings.TrimPrefix(u.Path, "/"); id != "" {
			addr["id"] = id
		}

		data, err := json.Marshal(addr)
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "poll", Data: data}, true

	case "sqs+https":
		addr := map[string]string{
			"url": fmt.Sprintf("https://%s%s", u.Host, u.Path),
		}

		data, err := json.Marshal(addr)
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "sqs", Data: data}, true

	case "pubsub":
		topic := strings.TrimPrefix(u.Path, "/")
		if topic == "" {
			return nil, false
		}

		addr := map[string]string{
			"topic": topic,
		}

		data, err := json.Marshal(addr)
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "pubsub", Data: data}, true

	default:
		return nil, false
	}
}
