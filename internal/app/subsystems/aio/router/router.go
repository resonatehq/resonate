package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/receiver"
)

type Config struct {
	Size    int            `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers int            `flag:"workers" desc:"number of workers" default:"1" dst:"1"`
	Sources []SourceConfig `flag:"sources" desc:"source config"`
}

type SourceConfig struct {
	Name string
	Type string
	Data json.RawMessage
}

type TagSourceConfig struct {
	Key string
}

// Subsystem

type Router struct {
	config  *Config
	sq      chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*RouterWorker
}

func New(aio aio.AIO, metrics *metrics.Metrics, config *Config) (*Router, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*RouterWorker, config.Workers)
	sources := make([]func(*promise.Promise) (any, bool), len(config.Sources))

	for i, source := range config.Sources {
		switch source.Type {
		case "tag":
			var config *TagSourceConfig
			if err := json.Unmarshal(source.Data, &config); err != nil {
				return nil, err
			}

			sources[i] = TagSource(config)
		default:
			return nil, fmt.Errorf("unknown source type: %s", source.Type)
		}
	}

	for i := 0; i < config.Workers; i++ {
		workers[i] = &RouterWorker{
			i:       i,
			sq:      sq,
			sources: sources,
			aio:     aio,
			metrics: metrics,
		}
	}

	return &Router{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (r *Router) String() string {
	return t_aio.Router.String()
}

func (r *Router) Kind() t_aio.Kind {
	return t_aio.Router
}

func (r *Router) Start(chan<- error) error {
	for _, worker := range r.workers {
		go worker.Start()
	}
	return nil
}

func (r *Router) Stop() error {
	close(r.sq)
	return nil
}

func (r *Router) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) bool {
	select {
	case r.sq <- sqe:
		return true
	default:
		return false
	}
}

func (r *Router) Flush(int64) {}

func (r *Router) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(r.workers) > 0, "must be at least one worker")

	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))
	for i, sqe := range sqes {
		cqes[i] = r.workers[0].Process(sqe)
	}

	return cqes
}

// Worker

type RouterWorker struct {
	i       int
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	sources []func(*promise.Promise) (any, bool)
	aio     aio.AIO
	metrics *metrics.Metrics
}

func (w *RouterWorker) String() string {
	return t_aio.Router.String()
}

func (w *RouterWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		sqe, ok := <-w.sq
		if !ok {
			return
		}

		slog.Debug("api:sqe:dequeue", "id", sqe.Id, "sqe", sqe)

		counter.Inc()
		w.aio.EnqueueCQE(w.Process(sqe)) // process one at a time
		counter.Dec()
	}
}

func (w *RouterWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(sqe.Submission != nil, "submission must not be nil")
	util.Assert(sqe.Submission.Router != nil, "router must not be nil")
	util.Assert(sqe.Submission.Router.Promise != nil, "promise must not be nil")

	cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Id:       sqe.Id,
		Callback: sqe.Callback,
	}

	// apply sources in succession, first match wins
	for _, f := range w.sources {
		if v, ok := f(sqe.Submission.Router.Promise); ok {
			v, ok := coerce(v)
			if !ok {
				slog.Error("failed to coerce to recv", "v", v)
				break
			}

			recv, err := json.Marshal(v)
			if err != nil {
				slog.Error("failed to marshal recv", "err", err)
				break
			}

			cqe.Completion = &t_aio.Completion{
				Kind: t_aio.Router,
				Tags: sqe.Submission.Tags,
				Router: &t_aio.RouterCompletion{
					Matched: true,
					Recv:    recv,
				},
			}
			return cqe
		}
	}

	cqe.Completion = &t_aio.Completion{
		Kind:   t_aio.Router,
		Tags:   sqe.Submission.Tags,
		Router: &t_aio.RouterCompletion{Matched: false},
	}
	return cqe
}

// Source functions

func TagSource(config *TagSourceConfig) func(*promise.Promise) (any, bool) {
	return func(p *promise.Promise) (any, bool) {
		util.Assert(p.Tags != nil, "tags must be set")

		v, ok := p.Tags[config.Key]

		// not a match if tag is not present
		if !ok {
			return nil, false
		}

		// check if tag is valid json
		if b := []byte(v); json.Valid(b) {
			var recv *receiver.Recv

			decoder := json.NewDecoder(bytes.NewReader(b))
			decoder.DisallowUnknownFields()

			// valid json is a match
			if err := decoder.Decode(&recv); err == nil && recv.Type != "" {
				return recv, true
			}

			// invalid json is not a match
			return nil, false
		}

		// otherwise return the string value
		return v, true
	}
}

// Helper functions

func coerce(v any) (any, bool) {
	switch v := v.(type) {
	case *receiver.Recv:
		return v, true
	case string:
		if recv, ok := schemeToRecv(v); ok {
			return recv, true
		}

		// will be matched against logical receivers in the queueing
		// subsystem
		return v, true
	default:
		return nil, false
	}
}

func schemeToRecv(v string) (*receiver.Recv, bool) {
	u, err := url.Parse(v)
	if err != nil {
		return nil, false
	}

	switch u.Scheme {
	case "http", "https":
		data, err := json.Marshal(map[string]interface{}{"url": u.String()})
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "http", Data: data}, true
	case "poll":
		data, err := json.Marshal(map[string]interface{}{"group": u.Host, "id": strings.TrimPrefix(u.Path, "/")})
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "poll", Data: data}, true
	default:
		return nil, false
	}
}
