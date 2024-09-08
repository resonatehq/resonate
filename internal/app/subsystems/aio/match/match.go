package match

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/receiver"
)

// Config

type Config struct {
	Size     int             `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers  int             `flag:"workers" desc:"number of workers" default:"1" dst:"1"`
	Matchers []MatcherConfig `flag:"matchers" desc:"matcher configurations"`
}

type MatcherConfig struct {
	Name   string
	Type   string
	Config json.RawMessage
}

type TagMatcherConfig struct {
	Key     string
	Default *receiver.Recv
}

// Subsystem

type Match struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*MatchWorker
}

func New(aio aio.AIO, metrics *metrics.Metrics, config *Config) (*Match, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*MatchWorker, config.Workers)
	matchers := make([]func(*promise.Promise) (*receiver.Recv, bool), len(config.Matchers))

	for i, matcher := range config.Matchers {
		switch matcher.Type {
		case "tag":
			var config *TagMatcherConfig
			if err := json.Unmarshal(matcher.Config, &config); err != nil {
				return nil, err
			}

			matchers[i] = TagMatcher(config)
		default:
			return nil, fmt.Errorf("unknown matcher type: %s", matcher.Type)
		}
	}

	for i := 0; i < config.Workers; i++ {
		workers[i] = &MatchWorker{
			i:        i,
			sq:       sq,
			matchers: matchers,
			aio:      aio,
			metrics:  metrics,
		}
	}

	return &Match{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (m *Match) String() string {
	return t_aio.Match.String()
}

func (m *Match) Kind() t_aio.Kind {
	return t_aio.Match
}

func (m *Match) Start() error {
	for _, worker := range m.workers {
		go worker.Start()
	}
	return nil
}

func (m *Match) Stop() error {
	close(m.sq)
	return nil
}

func (m *Match) SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion] {
	return m.sq
}

func (m *Match) Flush(int64) {}

func (m *Match) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(m.workers) > 0, "must be at least one worker")

	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))
	for i, sqe := range sqes {
		cqes[i] = m.workers[0].Process(sqe)
	}

	return cqes
}

// Worker

type MatchWorker struct {
	i        int
	sq       <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	matchers []func(*promise.Promise) (*receiver.Recv, bool)
	aio      aio.AIO
	metrics  *metrics.Metrics
}

func (w *MatchWorker) String() string {
	return t_aio.Match.String()
}

func (w *MatchWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		sqe, ok := <-w.sq
		if !ok {
			return
		}

		counter.Inc()
		w.aio.Enqueue(w.Process(sqe)) // process one at a time
		counter.Dec()
	}
}

type message struct {
	Type    string           `json:"type"`
	Promise *promise.Promise `json:"promise"`
}

func (w *MatchWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(sqe.Submission != nil, "submission must not be nil")
	util.Assert(sqe.Submission.Match != nil, "match must not be nil")
	util.Assert(sqe.Submission.Match.Promise != nil, "promise must not be nil")

	cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Id:       sqe.Id,
		Callback: sqe.Callback,
	}

	message, err := json.Marshal(&message{
		Type:    "invoke",
		Promise: sqe.Submission.Match.Promise,
	})
	if err != nil {
		cqe.Error = err
		return cqe
	}

	// apply matchers in succession, first match wins
	for _, f := range w.matchers {
		if recv, ok := f(sqe.Submission.Match.Promise); ok {
			cqe.Completion = &t_aio.Completion{
				Kind: t_aio.Match,
				Tags: sqe.Submission.Tags,
				Match: &t_aio.MatchCompletion{
					Matched: true,
					Command: &t_aio.CreateTaskCommand{
						RecvType: recv.Type,
						RecvData: recv.Data,
						Message:  message,
						Timeout:  sqe.Submission.Match.Promise.Timeout,
					},
				},
			}
			return cqe
		}
	}

	cqe.Completion = &t_aio.Completion{
		Kind:  t_aio.Match,
		Tags:  sqe.Submission.Tags,
		Match: &t_aio.MatchCompletion{Matched: false},
	}
	return cqe
}

// Matcher functions

func TagMatcher(config *TagMatcherConfig) func(*promise.Promise) (*receiver.Recv, bool) {
	return func(p *promise.Promise) (*receiver.Recv, bool) {
		util.Assert(p.Tags != nil, "tags must be set")

		if v, ok := p.Tags[config.Key]; ok {
			if config.Default != nil {
				switch config.Default.Type {
				case "http":
					return config.Default, true
				default:
					return nil, false
				}
			} else {
				switch protocol(v) {
				case "http", "https":
					data, err := json.Marshal(map[string]interface{}{"url": v})
					if err != nil {
						return nil, false
					}

					return &receiver.Recv{
						Type: "http",
						Data: data,
					}, true
				default:
					return nil, false
				}
			}
		}

		return nil, false
	}
}

func protocol(value string) string {
	url, err := url.Parse(value)
	if err != nil {
		return ""
	}

	return url.Scheme
}
