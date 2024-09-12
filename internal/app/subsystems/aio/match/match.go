package match

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/receiver"
)

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
	Key string
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
	matchers := make([]func(*promise.Promise) (any, bool), len(config.Matchers))

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
	matchers []func(*promise.Promise) (any, bool)
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

func (w *MatchWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(sqe.Submission != nil, "submission must not be nil")
	util.Assert(sqe.Submission.Match != nil, "match must not be nil")
	util.Assert(sqe.Submission.Match.Promise != nil, "promise must not be nil")

	cqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Id:       sqe.Id,
		Callback: sqe.Callback,
	}

	// apply matchers in succession, first match wins
	for _, f := range w.matchers {
		if v, ok := w.match(f(sqe.Submission.Match.Promise)); ok {
			recv, err := json.Marshal(v)
			if err != nil {
				slog.Error("failed to marshal recv", "err", err)
				break
			}

			mesg, err := json.Marshal(&message.Mesg{
				Type: message.Invoke,
				Root: sqe.Submission.Match.Promise.Id,
				Leaf: sqe.Submission.Match.Promise.Id,
			})
			if err != nil {
				slog.Error("failed to marshal mesg", "err", err)
				break
			}

			cqe.Completion = &t_aio.Completion{
				Kind: t_aio.Match,
				Tags: sqe.Submission.Tags,
				Match: &t_aio.MatchCompletion{
					Matched: true,
					Command: &t_aio.CreateTaskCommand{
						Recv:    recv,
						Mesg:    mesg,
						Timeout: sqe.Submission.Match.Promise.Timeout,
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

func (w *MatchWorker) match(v any, ok bool) (any, bool) {
	if ok {
		switch v := v.(type) {
		case *receiver.Recv:
			return v, true
		case string:
			if recv, ok := protocolToRecv(v); ok {
				return recv, true
			}

			// will be matched against logical receivers in the queueing
			// subsystem
			return v, true
		}
	}

	return nil, false
}

// Matcher functions

func TagMatcher(config *TagMatcherConfig) func(*promise.Promise) (any, bool) {
	return func(p *promise.Promise) (any, bool) {
		util.Assert(p.Tags != nil, "tags must be set")

		if v, ok := p.Tags[config.Key]; ok {
			// check if value can be unmarshaled to a recv
			var recv *receiver.Recv
			if err := json.Unmarshal([]byte(v), &recv); err == nil {
				return recv, true
			}

			// otherwise return the string value
			return v, true
		}

		return nil, false
	}
}

func protocolToRecv(v string) (*receiver.Recv, bool) {
	switch protocol(v) {
	case "http", "https":
		data, err := json.Marshal(map[string]interface{}{"url": v})
		if err != nil {
			return nil, false
		}

		return &receiver.Recv{Type: "http", Data: data}, true
	default:
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
