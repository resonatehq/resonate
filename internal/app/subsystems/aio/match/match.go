package match

import (
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
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
	Config interface{}
}

type TagMatcherConfig struct {
	Tag string
}

// Subsystem

type Match struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*MatchWorker
}

func New(aio aio.AIO, config *Config) (*Match, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*MatchWorker, config.Workers)

	matchers := make([]func(*promise.Promise) (*t_aio.CreateTaskCommand, bool), len(config.Matchers))
	for i, matcherConfig := range config.Matchers {
		switch matcherConfig.Type {
		case "tag":
			var tagMatcherConfig *TagMatcherConfig
			err := mapstructure.Decode(matcherConfig.Config, &tagMatcherConfig)
			if err != nil {
				return nil, err
			}
			matchers[i] = TagMatcher(tagMatcherConfig)
		default:
			return nil, fmt.Errorf("unknown matcher type: %s", matcherConfig.Type)
		}
	}

	for i := 0; i < config.Workers; i++ {
		workers[i] = &MatchWorker{
			sq:       sq,
			aio:      aio,
			matchers: matchers,
		}
	}

	return &Match{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (m *Match) String() string {
	return "match"
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
	sq       <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	aio      aio.AIO
	matchers []func(*promise.Promise) (*t_aio.CreateTaskCommand, bool)
}

func (w *MatchWorker) Start() {
	for {
		sqe, ok := <-w.sq
		if !ok {
			return
		}

		// process one at a time
		w.aio.Enqueue(w.Process(sqe))
	}
}

func (w *MatchWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(sqe.Submission != nil, "submission must not be nil")
	util.Assert(sqe.Submission.Match != nil, "match must not be nil")
	util.Assert(sqe.Submission.Match.Promise != nil, "promise must not be nil")

	// apply matchers in succession, first match wins
	for _, f := range w.matchers {
		if command, ok := f(sqe.Submission.Match.Promise); ok {
			return &bus.CQE[t_aio.Submission, t_aio.Completion]{
				Completion: &t_aio.Completion{
					Kind:  t_aio.Match,
					Tags:  sqe.Submission.Tags,
					Match: &t_aio.MatchCompletion{Matched: true, Command: command},
				},
				Callback: sqe.Callback,
				Error:    nil,
			}
		}
	}

	return &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Completion: &t_aio.Completion{
			Kind:  t_aio.Match,
			Tags:  sqe.Submission.Tags,
			Match: &t_aio.MatchCompletion{Matched: false},
		},
		Callback: sqe.Callback,
		Error:    nil,
	}
}

// Matcher functions

func TagMatcher(config *TagMatcherConfig) func(*promise.Promise) (*t_aio.CreateTaskCommand, bool) {
	return func(p *promise.Promise) (*t_aio.CreateTaskCommand, bool) {
		util.Assert(p.Tags != nil, "tags must be set")

		if recv, ok := p.Tags[config.Tag]; ok {
			return &t_aio.CreateTaskCommand{
				PromiseId: p.Id,
				Message: &message.Message{
					Recv: recv,
					Data: p.Param.Data,
				},
			}, true
		}
		return nil, false
	}
}
