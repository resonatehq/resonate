package echo

import (
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/util"
)

// Config

type Config struct {
	Size      int `flag:"size" desc:"submission buffered channel size" default:"100"`
	BatchSize int `flag:"batch-size" desc:"max submissions processed per iteration" default:"100"`
	Workers   int `flag:"workers" desc:"number of workers" default:"1"`
}

// Subsystem

type Echo struct {
	config  *Config
	sq      chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*EchoWorker
}

func New(aio aio.AIO, config *Config) (*Echo, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*EchoWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &EchoWorker{
			config: config,
			sq:     sq,
			aio:    aio,
			flush:  make(chan int64, 1),
		}
	}

	return &Echo{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (e *Echo) String() string {
	return "echo"
}

func (e *Echo) Kind() t_aio.Kind {
	return t_aio.Echo
}

func (e *Echo) Start() error {
	for _, worker := range e.workers {
		go worker.Start()
	}
	return nil
}

func (e *Echo) Stop() error {
	close(e.sq)
	return nil
}

func (e *Echo) SQ() chan<- *bus.SQE[t_aio.Submission, t_aio.Completion] {
	return e.sq
}

func (e *Echo) Flush(t int64) {
	for _, worker := range e.workers {
		worker.Flush(t)
	}
}

func (e *Echo) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(e.workers) > 0, "must be at least one worker")
	return e.workers[0].Process(sqes)
}

// Worker

type EchoWorker struct {
	config *Config
	sq     <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	aio    aio.AIO
	flush  chan int64
}

func (w *EchoWorker) Start() {
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

func (w *EchoWorker) Flush(t int64) {
	// ignore case where flush channel is full,
	// this means the flush is waiting on the cq
	select {
	case w.flush <- t:
	default:
	}
}

func (w *EchoWorker) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))

	for i, sqe := range sqes {
		cqes[i] = &bus.CQE[t_aio.Submission, t_aio.Completion]{
			Id: sqe.Id,
			Completion: &t_aio.Completion{
				Kind: t_aio.Echo,
				Tags: sqe.Submission.Tags, // propagate the tags
				Echo: &t_aio.EchoCompletion{
					Data: sqe.Submission.Echo.Data,
				},
			},
			Callback: sqe.Callback,
		}
	}

	return cqes
}
