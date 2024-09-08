package echo

import (
	"strconv"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
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

func New(aio aio.AIO, metrics *metrics.Metrics, config *Config) (*Echo, error) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*EchoWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &EchoWorker{
			i:       i,
			sq:      sq,
			flush:   make(chan int64, 1),
			aio:     aio,
			metrics: metrics,
		}
	}

	return &Echo{
		config:  config,
		sq:      sq,
		workers: workers,
	}, nil
}

func (e *Echo) String() string {
	return t_aio.Echo.String()
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

func (e *Echo) Flush(t int64) {}

func (e *Echo) Process(sqes []*bus.SQE[t_aio.Submission, t_aio.Completion]) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	util.Assert(len(e.workers) > 0, "must be at least one worker")

	cqes := make([]*bus.CQE[t_aio.Submission, t_aio.Completion], len(sqes))
	for i, sqe := range sqes {
		cqes[i] = e.workers[0].Process(sqe)
	}

	return cqes
}

// Worker

type EchoWorker struct {
	i       int
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	flush   chan int64
	aio     aio.AIO
	metrics *metrics.Metrics
}

func (w *EchoWorker) String() string {
	return t_aio.Echo.String()
}

func (w *EchoWorker) Start() {
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

func (w *EchoWorker) Process(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	return &bus.CQE[t_aio.Submission, t_aio.Completion]{
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
