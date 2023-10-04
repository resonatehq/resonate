package aio

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/t_aio"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

type AIO interface {
	String() string
	Enqueue(*bus.SQE[t_aio.Submission, t_aio.Completion])
	Dequeue(int) []*bus.CQE[t_aio.Submission, t_aio.Completion]
	Done() bool
	Flush(int64)
}

type aio struct {
	cq         chan *bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]*subsystemWrapper
	done       bool
	errors     chan error
	metrics    *metrics.Metrics
}

type subsystemWrapper struct {
	Subsystem
	sq      chan<- *bus.SQE[t_aio.Submission, t_aio.Completion]
	workers []*workerWrapper
}

type workerWrapper struct {
	Worker
	sq      <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	cq      chan<- *bus.CQE[t_aio.Submission, t_aio.Completion]
	max     int
	flushCh chan int64
}

func New(size int, metrics *metrics.Metrics) *aio {
	return &aio{
		cq:         make(chan *bus.CQE[t_aio.Submission, t_aio.Completion], size),
		subsystems: map[t_aio.Kind]*subsystemWrapper{},
		errors:     make(chan error),
		metrics:    metrics,
	}
}

func (a *aio) AddSubsystem(kind t_aio.Kind, subsystem Subsystem, size int, max int, n int) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], size)
	workers := make([]*workerWrapper, n)

	for i := 0; i < n; i++ {
		workers[i] = &workerWrapper{
			Worker:  subsystem.NewWorker(i),
			sq:      sq,
			cq:      a.cq,
			max:     max,
			flushCh: make(chan int64, 1),
		}
	}

	a.subsystems[kind] = &subsystemWrapper{
		Subsystem: subsystem,
		sq:        sq,
		workers:   workers,
	}
}

func (a *aio) Start() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Start(); err != nil {
			return err
		}
		for _, worker := range subsystem.workers {
			go worker.start()
		}
	}

	return nil
}

func (a *aio) Stop() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Stop(); err != nil {
			return err
		}

		close(subsystem.sq)
	}

	close(a.cq)
	return nil
}

func (a *aio) Shutdown() {}

func (a *aio) Done() bool {
	return a.done
}

func (a *aio) Errors() <-chan error {
	return a.errors
}

func (a *aio) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	if subsystem, ok := a.subsystems[sqe.Submission.Kind]; ok {
		select {
		case subsystem.sq <- sqe:
			slog.Debug("aio:enqueue", "sqe", sqe.Submission)
			a.metrics.AioInFlight.WithLabelValues(sqe.Tags).Inc()
		default:
			sqe.Callback(0, nil, fmt.Errorf("aio:subsystem:%s submission queue full", subsystem))
		}
	} else {
		panic("invalid aio submission")
	}
}

func (a *aio) Dequeue(n int) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := []*bus.CQE[t_aio.Submission, t_aio.Completion]{}

	// collects n entries or until the channel is
	// exhausted, whichever happens first
	for i := 0; i < n; i++ {
		select {
		case cqe, ok := <-a.cq:
			if !ok {
				return cqes
			}

			var status string
			if cqe.Error != nil {
				status = "failure"
			} else {
				status = "success"
			}

			slog.Debug("aio:dequeue", "cqe", cqe.Completion)
			a.metrics.AioTotal.WithLabelValues(cqe.Tags, status).Inc()
			a.metrics.AioInFlight.WithLabelValues(cqe.Tags).Dec()

			cqes = append(cqes, cqe)
		default:
			return cqes
		}
	}

	return cqes
}

func (a *aio) Flush(t int64) {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		for _, worker := range subsystem.workers {
			worker.flush(t)
		}
	}
}

func (a *aio) String() string {
	return fmt.Sprintf(
		"AIO(size=%d, subsystems=%s)",
		cap(a.cq),
		a.subsystems,
	)
}

// worker

func (w *workerWrapper) start() {
	for {
		sqes, ok := w.collect()
		if len(sqes) > 0 {
			for _, cqe := range w.Process(sqes) {
				w.cq <- cqe
			}
		}
		if !ok {
			return
		}
	}
}

func (w *workerWrapper) flush(t int64) {
	// ignore case where flush channel is full,
	// this means the flush is waiting on the cq
	select {
	case w.flushCh <- t:
	default:
	}
}

func (w *workerWrapper) collect() ([]*bus.SQE[t_aio.Submission, t_aio.Completion], bool) {
	sqes := []*bus.SQE[t_aio.Submission, t_aio.Completion]{}

	for i := 0; i < w.max; i++ {
		select {
		case sqe, ok := <-w.sq:
			if !ok {
				return sqes, false
			}
			sqes = append(sqes, sqe)
		case <-w.flushCh:
			return sqes, true
		}
	}

	return sqes, true
}
