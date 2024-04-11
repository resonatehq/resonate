package aio

import (
	"fmt"
	"log/slog"

	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
)

type AIO interface {
	String() string
	AddSubsystem(kind t_aio.Kind, subsystem Subsystem, config *SubsystemConfig)
	Start() error
	Stop() error
	Shutdown()
	Enqueue(*bus.SQE[t_aio.Submission, t_aio.Completion])
	Dequeue(int) []*bus.CQE[t_aio.Submission, t_aio.Completion]
	Flush(int64)
	Errors() <-chan error
}

type aio struct {
	cq         chan *bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]*subsystemWrapper
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
	sq        <-chan *bus.SQE[t_aio.Submission, t_aio.Completion]
	cq        chan<- *bus.CQE[t_aio.Submission, t_aio.Completion]
	flushCh   chan int64
	batchSize int
}

func New(size int, metrics *metrics.Metrics) AIO {
	return &aio{
		cq:         make(chan *bus.CQE[t_aio.Submission, t_aio.Completion], size),
		subsystems: map[t_aio.Kind]*subsystemWrapper{},
		errors:     make(chan error),
		metrics:    metrics,
	}
}

func (a *aio) AddSubsystem(kind t_aio.Kind, subsystem Subsystem, config *SubsystemConfig) {
	sq := make(chan *bus.SQE[t_aio.Submission, t_aio.Completion], config.Size)
	workers := make([]*workerWrapper, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &workerWrapper{
			Worker:    subsystem.NewWorker(i),
			sq:        sq,
			cq:        a.cq,
			flushCh:   make(chan int64, 1),
			batchSize: config.BatchSize,
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
	defer close(a.cq)

	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Stop(); err != nil {
			return err
		}

		close(subsystem.sq)
	}

	return nil
}

func (a *aio) Shutdown() {}

func (a *aio) Errors() <-chan error {
	return a.errors
}

func (a *aio) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	if subsystem, ok := a.subsystems[sqe.Submission.Kind]; ok {
		select {
		case subsystem.sq <- sqe:
			slog.Debug("aio:enqueue", "sqe", sqe)
			a.metrics.AioInFlight.WithLabelValues(sqe.Metadata.Tags.Split("aio")...).Inc()
		default:
			sqe.Callback(nil, t_api.NewResonateError(t_api.ErrAIOSubmissionQueueFull, fmt.Sprintf("aio:subsytem:%s submission queue full", subsystem), nil))
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

			tags := cqe.Metadata.Tags.Split("aio")
			a.metrics.AioTotal.WithLabelValues(append(tags, status)...).Inc()
			a.metrics.AioInFlight.WithLabelValues(tags...).Dec()

			slog.Debug("aio:dequeue", "cqe", cqe)
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

	for i := 0; i < w.batchSize; i++ {
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
