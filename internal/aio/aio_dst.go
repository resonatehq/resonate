package aio

import (
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/metadata"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/util"
)

type aioDST struct {
	r                  *rand.Rand
	sqes               []*bus.SQE[t_aio.Submission, t_aio.Completion]
	cqes               []*bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems         map[t_aio.Kind]Subsystem
	metrics            *metrics.Metrics
	failureProbability float64
}

func NewDST(r *rand.Rand, metrics *metrics.Metrics, failureProbability float64) *aioDST {
	return &aioDST{
		r:                  r,
		subsystems:         map[t_aio.Kind]Subsystem{},
		metrics:            metrics,
		failureProbability: failureProbability,
	}
}

func (a *aioDST) AddSubsystem(kind t_aio.Kind, subsystem Subsystem) {
	a.subsystems[kind] = subsystem
}

func (a *aioDST) Start() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (a *aioDST) Stop() error {
	for _, subsystem := range util.OrderedRange(a.subsystems) {
		if err := subsystem.Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (a *aioDST) Shutdown() {}

func (a *aioDST) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	slog.Debug("aio:enqueue", "sqe", sqe)
	a.metrics.AioInFlight.WithLabelValues(sqe.Metadata.Tags.Split("aio")...).Inc()

	i := a.r.Intn(len(a.sqes) + 1)
	a.sqes = append(a.sqes[:i], append([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe}, a.sqes[i:]...)...)
}

func (a *aioDST) Dequeue(n int) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := a.cqes[:min(n, len(a.cqes))]
	a.cqes = a.cqes[min(n, len(a.cqes)):]

	for _, cqe := range cqes {
		var status string
		if cqe.Error != nil {
			status = "failure"
		} else {
			status = "success"
		}

		tags := cqe.Metadata.Tags.Split("aio")
		a.metrics.AioTotal.WithLabelValues(append(tags, status)...).Inc()
		a.metrics.AioInFlight.WithLabelValues(tags...).Dec()
	}

	return cqes
}

func (a *aioDST) Flush(t int64) {
	flush := map[t_aio.Kind][]*bus.SQE[t_aio.Submission, t_aio.Completion]{}
	for _, sqe := range a.sqes {
		flush[sqe.Submission.Kind] = append(flush[sqe.Submission.Kind], sqe)
	}

	for _, sqes := range util.OrderedRangeKV(flush) {
		if subsystem, ok := a.subsystems[sqes.Key]; ok {
			// Randomly decide whether to process SQE or return an error
			if a.r.Float64() < a.failureProbability {
				// Create a new CQE
				errorCqe := createErrorCQE("aio dst error occurred while processing SQE", "aio dst failure", simpleCallback)
				errorCqe.Completion = &t_aio.Completion{}

				slog.Debug("aio dst: failure", "err", errorCqe)
				a.cqes = append(a.cqes, errorCqe)
			} else {
				// Do the I/O
				processedCQEs := subsystem.NewWorker(0).Process(sqes.Value)
				// Randomly decide whether to return an error after processing SQE
				if a.r.Float64() < a.failureProbability {
					for _, cqe := range processedCQEs {
						errorCqe := createErrorCQE("aio dst error occurred after processing SQE", "aio dst failure", simpleCallback)
						errorCqe.Completion = &t_aio.Completion{}

						slog.Debug("aio dst: failure", "err", errorCqe, "cqe", cqe)
						a.cqes = append(a.cqes, cqe, errorCqe)
					}
				} else {
					a.cqes = append(a.cqes, processedCQEs...)
				}
			}
		} else {
			panic("invalid aio submission")
		}
	}

	a.sqes = nil
}

func simpleCallback(completion *t_aio.Completion, err error) {
}

func createErrorCQE(errorMsg string, tags string, callback func(*t_aio.Completion, error)) *bus.CQE[t_aio.Submission, t_aio.Completion] {
	errorCqe := &bus.CQE[t_aio.Submission, t_aio.Completion]{
		Error:    fmt.Errorf(errorMsg),
		Metadata: metadata.New("dst error"),
	}
	errorCqe.Metadata.Tags.Set("aio", tags)
	errorCqe.Callback = callback
	return errorCqe
}

func (a *aioDST) String() string {
	// use subsystem keys so that we can compare cross-store dst runs
	subsystems := make([]t_aio.Kind, len(a.subsystems))
	for i, subsystem := range util.OrderedRangeKV[t_aio.Kind](a.subsystems) {
		subsystems[i] = subsystem.Key
	}

	return fmt.Sprintf("AIODST(subsystems=%s)", subsystems)
}
