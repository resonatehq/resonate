package aio

import (
	"fmt"
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/t_aio"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/util"
)

type aioDST struct {
	r          *rand.Rand
	sqes       []*bus.SQE[t_aio.Submission, t_aio.Completion]
	cqes       []*bus.CQE[t_aio.Submission, t_aio.Completion]
	subsystems map[t_aio.Kind]Subsystem
	done       bool
}

func NewDST(r *rand.Rand) *aioDST {
	return &aioDST{
		r:          r,
		subsystems: map[t_aio.Kind]Subsystem{},
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

func (a *aioDST) Shutdown() {
	a.done = true
}

func (a *aioDST) Done() bool {
	return a.done
}

func (a *aioDST) Enqueue(sqe *bus.SQE[t_aio.Submission, t_aio.Completion]) {
	i := a.r.Intn(len(a.sqes) + 1)
	a.sqes = append(a.sqes[:i], append([]*bus.SQE[t_aio.Submission, t_aio.Completion]{sqe}, a.sqes[i:]...)...)
}

func (a *aioDST) Dequeue(n int) []*bus.CQE[t_aio.Submission, t_aio.Completion] {
	cqes := a.cqes[:min(n, len(a.cqes))]
	a.cqes = a.cqes[min(n, len(a.cqes)):]

	return cqes
}

func (a *aioDST) Flush(t int64) {
	flush := map[t_aio.Kind][]*bus.SQE[t_aio.Submission, t_aio.Completion]{}
	for _, sqe := range a.sqes {
		flush[sqe.Submission.Kind] = append(flush[sqe.Submission.Kind], sqe)
	}

	for _, sqes := range util.OrderedRangeKV(flush) {
		if subsystem, ok := a.subsystems[sqes.Key]; ok {
			a.cqes = append(a.cqes, subsystem.NewWorker(0).Process(sqes.Value)...)
		} else {
			panic("invalid aio submission")
		}
	}

	a.sqes = nil
}

func (a *aioDST) String() string {
	// use subsystem keys so that we can compare cross-store dst runs
	subsystems := make([]t_aio.Kind, len(a.subsystems))
	for i, subsystem := range util.OrderedRangeKV[t_aio.Kind](a.subsystems) {
		subsystems[i] = subsystem.Key
	}

	return fmt.Sprintf("AIODST(subsystems=%s)", subsystems)
}
