package aio

import (
	"fmt"
	"math/rand" // nosemgrep

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
)

type aioDST struct {
	r          *rand.Rand
	sqes       []*bus.SQE[types.Submission, types.Completion]
	cqes       []*bus.CQE[types.Submission, types.Completion]
	subsystems map[types.AIOKind]Subsystem
	done       bool
}

func NewDST(r *rand.Rand) *aioDST {
	return &aioDST{
		r:          r,
		subsystems: map[types.AIOKind]Subsystem{},
	}
}

func (a *aioDST) AddSubsystem(kind types.AIOKind, subsystem Subsystem) {
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

func (a *aioDST) Enqueue(sqe *bus.SQE[types.Submission, types.Completion]) {
	i := a.r.Intn(len(a.sqes) + 1)
	a.sqes = append(a.sqes[:i], append([]*bus.SQE[types.Submission, types.Completion]{sqe}, a.sqes[i:]...)...)
}

func (a *aioDST) Dequeue(n int) []*bus.CQE[types.Submission, types.Completion] {
	cqes := a.cqes[:min(n, len(a.cqes))]
	a.cqes = a.cqes[min(n, len(a.cqes)):]

	return cqes
}

func (a *aioDST) Flush(t int64) {
	flush := map[types.AIOKind][]*bus.SQE[types.Submission, types.Completion]{}
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
	subsystems := make([]types.AIOKind, len(a.subsystems))
	for i, subsystem := range util.OrderedRangeKV[types.AIOKind](a.subsystems) {
		subsystems[i] = subsystem.Key
	}

	return fmt.Sprintf("AIODST(subsystems=%s)", subsystems)
}
