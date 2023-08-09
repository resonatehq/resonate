package aio

import (
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type aioDST struct {
	sqes       []*bus.SQE[types.Submission, types.Completion]
	cqes       []*bus.CQE[types.Submission, types.Completion]
	subsystems map[types.AIOKind]Subsystem
	done       bool
}

func NewDST() *aioDST {
	return &aioDST{
		subsystems: map[types.AIOKind]Subsystem{},
	}
}

func (a *aioDST) AddSubsystem(kind types.AIOKind, subsystem Subsystem) {
	a.subsystems[kind] = subsystem
}

func (a *aioDST) Start() error {
	for _, subsystem := range a.subsystems {
		if err := subsystem.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (a *aioDST) Stop() error {
	for _, subsystem := range a.subsystems {
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
	a.sqes = append(a.sqes, sqe)
}

func (a *aioDST) Dequeue(n int) []*bus.CQE[types.Submission, types.Completion] {
	cqes := a.cqes[:min(n, len(a.cqes))]
	a.cqes = a.cqes[min(n+1, len(a.cqes)):]

	return cqes
}

func (a *aioDST) Flush(t int64) {
	sqes := map[types.AIOKind][]*bus.SQE[types.Submission, types.Completion]{}
	for _, sqe := range a.sqes {
		sqes[sqe.Submission.Kind] = append(sqes[sqe.Submission.Kind], sqe)
	}

	for kind, sqes := range sqes {
		if subsystem, ok := a.subsystems[kind]; ok {
			a.cqes = append(a.cqes, subsystem.NewWorker(0).Process(sqes)...)
		} else {
			panic("invalid aio submission")
		}
	}

	a.sqes = nil
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}
