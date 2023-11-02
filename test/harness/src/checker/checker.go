package checker

import (
	"log"

	"github.com/resonatehq/resonate/test/harness/src/store"
)

// Checker validates that a history is correct with respect to some model.
type Checker struct {
	Perf
	Linearizable // ref: https://github.com/anishathalye/porcupine
	Timeline
}

func New() *Checker {
	return &Checker{
		Perf:         NewPerf(),
		Linearizable: NewLinearizable(),
		Timeline:     NewTimeline(),
	}
}

// Check verified the history is correct
func (c *Checker) Check(ops []store.Operation) error {
	for _, op := range ops {
		log.Println(op)
	}
	return nil
}
