package scheduler

import "github.com/resonatehq/resonate/internal/kernel/types"

type Coroutine struct {
	name         string
	init         func(*Scheduler, *Coroutine)
	onDone       []func()
	submission   *types.Submission
	continuation func(*types.Completion, error)
}

func NewCoroutine(name string, init func(*Scheduler, *Coroutine)) *Coroutine {
	return &Coroutine{
		name: name,
		init: init,
	}
}

func (c *Coroutine) String() string {
	return c.name
}

func (c *Coroutine) Yield(submission *types.Submission, continuation func(*types.Completion, error)) {
	c.submission = submission
	c.continuation = continuation
}

func (c *Coroutine) OnDone(f func()) {
	c.onDone = append(c.onDone, f)
}

func (c *Coroutine) resume(completion *types.Completion, err error) {
	continuation := c.continuation
	c.continuation = nil

	if continuation != nil {
		continuation(completion, err)
	}
}

func (c *Coroutine) next() *types.Submission {
	submission := c.submission
	c.submission = nil

	if c.done() {
		for _, f := range c.onDone {
			f()
		}
	}

	return submission
}

func (c *Coroutine) done() bool {
	return c.continuation == nil
}
