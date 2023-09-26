package scheduler

import "github.com/resonatehq/resonate/internal/kernel/types"

type Coroutine struct {
	name         string
	kind         string
	init         func(*Scheduler, *Coroutine)
	onDone       []func()
	submission   *types.Submission
	continuation func(int64, *types.Completion, error)
}

func NewCoroutine(name string, kind string, init func(*Scheduler, *Coroutine)) *Coroutine {
	return &Coroutine{
		name: name,
		kind: kind,
		init: init,
	}
}

func (c *Coroutine) String() string {
	return c.name
}

func (c *Coroutine) Kind() string {
	return c.kind
}

func (c *Coroutine) Yield(submission *types.Submission, continuation func(int64, *types.Completion, error)) {
	c.submission = submission
	c.continuation = continuation
}

func (c *Coroutine) OnDone(f func()) {
	c.onDone = append(c.onDone, f)
}

func (c *Coroutine) resume(t int64, completion *types.Completion, err error) {
	continuation := c.continuation
	c.continuation = nil

	if continuation != nil {
		continuation(t, completion, err)
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
