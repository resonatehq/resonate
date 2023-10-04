package scheduler

import (
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
)

type Coroutine struct {
	name         string
	init         func(*Scheduler, *Coroutine)
	onDone       []func()
	submission   *t_aio.Submission
	continuation func(int64, *t_aio.Completion, error)
}

func NewCoroutine(name string, init func(*Scheduler, *Coroutine)) *Coroutine {
	return &Coroutine{
		name: name,
		init: init,
	}
}

func (c *Coroutine) Yield(submission *t_aio.Submission, continuation func(int64, *t_aio.Completion, error)) {
	c.submission = submission
	c.continuation = continuation
}

func (c *Coroutine) OnDone(f func()) {
	c.onDone = append(c.onDone, f)
}

func (c *Coroutine) resume(t int64, completion *t_aio.Completion, err error) {
	continuation := c.continuation
	c.continuation = nil

	if continuation != nil {
		continuation(t, completion, err)
	}
}

func (c *Coroutine) next() *t_aio.Submission {
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
