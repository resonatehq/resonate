package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
)

type RouterSubmission struct {
	Promise *promise.Promise
}

func (s *RouterSubmission) String() string {
	return fmt.Sprintf("Router(promise=%s)", s.Promise)
}

type RouterCompletion struct {
	Matched bool
	Command *CreateTaskCommand
}

func (c *RouterCompletion) String() string {
	return fmt.Sprintf("Router(match=%t)", c.Matched)
}
