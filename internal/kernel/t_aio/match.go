package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
)

type MatchSubmission struct {
	Promise *promise.Promise
}

func (s *MatchSubmission) String() string {
	return fmt.Sprintf("Match(promise=%s)", s.Promise)
}

type MatchCompletion struct {
	Matched bool
	Command *CreateTaskCommand
}

func (c *MatchCompletion) String() string {
	return fmt.Sprintf("Match(match=%t)", c.Matched)
}
