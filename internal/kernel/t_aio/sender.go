package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

type SenderSubmission struct {
	Task          *task.Task
	Promise       *promise.Promise
	ClaimHref     string
	CompleteHref  string
	HeartbeatHref string
}

func (s *SenderSubmission) String() string {
	return fmt.Sprintf("Sender(task=%s)", s.Task)
}

type SenderCompletion struct {
	Success bool
}

func (c *SenderCompletion) String() string {
	return fmt.Sprintf("Sender(success=%t)", c.Success)
}
