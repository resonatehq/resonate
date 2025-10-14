package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

type SenderSubmission struct {
	Task          *task.Task
	Promise       *promise.Promise
	BaseHref      string
	ClaimHref     string
	CompleteHref  string
	HeartbeatHref string
}

func (s *SenderSubmission) String() string {
	return fmt.Sprintf("Sender(task=%s)", s.Task)
}

type SenderCompletion struct {
	Success     bool
	TimeToRetry int64
	TimeToClaim int64
}

func (c *SenderCompletion) String() string {
	return fmt.Sprintf("Sender(success=%t, ttr=%d, ttc=%d)", c.Success, c.TimeToRetry, c.TimeToClaim)
}
