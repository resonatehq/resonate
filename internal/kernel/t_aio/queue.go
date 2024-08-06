package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/task"
)

type QueueSubmission struct {
	Task *task.Task
}

func (s *QueueSubmission) String() string {
	return fmt.Sprintf("Queue(task=%s)", s.Task)
}

type QueueCompletion struct {
	Success bool
}

func (c *QueueCompletion) String() string {
	return fmt.Sprintf("Queue(success=%t)", c.Success)
}
