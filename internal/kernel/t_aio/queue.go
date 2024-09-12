package t_aio

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/task"
)

type QueueSubmission struct {
	Task *task.Task
	Body []byte
}

func (s *QueueSubmission) String() string {
	return fmt.Sprintf("Queue(task=%s, body=%d)", s.Task, len(s.Body))
}

type QueueCompletion struct {
	Success bool
}

func (c *QueueCompletion) String() string {
	return fmt.Sprintf("Queue(success=%t)", c.Success)
}
