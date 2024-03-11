package t_aio

import "fmt"

// QueuingSubmission is the request to send a task.
type QueuingSubmission struct {

	// TaskId is the id of the task that is available to be claimed by an external worker process.
	TaskId string `json:"taskid"`

	// Counter is the number of times this task has been attempted. Only last attempt is guaranteed to be successful.
	Counter int `json:"counter"`
}

func (s *QueuingSubmission) String() string {
	return fmt.Sprintf("queuing: %s", s.TaskId)
}

type QueuingCompletionStatus int

const (
	Success QueuingCompletionStatus = iota
	Failure
)

// TODO: not needed for now, but we should include more detailed error information.
// QueuingCompletion is the response from sending a task.
type QueuingCompletion struct {
	Result QueuingCompletionStatus
}

func (c *QueuingCompletion) String() string {
	switch c.Result {
	case Success:
		return "success"
	case Failure:
		return "failure" // todo: include more detailed error.
	default:
		panic("invalid queuing completion")
	}
}
