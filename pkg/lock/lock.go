package lock

import "fmt"

type Lock struct {
	ResourceId  string `json:"resourceId"`
	ProcessId   string `json:"processId"`
	ExecutionId string `json:"executionId"`
	Timeout     int64  `json:"timeout"`
}

func (l *Lock) String() string {
	return fmt.Sprintf(
		"Lock(resourceId=%s, processId=%s, executionId=%s, timeout=%d)",
		l.ResourceId,
		l.ProcessId,
		l.ExecutionId,
		l.Timeout,
	)
}
