package lock

import "fmt"

type Lock struct {
	ResourceId      string `json:"resourceId"`
	ProcessId       string `json:"processId"`
	ExecutionId     string `json:"executionId"`
	ExpiryInSeconds int64  `json:"expiryInSeconds"`
	ExpiresAt       int64  `json:"expiresAt"`
}

func (l *Lock) String() string {
	return fmt.Sprintf(
		"Lock(resourceId=%s, processId=%s, executionId=%s, expiryInSeconds=%d, expiresAt=%d)",
		l.ResourceId,
		l.ProcessId,
		l.ExecutionId,
		l.ExpiryInSeconds,
		l.ExpiresAt,
	)
}
