package lock

import "fmt"

type Lock struct {
	ResourceId  string `json:"resourceId"`
	ExecutionId string `json:"executionId"`
	ProcessId   string `json:"processId"`
	Ttl         int64  `json:"ttl"`
	ExpiresAt   int64  `json:"expiresAt"`
}

func (l *Lock) String() string {
	return fmt.Sprintf(
		"Lock(resourceId=%s, executionId=%s, processId=%s, ttl=%d, expiresAt=%d)",
		l.ResourceId,
		l.ExecutionId,
		l.ProcessId,
		l.Ttl,
		l.ExpiresAt,
	)
}

func (l1 *Lock) Equals(l2 *Lock) bool {
	// for dst only
	return l1.ResourceId == l2.ResourceId &&
		l1.ExecutionId == l2.ExecutionId &&
		l1.ProcessId == l2.ProcessId &&
		l1.ExpiresAt == l2.ExpiresAt
}
