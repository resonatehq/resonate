package notification

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/subscription"
)

type Notification struct {
	Id          int64                     `json:"id"`
	PromiseId   string                    `json:"promiseId"`
	Url         string                    `json:"url"`
	RetryPolicy *subscription.RetryPolicy `json:"retryPolicy"`
	Time        int64                     `json:"time"`
	Attempt     int64                     `json:"attempt"`
}

func (n *Notification) String() string {
	return fmt.Sprintf(
		"Notification(id=%d, url=%s, retryPolicy=%s, time=%d, attempt=%d)",
		n.Id,
		n.Url,
		n.RetryPolicy,
		n.Time,
		n.Attempt,
	)
}
