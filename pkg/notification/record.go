package notification

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/subscription"
)

type NotificationRecord struct {
	Id          int64
	PromiseId   string
	Url         string
	RetryPolicy []byte
	Time        int64
	Attempt     int64
}

func (r *NotificationRecord) Notification() (*Notification, error) {
	var retryPolicy *subscription.RetryPolicy
	if err := json.Unmarshal(r.RetryPolicy, &retryPolicy); err != nil {
		return nil, err
	}

	return &Notification{
		Id:          r.Id,
		PromiseId:   r.PromiseId,
		Url:         r.Url,
		RetryPolicy: retryPolicy,
		Time:        r.Time,
		Attempt:     r.Attempt,
	}, nil
}
