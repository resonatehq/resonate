package subscription

import "encoding/json"

type SubscriptionRecord struct {
	PromiseId   string
	Id          int64
	Url         string
	RetryPolicy []byte
}

func (r *SubscriptionRecord) Subscription() (*Subscription, error) {
	var retryPolicy *RetryPolicy
	if err := json.Unmarshal(r.RetryPolicy, &retryPolicy); err != nil {
		return nil, err
	}

	return &Subscription{
		Id:          r.Id,
		Url:         r.Url,
		RetryPolicy: retryPolicy,
	}, nil
}
