package subscription

import "encoding/json"

type SubscriptionRecord struct {
	PromiseId   string
	Id          int64
	Url         string
	RetryPolicy []byte
	CreatedOn   int64
}

func (r *SubscriptionRecord) Subscription() (*Subscription, error) {
	var retryPolicy *RetryPolicy
	if err := json.Unmarshal(r.RetryPolicy, &retryPolicy); err != nil {
		return nil, err
	}

	return &Subscription{
		PromiseId:   r.PromiseId,
		Id:          r.Id,
		Url:         r.Url,
		RetryPolicy: retryPolicy,
		CreatedOn:   r.CreatedOn,
	}, nil
}
