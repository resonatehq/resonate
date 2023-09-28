package subscription

import "encoding/json"

type SubscriptionRecord struct {
	Id          string
	PromiseId   string
	Url         string
	RetryPolicy []byte
	CreatedOn   int64
	SortId      int64
}

func (r *SubscriptionRecord) Subscription() (*Subscription, error) {
	var retryPolicy *RetryPolicy
	if err := json.Unmarshal(r.RetryPolicy, &retryPolicy); err != nil {
		return nil, err
	}

	return &Subscription{
		Id:          r.Id,
		PromiseId:   r.PromiseId,
		Url:         r.Url,
		RetryPolicy: retryPolicy,
		CreatedOn:   r.CreatedOn,
		SortId:      r.SortId,
	}, nil
}
