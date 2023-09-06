package subscription

import "fmt"

type Subscription struct {
	Id          string       `json:"id"`
	PromiseId   string       `json:"promiseId"`
	Url         string       `json:"url"`
	RetryPolicy *RetryPolicy `json:"retryPolicy"`
	CreatedOn   int64        `json:"createdOn"`
}

type RetryPolicy struct {
	Delay    int64 `json:"delay"`
	Attempts int64 `json:"attempts"`
}

func (s *Subscription) String() string {
	return fmt.Sprintf(
		"Subscription(id=%s, promiseId=%s, url=%s, retryPolicy=%s)",
		s.Id,
		s.PromiseId,
		s.Url,
		s.RetryPolicy,
	)
}

func (r *RetryPolicy) String() string {
	return fmt.Sprintf(
		"RetryPolicy(delay=%d, attempts=%d)",
		r.Delay,
		r.Attempts,
	)
}
