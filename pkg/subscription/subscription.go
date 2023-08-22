package subscription

import "fmt"

type Subscription struct {
	PromiseId   string       `json:"promiseId"`
	Id          int64        `json:"id"`
	Url         string       `json:"url"`
	RetryPolicy *RetryPolicy `json:"retryPolicy"`
}

type RetryPolicy struct {
	Delay    int64 `json:"delay"`
	Attempts int64 `json:"attempts"`
}

func (s *Subscription) String() string {
	return fmt.Sprintf(
		"Subscription(promiseId=%s, id=%d, url=%s, retryPolicy=%s)",
		s.PromiseId,
		s.Id,
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
