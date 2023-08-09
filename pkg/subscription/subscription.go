package subscription

import "fmt"

type Subscription struct {
	Id  int64
	Url string
}

func (s *Subscription) String() string {
	return fmt.Sprintf(
		"Subscription(id=%d, url=%s)",
		s.Id,
		s.Url,
	)
}
