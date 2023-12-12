package schedule

import "fmt"

type Schedule struct {
	Id          string `json:"id"`
	SortId      int64  `json:"-"` // unexported
	Interval    string `json:"interval"`
	LastRunTime *int64 `json:"lastRunTime,omitempty"`
	CreatedOn   *int64 `json:"createdOn,omitempty"`
}

func (s *Schedule) String() string {
	return fmt.Sprintf(
		"Schedule(id=%s, interval=%s, lastRunTime=%d, createdOn=%d)",
		s.Id,
		s.Interval,
		s.LastRunTime,
		s.CreatedOn,
	)
}
