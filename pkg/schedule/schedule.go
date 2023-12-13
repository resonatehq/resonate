package schedule

import (
	"fmt"
)

type Schedule struct {
	Id          string  `json:"id"`
	Desc        *string `json:"desc,omitempty"`
	Cron        string  `json:"cron"`
	LastRunTime *int64  `json:"lastRunTime,omitempty"`
	NextRunTime int64   `json:"nextRunTime"`
	CreatedOn   *int64  `json:"createdOn,omitempty"`
}

func (s *Schedule) String() string {
	return fmt.Sprintf(
		"Schedule(id=%s, desc=%v, cron=%s, lastRunTime=%d, nextRunTime=%d, createdOn=%d)",
		s.Id,
		s.Desc,
		s.Cron,
		s.LastRunTime,
		s.NextRunTime,
		s.CreatedOn,
	)
}
