package schedule

import (
	"fmt"
)

type Schedule struct {
	Id           string  `json:"id"`
	Desc         *string `json:"desc,omitempty"`
	Cron         string  `json:"cron"`
	PromiseId    string  `json:"promiseId"`
	PromiseParam *string `json:"promiseParam,omitempty"`
	LastRunTime  *int64  `json:"lastRunTime,omitempty"`
	NextRunTime  int64   `json:"nextRunTime"`
	CreatedOn    *int64  `json:"createdOn,omitempty"`
}

func (s *Schedule) String() string {
	return fmt.Sprintf(
		"Schedule(id=%s, desc=%v, cron=%s, promiseId=%s, promiseParam=%v, lastRunTime=%d, nextRunTime=%d, createdOn=%d)",
		s.Id,
		s.Desc,
		s.Cron,
		s.PromiseId,
		s.PromiseParam,
		s.LastRunTime,
		s.NextRunTime,
		s.CreatedOn,
	)
}
