package schedule

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
)

type Schedule struct {
	Id             string                  `json:"id"`
	Desc           string                  `json:"desc,omitempty"`
	Cron           string                  `json:"cron"`
	PromiseId      string                  `json:"promiseId"`
	PromiseParam   promise.Value           `json:"promiseParam,omitempty"`
	PromiseTimeout int64                   `json:"promiseTimeout"`
	LastRunTime    *int64                  `json:"lastRunTime,omitempty"`
	NextRunTime    int64                   `json:"nextRunTime"`
	CreatedOn      int64                   `json:"createdOn"`
	IdempotencyKey *promise.IdempotencyKey `json:"idempotencyKey,omitempty"`
}

func (s *Schedule) String() string {
	return fmt.Sprintf(
		"Schedule(id=%s, desc=%s, cron=%s, promiseId=%s, promiseParam=%s, promiseTimeout=%d, lastRunTime=%d, nextRunTime=%d, createdOn=%d, idempotencyKey=%s)",
		s.Id,
		s.Desc,
		s.Cron,
		s.PromiseId,
		s.PromiseParam,
		s.PromiseTimeout,
		s.LastRunTime,
		s.NextRunTime,
		s.CreatedOn,
		s.IdempotencyKey,
	)
}
