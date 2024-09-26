package schedule

import (
	"fmt"

	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Schedule struct {
	Id             string            `json:"id"`
	Description    string            `json:"desc,omitempty"`
	Cron           string            `json:"cron"`
	Tags           map[string]string `json:"tags"`
	PromiseId      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
	LastRunTime    *int64            `json:"lastRunTime,omitempty"`
	NextRunTime    int64             `json:"nextRunTime"`
	IdempotencyKey *idempotency.Key  `json:"idempotencyKey,omitempty"`
	CreatedOn      int64             `json:"createdOn"`
	SortId         int64             `json:"-"` // unexported
}

func (s *Schedule) String() string {
	return fmt.Sprintf(
		"Schedule(id=%s, desc=%s, cron=%s, tags=%s, promiseId=%s, promiseTimeout=%d, promiseParam=%s, promiseTags=%s, lastRunTime=%d, nextRunTime=%d, idempotencyKey=%s, createdOn=%d)",
		s.Id,
		s.Description,
		s.Cron,
		s.Tags,
		s.PromiseId,
		s.PromiseTimeout,
		s.PromiseParam,
		s.PromiseTags,
		util.SafeDeref(s.LastRunTime),
		s.NextRunTime,
		s.IdempotencyKey,
		s.CreatedOn,
	)
}

func (s1 *Schedule) Equals(s2 *Schedule) bool {
	// for dst only
	return s1.Id == s2.Id && s1.IdempotencyKey.Equals(s2.IdempotencyKey)
}
