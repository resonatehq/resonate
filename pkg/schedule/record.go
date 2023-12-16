package schedule

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/promise"
)

type ScheduleRecord struct {
	Id                      string
	Desc                    *string
	Cron                    string
	PromiseId               string
	PromiseParamHeaders     []byte
	PromiseParamData        []byte
	PromiseTimeout          int64
	LastRunTime             *int64
	NextRunTime             int64
	CreatedOn               int64
	IdempotencyKeyForCreate *IdempotencyKey
}

func (r *ScheduleRecord) Schedule() (*Schedule, error) {
	param, err := r.param()
	if err != nil {
		return nil, err
	}

	return &Schedule{
		Id:             r.Id,
		Desc:           r.Desc,
		Cron:           r.Cron,
		PromiseId:      r.PromiseId,
		PromiseParam:   param,
		PromiseTimeout: r.PromiseTimeout,
		LastRunTime:    r.LastRunTime,
		NextRunTime:    r.NextRunTime,
		CreatedOn:      r.CreatedOn,
	}, nil
}

func (r *ScheduleRecord) param() (promise.Value, error) {
	var headers map[string]string

	if r.PromiseParamHeaders != nil {
		if err := json.Unmarshal(r.PromiseParamHeaders, &headers); err != nil {
			return promise.Value{}, err
		}
	}

	return promise.Value{
		Headers: headers,
		Data:    r.PromiseParamData,
	}, nil
}
