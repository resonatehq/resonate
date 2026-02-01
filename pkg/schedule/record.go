package schedule

import (
	"encoding/json"

	"github.com/resonatehq/resonate/pkg/promise"
)

type ScheduleRecord struct {
	Id                  string
	Description         string
	Cron                string
	Tags                []byte
	PromiseId           string
	PromiseTimeout      int64
	PromiseParamHeaders []byte
	PromiseParamData    []byte
	PromiseTags         []byte
	LastRunTime         *int64
	NextRunTime         int64
	CreatedOn           int64
	SortId              int64
}

func (r *ScheduleRecord) Schedule() (*Schedule, error) {
	tags, err := bytesToMap(r.Tags)
	if err != nil {
		return nil, err
	}

	promiseParamHeaders, err := bytesToMap(r.PromiseParamHeaders)
	if err != nil {
		return nil, err
	}

	promiseTags, err := bytesToMap(r.PromiseTags)
	if err != nil {
		return nil, err
	}

	return &Schedule{
		Id:             r.Id,
		Description:    r.Description,
		Cron:           r.Cron,
		Tags:           tags,
		PromiseId:      r.PromiseId,
		PromiseTimeout: r.PromiseTimeout,
		PromiseParam:   promise.Value{Headers: promiseParamHeaders, Data: r.PromiseParamData},
		PromiseTags:    promiseTags,
		LastRunTime:    r.LastRunTime,
		NextRunTime:    r.NextRunTime,
		CreatedOn:      r.CreatedOn,
		SortId:         r.SortId,
	}, nil
}

func bytesToMap(b []byte) (map[string]string, error) {
	m := map[string]string{}

	if b != nil {
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, err
		}
	}

	return m, nil
}
