package schedule

type ScheduleRecord struct {
	Id                      string
	Desc                    *string
	Cron                    string
	PromiseId               string
	PromiseParam            *string
	LastRunTime             *int64
	NextRunTime             int64 // no nill alway has a next run time, even at creation
	CreatedOn               int64
	IdempotencyKeyForCreate *IdempotencyKey
}

func (r *ScheduleRecord) Schedule() (*Schedule, error) {
	return &Schedule{
		Id:           r.Id,
		Desc:         r.Desc,
		Cron:         r.Cron,
		PromiseId:    r.PromiseId,
		PromiseParam: r.PromiseParam,
		LastRunTime:  r.LastRunTime,
		NextRunTime:  r.NextRunTime,
		CreatedOn:    r.CreatedOn,
	}, nil
}
