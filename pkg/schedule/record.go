package schedule

type ScheduleRecord struct {
	Id          string
	SortId      int64
	Interval    string
	LastRunTime *int64
	CreatedOn   *int64
}

func (r *ScheduleRecord) Schedule() (*Schedule, error) {
	return &Schedule{
		Id:          r.Id,
		SortId:      r.SortId,
		Interval:    r.Interval,
		LastRunTime: r.LastRunTime,
		CreatedOn:   r.CreatedOn,
	}, nil
}
