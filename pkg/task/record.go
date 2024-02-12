package task

type TaskRecord struct {
	Id              string
	Counter         int
	PromiseId       string
	ClaimTimeout    int64
	CompleteTimeout int64
	PromiseTimeout  int64
	CreatedOn       int64
	CompletedOn     int64
	IsCompleted     bool
}

func (r *TaskRecord) Task() (*Task, error) {
	return &Task{
		Id:              r.Id,
		Counter:         r.Counter,
		PromiseId:       r.PromiseId,
		ClaimTimeout:    r.ClaimTimeout,
		CompleteTimeout: r.CompleteTimeout,
		PromiseTimeout:  r.PromiseTimeout,
		CreatedOn:       r.CreatedOn,
		CompletedOn:     r.CompletedOn,
		IsCompleted:     r.IsCompleted,
	}, nil
}
