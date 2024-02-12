package task

import "fmt"

type Task struct {
	Id              string `json:"id"`
	Counter         int    `json:"counter"`
	PromiseId       string `json:"promiseId"`
	ClaimTimeout    int64  `json:"claimTimeout"`
	CompleteTimeout int64  `json:"completeTimeout"`
	PromiseTimeout  int64  `json:"promiseTimeout"`
	CreatedOn       int64  `json:"createdOn"`
	CompletedOn     int64  `json:"completedOn"`
	IsCompleted     bool   `json:"isCompleted"`
}

func (t *Task) String() string {
	return fmt.Sprintf(
		"Task(id=%s, counter=%d, promiseId=%s, claimTimeout=%d, completeTimeout=%d, promiseTimeout=%d, createdOn=%d, completedOn=%d, isCompleted=%t)",
		t.Id,
		t.Counter,
		t.PromiseId,
		t.ClaimTimeout,
		t.CompleteTimeout,
		t.PromiseTimeout,
		t.CreatedOn,
		t.CompletedOn,
		t.IsCompleted,
	)
}
