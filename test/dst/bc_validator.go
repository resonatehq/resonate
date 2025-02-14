package dst

import (
	"fmt"
	"math/rand"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

type BcValidator struct {
	validators []BcValidatorFn
}

type BcValidatorFn func(*Model, int64, int64, *Req) (*Model, error)

func NewBcValidator(r *rand.Rand, config *Config) *BcValidator {
	return &BcValidator{
		validators: []BcValidatorFn{},
	}
}

func (v *BcValidator) AddBcValidator(bcv BcValidatorFn) {
	v.validators = append(v.validators, bcv)
}

func (v *BcValidator) Validate(model *Model, reqTime int64, resTime int64, req *Req) (*Model, error) {
	var err error
	for _, bcv := range v.validators {
		model, err = bcv(model, reqTime, resTime, req)
		if err != nil {
			return model, err
		}
	}

	return model, nil
}

func ValidateNotify(model *Model, reqTime int64, resTime int64, req *Req) (*Model, error) {
	if req.bc.Promise != nil {
		storedP := model.promises.get(req.bc.Promise.Id)
		p := req.bc.Promise
		if storedP.State == promise.Pending {
			// the only way this can happen is if the promise timedout
			if p.State == promise.GetTimedoutState(storedP) && resTime >= storedP.Timeout {
				model = model.Copy()
				model.promises.set(p.Id, p)
				return model, nil
			}
			return model, fmt.Errorf("received a notification for promise '%s' but promise was not completed", req.bc.Promise.Id)
		}
	}

	return model, nil
}

func ValidateTasksWithSameRootPromiseId(model *Model, _ int64, _ int64, req *Req) (*Model, error) {
	if req.bc.Task != nil {
		for _, stored := range *model.tasks {
			if stored.value.Id == req.bc.Task.Id &&
				(stored.value.Counter < req.bc.Task.Counter ||
					stored.value.Attempt < req.bc.Task.Attempt) {
				continue
			}

			if stored.value.RootPromiseId == req.bc.Task.RootPromiseId &&
				stored.value.State != task.Completed &&
				stored.value.ExpiresAt > req.time &&
				stored.value.Timeout > req.time {
				return model, fmt.Errorf("task '%s' for root promise  '%s' should not have been enqueued", req.bc.Task.Id, req.bc.Task.RootPromiseId)
			}
		}
		reqT := req.bc.Task
		stored := model.tasks.get(reqT.Id)
		if stored == nil || stored.State != task.Completed {
			// At this point we have verified that it was fine that this
			// task was r-eenqueued. Is possible that a task was enqueued
			// and then completed by completing the root promise, if that is the
			// case just ignore that tasks
			newT := &task.Task{
				Id:            reqT.Id,
				Counter:       reqT.Counter,
				Timeout:       reqT.Timeout,
				ProcessId:     reqT.ProcessId,
				State:         reqT.State,
				RootPromiseId: reqT.RootPromiseId,
				Recv:          reqT.Recv,
				Mesg:          reqT.Mesg,
				Attempt:       reqT.Attempt,
				Ttl:           reqT.Ttl,
				ExpiresAt:     reqT.ExpiresAt,
				CreatedOn:     reqT.CreatedOn,
				CompletedOn:   reqT.CompletedOn,
			}
			model = model.Copy()
			model.tasks.set(newT.Id, newT)
		}
	}

	return model, nil
}
