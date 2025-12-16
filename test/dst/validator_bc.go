package dst

import (
	"fmt"
	"math/rand"

	"github.com/resonatehq/resonate/pkg/message"
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

func ValidateTasksWithSameRootPromiseId(model *Model, reqTime int64, _ int64, req *Req) (*Model, error) {
	if req.bc.Task != nil {
		reqT := req.bc.Task
		stored := model.tasks.get(reqT.Id)
		p := model.promises.get(req.bc.Task.RootPromiseId)

		if stored != nil && stored.RootPromiseId != reqT.RootPromiseId {
			return model, fmt.Errorf("same task with different rootpromiseId: '%s'", req.bc.Task.Id)
		}

		if stored != nil && stored.State.In(task.Completed|task.Timedout) {
			return model, nil
		}

		if stored != nil && stored.State == task.Claimed && stored.ExpiresAt > reqTime {
			return model, nil
		}

		if p != nil && p.State != promise.Pending && reqT.Mesg.Type == message.Invoke && *p.CompletedOn <= *reqT.CreatedOn {
			return model, fmt.Errorf("invoke message recieved for complete promise")
		}

		for _, t := range model.tasks.all() {
			if t.RootPromiseId != reqT.RootPromiseId || t.Mesg.Type != reqT.Mesg.Type || t.Id == reqT.Id {
				continue
			}
			if !t.State.In(task.Completed|task.Timedout) && t.ExpiresAt > reqTime && t.Timeout > reqTime {
				return model, fmt.Errorf("multiple tasks with same rootpromiseId '%s' active at the same time", req.bc.Task.RootPromiseId)
			}
		}

		model = model.Copy()
		model.tasks.set(reqT.Id, reqT)
	}

	return model, nil
}

func ValidateTaskExpiry(model *Model, reqTime int64, _ int64, req *Req) (*Model, error) {
	if req.bc.Task != nil && req.bc.Task.ExpiresAt < 0 {
		return model, fmt.Errorf("invalid task expiry %d", req.bc.Task.ExpiresAt)
	}

	return model, nil
}
