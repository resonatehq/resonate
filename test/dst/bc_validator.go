package dst

import (
	"fmt"
	"math/rand"

	"github.com/resonatehq/resonate/internal/util"
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

		state := reqT.State
		completedOn := reqT.CompletedOn
		if p != nil &&
			p.State != promise.Pending {
			if reqT.Mesg.Type == message.Invoke && *p.CompletedOn <= *reqT.CreatedOn {
				return model, fmt.Errorf("invocation for a promise that is alredy completed")
			} else if *p.CompletedOn > *reqT.CreatedOn {
				state = task.Completed
				completedOn = util.ToPointer(*p.CompletedOn)
			}
		}

		for _, t := range *model.tasks {
			if t.value.RootPromiseId != reqT.RootPromiseId ||
				t.value.Mesg.Type != reqT.Mesg.Type ||
				t.value.Id == reqT.Id {
				continue
			}
			if !t.value.State.In(task.Completed|task.Timedout) &&
				t.value.Timeout > reqTime {
				return model, fmt.Errorf("multiple tasks with same rootpromiseId '%s' active at the same time", req.bc.Task.RootPromiseId)
			}
		}

		newT := &task.Task{
			Id:            reqT.Id,
			Counter:       reqT.Counter,
			Timeout:       reqT.Timeout,
			ProcessId:     reqT.ProcessId,
			State:         state,
			RootPromiseId: reqT.RootPromiseId,
			Recv:          reqT.Recv,
			Mesg:          reqT.Mesg,
			Attempt:       reqT.Attempt,
			Ttl:           reqT.Ttl,
			ExpiresAt:     reqT.ExpiresAt,
			CreatedOn:     reqT.CreatedOn,
			CompletedOn:   completedOn,
		}
		model = model.Copy()
		model.tasks.set(newT.Id, newT)
	}

	return model, nil
}
