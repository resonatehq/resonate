package dst

import (
	"fmt"
	"math/rand"

	"github.com/resonatehq/resonate/pkg/task"
)

type BcValidator struct {
	validators []BcValidatorFn
}

type BcValidatorFn func(*Model, *Req) (*Model, error)

func NewBcValidator(r *rand.Rand, config *Config) *BcValidator {
	return &BcValidator{
		validators: []BcValidatorFn{},
	}
}

func (v *BcValidator) AddBcValidator(bcv BcValidatorFn) {
	v.validators = append(v.validators, bcv)
}

func (v *BcValidator) Validate(model *Model, req *Req) (*Model, error) {
	var err error = nil
	for _, bcv := range v.validators {
		model, err = bcv(model, req)
		if err != nil {
			fmt.Printf("Got error %v\n", err)
			return model, err
		}
	}

	return model, nil
}

func ValidateTasksWithSameRootPromiseId(model *Model, req *Req) (*Model, error) {
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
		model = model.Copy()
		model.tasks.set(req.bc.Task.Id, req.bc.Task)
	}

	return model, nil
}
