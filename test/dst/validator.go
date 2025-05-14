package dst

import (
	"fmt"
	"math/rand"
	"regexp"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/task"
)

type Validator struct {
	regexes    map[string]*regexp.Regexp
	validators map[t_api.Kind]ResponseValidator
}

type ResponseValidator func(*Model, int64, int64, *t_api.Request, *t_api.Response) (*Model, error)

func NewValidator(r *rand.Rand, config *Config) *Validator {
	regexes := map[string]*regexp.Regexp{}
	for i := range 10 {
		regexes[fmt.Sprintf("p*%d", i)] = regexp.MustCompile(fmt.Sprintf("^p.*%d$", i))
		regexes[fmt.Sprintf("p%d*", i)] = regexp.MustCompile(fmt.Sprintf("^p%d.*$", i))
		regexes[fmt.Sprintf("s*%d", i)] = regexp.MustCompile(fmt.Sprintf("^s.*%d$", i))
		regexes[fmt.Sprintf("s%d*", i)] = regexp.MustCompile(fmt.Sprintf("^s%d.*$", i))
	}

	return &Validator{
		regexes:    regexes,
		validators: map[t_api.Kind]ResponseValidator{},
	}
}

func (v *Validator) AddValidator(kind t_api.Kind, validator ResponseValidator) {
	v.validators[kind] = validator
}

func (v *Validator) Validate(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	f, ok := v.validators[res.Kind]
	if !ok {
		return model, fmt.Errorf("unexpected response kind '%s'", res.Kind)
	}

	return f(model, reqTime, resTime, req, res)
}

// PROMISES

func (v *Validator) ValidateReadPromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	p := model.promises.get(req.ReadPromise.Id)

	switch res.ReadPromise.Status {
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.ReadPromise.Id)
		}
		if res.ReadPromise.Promise.State == promise.Pending && reqTime > p.Timeout {
			return model, fmt.Errorf("promise '%s' should be timedout", p.Id)
		}
		if p.State != res.ReadPromise.Promise.State {
			// the only way this can happen is if the promise timedout
			if res.ReadPromise.Promise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(req.ReadPromise.Id, res.ReadPromise.Promise)
				completeRelatedTasks(model, p.Id, reqTime)
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, res.ReadPromise.Promise.State, req.ReadPromise.Id)
			}
		}
		return model, nil
	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", req.ReadPromise.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.ReadPromise.Status)
	}
}

func (v *Validator) ValidateSearchPromises(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	switch res.SearchPromises.Status {
	case t_api.StatusOK:
		regex := v.regexes[req.SearchPromises.Id]

		for _, p := range res.SearchPromises.Promises {
			// ignore scheduled promises
			if _, ok := p.Tags["resonate:schedule"]; ok {
				continue
			}

			if model.promises.get(p.Id) == nil {
				return model, fmt.Errorf("promise '%s' does not exist", p.Id)
			}

			if p.State == promise.Pending && reqTime > p.Timeout {
				return model, fmt.Errorf("promise '%s' should be timedout", p.Id)
			}
			if !regex.MatchString(p.Id) {
				return model, fmt.Errorf("promise id '%s' does not match search query '%s'", p.Id, req.SearchPromises.Id)
			}
			states := map[promise.State]bool{}
			for _, state := range req.SearchPromises.States {
				states[state] = true
			}
			if _, ok := states[p.State]; !ok {
				return model, fmt.Errorf("promise state '%s' not in search states '%s'", p.State, req.SearchPromises.States)
			}
			if req.SearchPromises.SortId != nil && *req.SearchPromises.SortId <= p.SortId {
				return model, fmt.Errorf("unexpected sortId, promise sortId %d is greater than the request sortId %d", p.SortId, *req.SearchPromises.SortId)
			}
			for k, v := range req.SearchPromises.Tags {
				if _v, ok := p.Tags[k]; !ok || v != _v {
					return model, fmt.Errorf("promise %s has unexpected tag '%s:%s', expected '%s:%s'", p.Id, k, _v, k, v)
				}
			}
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.SearchPromises.Status)
	}
}

func (v *Validator) ValidateCreatePromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	return v.validateCreatePromise(model, reqTime, resTime, req.CreatePromise, res.CreatePromise)
}

func (v *Validator) ValidateCreatePromiseAndTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	switch res.CreatePromiseAndTask.Status {
	case t_api.StatusCreated:
		if model.tasks.get(res.CreatePromiseAndTask.Task.Id) != nil {
			return model, fmt.Errorf("task '%s' exists", res.CreatePromiseAndTask.Task.Id)
		}

		model = model.Copy()
		model.tasks.set(res.CreatePromiseAndTask.Task.Id, res.CreatePromiseAndTask.Task)
	}

	promiseRes := &t_api.CreatePromiseResponse{
		Status:  res.CreatePromiseAndTask.Status,
		Promise: res.CreatePromiseAndTask.Promise,
	}

	return v.validateCreatePromise(model, reqTime, resTime, req.CreatePromiseAndTask.Promise, promiseRes)
}

func (v *Validator) validateCreatePromise(model *Model, reqTime int64, resTime int64, req *t_api.CreatePromiseRequest, res *t_api.CreatePromiseResponse) (*Model, error) {
	p := model.promises.get(req.Id)

	switch res.Status {
	case t_api.StatusCreated:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", req.Id)
		}
		if res.Promise.State != promise.Pending {
			return model, fmt.Errorf("invalid state transition (INIT -> %s) for promise '%s'", res.Promise.State, req.Id)
		}

		// update model state
		model = model.Copy()
		model.promises.set(req.Id, res.Promise)

		return model, nil
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.Id)
		}
		if p.State != res.Promise.State {
			// the only way this can happen is if the promise timedout
			if res.Promise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(req.Id, res.Promise)
				completeRelatedTasks(model, p.Id, reqTime)
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, res.Promise.State, req.Id)
			}
		}
		if !p.IdempotencyKeyForCreate.Match(res.Promise.IdempotencyKeyForCreate) {
			return model, fmt.Errorf("ikey mismatch (%s, %s) for promise '%s'", p.IdempotencyKeyForCreate, res.Promise.IdempotencyKeyForCreate, req.Id)
		} else if req.Strict && p.State != promise.Pending {
			return model, fmt.Errorf("unexpected prior state '%s' when strict true for promise '%s'", p.State, req.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyExists:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateCompletePromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	p := model.promises.get(req.CompletePromise.Id)

	switch res.CompletePromise.Status {
	case t_api.StatusCreated:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CompletePromise.Id)
		}
		if p.State != promise.Pending || res.CompletePromise.Promise.State == promise.Pending || res.CompletePromise.Promise.State == promise.Timedout {
			return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, res.CompletePromise.Promise.State, req.CompletePromise.Id)
		}
		if req.CompletePromise.State == promise.Resolved && res.CompletePromise.Promise.State != promise.Resolved {
			return model, fmt.Errorf("unexpected state '%s' after resolve for promise '%s'", res.CompletePromise.Promise.State, req.CompletePromise.Id)
		}
		if req.CompletePromise.State == promise.Rejected && res.CompletePromise.Promise.State != promise.Rejected {
			return model, fmt.Errorf("unexpected state '%s' after reject for promise '%s'", res.CompletePromise.Promise.State, req.CompletePromise.Id)
		}
		if req.CompletePromise.State == promise.Canceled && res.CompletePromise.Promise.State != promise.Canceled {
			return model, fmt.Errorf("unexpected state '%s' after cancel for promise '%s'", res.CompletePromise.Promise.State, req.CompletePromise.Id)
		}

		// update model state
		model = model.Copy()
		model.promises.set(req.CompletePromise.Id, res.CompletePromise.Promise)
		completeRelatedTasks(model, p.Id, reqTime)
		return model, nil
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CompletePromise.Id)
		}
		if res.CompletePromise.Promise.State == promise.Pending {
			return model, fmt.Errorf("unexpected state '%s' after complete for promise '%s'", res.CompletePromise.Promise.State, req.CompletePromise.Id)
		}
		if p.State != res.CompletePromise.Promise.State {
			// the only way this can happen is if the promise timedout
			if res.CompletePromise.Promise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(req.CompletePromise.Id, res.CompletePromise.Promise)
				completeRelatedTasks(model, p.Id, reqTime)
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, res.CompletePromise.Promise.State, req.CompletePromise.Id)
			}
		}
		if !p.IdempotencyKeyForComplete.Match(res.CompletePromise.Promise.IdempotencyKeyForComplete) && (req.CompletePromise.Strict || res.CompletePromise.Promise.State != promise.Timedout) {
			return model, fmt.Errorf("ikey mismatch (%s, %s) for promise '%s'", p.IdempotencyKeyForCreate, res.CompletePromise.Promise.IdempotencyKeyForCreate, req.CompletePromise.Id)
		} else if req.CompletePromise.Strict && p.State != req.CompletePromise.State {
			return model, fmt.Errorf("unexpected prior state '%s' when strict true for promise '%s'", p.State, req.CompletePromise.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyResolved:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CompletePromise.Id)
		}
		if p.State != promise.Resolved && (p.State != promise.Pending || p.Tags["resonate:timeout"] != "true" || resTime < p.Timeout) {
			return model, fmt.Errorf("promise '%s' not resolved", req.CompletePromise.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyRejected:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CompletePromise.Id)
		}
		if p.State != promise.Rejected {
			return model, fmt.Errorf("promise '%s' not rejected", req.CompletePromise.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyCanceled:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CompletePromise.Id)
		}
		if p.State != promise.Canceled {
			return model, fmt.Errorf("promise '%s' not canceled", req.CompletePromise.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyTimedout:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CompletePromise.Id)
		}
		if p.State != promise.Timedout && (p.State != promise.Pending || p.Tags["resonate:timeout"] == "true" || resTime < p.Timeout) {
			return model, fmt.Errorf("promise '%s' not timedout", req.CompletePromise.Id)
		}
		return model, nil
	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", req.CompletePromise.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.CompletePromise.Status)
	}
}

// CALLBACKS

func (v *Validator) ValidateCreateCallback(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	p := model.promises.get(req.CreateCallback.PromiseId)

	switch res.CreateCallback.Status {
	case t_api.StatusCreated:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CreateCallback.PromiseId)
		}

		if p.State != promise.Pending {
			return model, fmt.Errorf("promise '%s' must be pending", req.CreateCallback.PromiseId)
		}

		if model.callbacks.get(res.CreateCallback.Callback.Id) != nil {
			return model, fmt.Errorf("callback '%s' exists", res.CreateCallback.Callback.Id)
		}

		model = model.Copy()
		model.callbacks.set(res.CreateCallback.Callback.Id, res.CreateCallback.Callback)
		return model, nil

	case t_api.StatusOK:
		// If the status is Ok there has to be a promise
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CreateCallback.PromiseId)
		}

		// If the promise is completed we don't create a callback but return 200
		if p.State != promise.Pending {
			return model, nil
		}

		// If the promise is pending is possible that it has been timeout so we update the promises model
		// if the promise is timedout we can handle it as if it was completed
		if resTime >= p.Timeout {
			model = model.Copy()
			model.promises.set(p.Id, res.CreateCallback.Promise)
			completeRelatedTasks(model, p.Id, reqTime)
			return model, nil
		}

		// otherwise verify the callback was created previously
		if model.callbacks.get(req.CreateCallback.Id) == nil {
			return model, fmt.Errorf("callback '%s' must exist", req.CreateCallback.Id)
		}

		return model, nil

	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", req.CreateCallback.PromiseId)
		}
		return model, nil

	case t_api.StatusCallbackInvalidPromise:
		if req.CreateCallback.PromiseId != req.CreateCallback.Mesg.Root {
			return model, fmt.Errorf("PromiseId and RootPromiseId are not equal (promiseId=%s, rootPromiseId=%s)", req.CreateCallback.PromiseId, req.CreateCallback.Mesg.Root)
		}
		return model, nil

	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.CreateCallback.Status)
	}
}

// SCHEDULES

func (v *Validator) ValidateReadSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	s := model.schedules.get(req.ReadSchedule.Id)

	switch res.ReadSchedule.Status {
	case t_api.StatusOK:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", req.ReadSchedule.Id)
		}
		return model, nil
	case t_api.StatusScheduleNotFound:
		if s != nil {
			return model, fmt.Errorf("schedule '%s' exists", req.ReadSchedule.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.ReadSchedule.Status)
	}
}

func (v *Validator) ValidateSearchSchedules(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	switch res.SearchSchedules.Status {
	case t_api.StatusOK:
		regex := v.regexes[req.SearchSchedules.Id]

		for _, s := range res.SearchSchedules.Schedules {
			if model.schedules.get(s.Id) == nil {
				return model, fmt.Errorf("schedule '%s' does not exist", s.Id)
			}
			if !regex.MatchString(s.Id) {
				return model, fmt.Errorf("schedule id '%s' does not match search query '%s'", s.Id, req.SearchSchedules.Id)
			}
			if req.SearchSchedules.SortId != nil && *req.SearchSchedules.SortId <= s.SortId {
				return model, fmt.Errorf("unexpected sortId, schedule sortId %d is greater than the request sortId %d", s.SortId, *req.SearchSchedules.SortId)
			}
			for k, v := range req.SearchSchedules.Tags {
				if _v, ok := s.Tags[k]; !ok || v != _v {
					return model, fmt.Errorf("schedule %s has unexpected tag '%s:%s', expected '%s:%s'", s.Id, k, _v, k, v)
				}
			}
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.SearchPromises.Status)
	}
}

func (v *Validator) ValidateCreateSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	s := model.schedules.get(req.CreateSchedule.Id)

	switch res.CreateSchedule.Status {
	case t_api.StatusCreated:
		if s != nil {
			return model, fmt.Errorf("schedule '%s' exists", req.CreateSchedule.Id)
		}

		model = model.Copy()
		model.schedules.set(req.CreateSchedule.Id, res.CreateSchedule.Schedule)
		return model, nil
	case t_api.StatusOK:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", req.CreateSchedule.Id)
		}
		if !s.IdempotencyKey.Match(res.CreateSchedule.Schedule.IdempotencyKey) {
			return model, fmt.Errorf("ikey mismatch (%s, %s) for schedule '%s'", s.IdempotencyKey, res.CreateSchedule.Schedule.IdempotencyKey, req.CreateSchedule.IdempotencyKey)
		}
		return model, nil
	case t_api.StatusScheduleAlreadyExists:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", req.CreateSchedule.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.CreateSchedule.Status)
	}
}

func (v *Validator) ValidateDeleteSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	s := model.schedules.get(req.DeleteSchedule.Id)

	switch res.DeleteSchedule.Status {
	case t_api.StatusNoContent:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", req.DeleteSchedule.Id)
		}

		model = model.Copy()
		model.schedules.delete(req.DeleteSchedule.Id)
		return model, nil
	case t_api.StatusScheduleNotFound:
		if s != nil {
			return model, fmt.Errorf("schedule '%s' exists", req.DeleteSchedule.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.DeleteSchedule.Status)
	}
}

// LOCKS

func (v *Validator) ValidateAcquireLock(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	l := model.locks.get(req.AcquireLock.ResourceId)

	switch res.AcquireLock.Status {
	case t_api.StatusCreated:
		if l != nil && l.ExecutionId != req.AcquireLock.ExecutionId && l.ExpiresAt >= resTime {
			return model, fmt.Errorf("lock '%s' exists", req.AcquireLock.ResourceId)
		}

		model = model.Copy()
		model.locks.set(req.AcquireLock.ResourceId, res.AcquireLock.Lock)
		return model, nil
	case t_api.StatusLockAlreadyAcquired:
		if l == nil {
			return model, fmt.Errorf("lock '%s' does not exist", req.AcquireLock.ResourceId)
		}
		if l.ExecutionId == req.AcquireLock.ExecutionId && l.ExpiresAt > resTime {
			return model, fmt.Errorf("lock '%s' should have been acquired", req.AcquireLock.ResourceId)
		}

		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.AcquireLock.Status)
	}
}

func (v *Validator) ValidateReleaseLock(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	l := model.locks.get(req.ReleaseLock.ResourceId)

	switch res.ReleaseLock.Status {
	case t_api.StatusNoContent:
		if l == nil {
			return model, fmt.Errorf("lock '%s' does not exist", req.ReleaseLock.ResourceId)
		}
		if l.ExecutionId != req.ReleaseLock.ExecutionId && l.ExpiresAt >= resTime {
			return model, fmt.Errorf("lock '%s' not acquired", req.ReleaseLock.ResourceId)
		}

		model = model.Copy()
		model.locks.delete(req.ReleaseLock.ResourceId)
		return model, nil
	case t_api.StatusLockNotFound:
		if l != nil && l.ExecutionId == req.ReleaseLock.ExecutionId && l.ExpiresAt > resTime {
			return model, fmt.Errorf("lock '%s' exists", req.ReleaseLock.ResourceId)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.ReleaseLock.Status)
	}
}

func (v *Validator) ValidateHeartbeatLocks(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	switch res.HeartbeatLocks.Status {
	case t_api.StatusOK:
		// TODO(avillega): Disabling heartbeat locks validation until we find a better way to partition them
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.HeartbeatLocks.Status)
	}
}

// TASKS

func (v *Validator) ValidateClaimTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	t := model.tasks.get(req.ClaimTask.Id)

	switch res.ClaimTask.Status {
	case t_api.StatusCreated:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.ClaimTask.Id)
		}
		if t.State != task.Init && t.State != task.Enqueued {
			return model, fmt.Errorf("task '%s' not claimable", req.ClaimTask.Id)
		}
		if req.ClaimTask.Counter != t.Counter {
			return model, fmt.Errorf("task '%s' counter mismatch (%d != %d)", req.ClaimTask.Id, req.ClaimTask.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(req.ClaimTask.Id, res.ClaimTask.Task)
		return model, nil
	case t_api.StatusTaskAlreadyClaimed:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.ClaimTask.Id)
		}
		if t.State != task.Claimed {
			return model, fmt.Errorf("task '%s' not claimed", req.ClaimTask.Id)
		}
		return model, nil
	case t_api.StatusTaskAlreadyCompleted:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.ClaimTask.Id)
		}
		if !t.State.In(task.Completed|task.Timedout) && t.Timeout >= resTime {
			// This could happen if the promise timetout
			p := model.promises.get(t.RootPromiseId)
			if p == nil {
				return model, nil
			}

			if !promise.GetTimedoutState(p).In(promise.Pending) && resTime >= p.Timeout {
				model = model.Copy()
				newP := promise.Promise{
					Id:                        p.Id,
					State:                     promise.GetTimedoutState(p),
					Param:                     p.Param,
					Value:                     p.Value,
					Timeout:                   p.Timeout,
					IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
					IdempotencyKeyForComplete: p.IdempotencyKeyForComplete,
					Tags:                      p.Tags,
					CreatedOn:                 p.CreatedOn,
					CompletedOn:               util.ToPointer(p.Timeout),
					SortId:                    p.SortId,
				}
				model.promises.set(p.Id, &newP)
				completeRelatedTasks(model, p.Id, reqTime)
			} else {
				return model, fmt.Errorf("task '%s' state not completed", req.ClaimTask.Id)
			}
		}
		return model, nil
	case t_api.StatusTaskInvalidCounter:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.ClaimTask.Id)
		}
		if req.ClaimTask.Counter == t.Counter && t.ExpiresAt >= resTime {
			return model, fmt.Errorf("task '%s' counter match (%d == %d)", req.ClaimTask.Id, req.ClaimTask.Counter, t.Counter)
		}
		return model, nil
	case t_api.StatusTaskNotFound:
		if t != nil {
			return model, fmt.Errorf("task '%s' exists", req.ClaimTask.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.ClaimTask.Status)
	}
}

func (v *Validator) ValidateCompleteTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	t := model.tasks.get(req.CompleteTask.Id)

	switch res.CompleteTask.Status {
	case t_api.StatusCreated:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.CompleteTask.Id)
		}
		if t.State != task.Claimed {
			return model, fmt.Errorf("task '%s' state not claimed", req.CompleteTask.Id)
		}
		if req.CompleteTask.Counter != t.Counter {
			return model, fmt.Errorf("task '%s' counter mismatch (%d != %d)", req.CompleteTask.Id, req.CompleteTask.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(req.CompleteTask.Id, res.CompleteTask.Task)
		return model, nil
	case t_api.StatusOK:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.CompleteTask.Id)
		}
		if !t.State.In(task.Completed|task.Timedout) && t.Timeout >= resTime {
			// This could happen if the promise timedout
			p := model.promises.get(t.RootPromiseId)
			if p == nil {
				return model, nil
			}

			if !promise.GetTimedoutState(p).In(promise.Pending) && resTime >= p.Timeout {
				model = model.Copy()
				newP := promise.Promise{
					Id:                        p.Id,
					State:                     promise.GetTimedoutState(p),
					Param:                     p.Param,
					Value:                     p.Value,
					Timeout:                   p.Timeout,
					IdempotencyKeyForCreate:   p.IdempotencyKeyForCreate,
					IdempotencyKeyForComplete: p.IdempotencyKeyForComplete,
					Tags:                      p.Tags,
					CreatedOn:                 p.CreatedOn,
					CompletedOn:               util.ToPointer(p.Timeout),
					SortId:                    p.SortId,
				}
				model.promises.set(p.Id, &newP)
				completeRelatedTasks(model, p.Id, reqTime)
			} else {
				return model, fmt.Errorf("task '%s' state not completed", req.CompleteTask.Id)
			}
		}
		return model, nil
	case t_api.StatusTaskInvalidCounter:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.CompleteTask.Id)
		}
		if req.CompleteTask.Counter == t.Counter && t.ExpiresAt >= resTime {
			return model, fmt.Errorf("task '%s' counter match (%d == %d)", req.CompleteTask.Id, req.CompleteTask.Counter, t.Counter)
		}
		return model, nil
	case t_api.StatusTaskInvalidState:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", req.CompleteTask.Id)
		}
		return model, nil
	case t_api.StatusTaskNotFound:
		if t != nil {
			return model, fmt.Errorf("task '%s' exists", req.CompleteTask.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.CompleteTask.Status)
	}
}

func (v *Validator) ValidateHeartbeatTasks(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	switch res.HeartbeatTasks.Status {
	case t_api.StatusOK:
		var tasks []*task.Task
		var lb, ub int64

		for _, t := range *model.tasks {
			util.Assert(t.value.State != task.Claimed || t.value.ProcessId != nil, "process id must be set if claimed")
			if t.value.State == task.Claimed && *t.value.ProcessId == req.HeartbeatTasks.ProcessId {
				ub++
				if t.value.ExpiresAt > resTime && t.value.Timeout > resTime {
					lb++
					tasks = append(tasks, t.value)
				}
			}
		}

		if res.HeartbeatTasks.TasksAffected < lb || res.HeartbeatTasks.TasksAffected > ub {
			return model, fmt.Errorf("tasks affected (%d) must be between [%d, %d] for pid '%s'", res.HeartbeatTasks.TasksAffected, lb, ub, req.HeartbeatTasks.ProcessId)
		}

		for i, t := range tasks {
			if i == 0 {
				// copy the model only once
				model = model.Copy()
			}

			// we can only update the model for tasks that are unambiguously
			// heartbeated, and it's only an approximation
			t.ExpiresAt = reqTime + int64(t.Ttl)
			model.tasks.set(t.Id, t)
		}

		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.HeartbeatTasks.Status)
	}
}

// This function modifies the model in place, make sure you have called
// model.copy() before calling this function
func completeRelatedTasks(model *Model, promiseId string, _ int64) {
	new_tasks := []task.Task{}
	rp := model.promises.get(promiseId)
	rpCompletedOn := util.SafeDeref(rp.CompletedOn)
	for _, t := range *model.tasks {
		if t.value.State.In(task.Completed | task.Timedout) {
			continue
		}
		// A task created after the promise was completed (resumes) must not be completed
		if t.value.RootPromiseId == promiseId && *t.value.CreatedOn < rpCompletedOn {
			new_t := *t.value // Make a copy to avoid modifing the model
			new_t.State = task.Completed
			new_t.CompletedOn = &rpCompletedOn
			new_tasks = append(new_tasks, new_t)
		}
	}

	for _, new_t := range new_tasks {
		model.tasks.set(new_t.Id, &new_t)
	}
}
