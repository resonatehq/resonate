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
	f, ok := v.validators[res.Kind()]
	if !ok {
		return model, fmt.Errorf("unexpected response kind '%s'", res.Kind())
	}

	return f(model, reqTime, resTime, req, res)
}

// PROMISES

func (v *Validator) ValidateReadPromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	readPromiseReq := req.Payload.(*t_api.ReadPromiseRequest)
	p := model.promises.get(readPromiseReq.Id)

	switch res.Status {
	case t_api.StatusOK:
		resPromise := res.Payload.(*t_api.ReadPromiseResponse).Promise
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", readPromiseReq.Id)
		}
		if resPromise.State == promise.Pending && reqTime > p.Timeout {
			return model, fmt.Errorf("promise '%s' should be timedout", p.Id)
		}
		if p.State != resPromise.State {
			// the only way this can happen is if the promise timedout
			if resPromise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(readPromiseReq.Id, resPromise)
				return model, nil
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, resPromise.State, readPromiseReq.Id)
			}
		}
		return model, nil
	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", readPromiseReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}
func (v *Validator) ValidateSearchPromises(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	searchPromisesReq := req.Payload.(*t_api.SearchPromisesRequest)

	switch res.Status {
	case t_api.StatusOK:
		regex := v.regexes[searchPromisesReq.Id]
		promises := res.Payload.(*t_api.SearchPromisesResponse).Promises

		for _, p := range promises {
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
				return model, fmt.Errorf("promise id '%s' does not match search query '%s'", p.Id, searchPromisesReq.Id)
			}
			states := map[promise.State]bool{}
			for _, state := range searchPromisesReq.States {
				states[state] = true
			}
			if _, ok := states[p.State]; !ok {
				return model, fmt.Errorf("promise state '%s' not in search states '%s'", p.State, searchPromisesReq.States)
			}
			if searchPromisesReq.SortId != nil && *searchPromisesReq.SortId <= p.SortId {
				return model, fmt.Errorf("unexpected sortId, promise sortId %d is greater than the request sortId %d", p.SortId, *searchPromisesReq.SortId)
			}
			for k, v := range searchPromisesReq.Tags {
				if _v, ok := p.Tags[k]; !ok || v != _v {
					return model, fmt.Errorf("promise %s has unexpected tag '%s:%s', expected '%s:%s'", p.Id, k, _v, k, v)
				}
			}
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateCreatePromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	createPromiseReq := req.Payload.(*t_api.CreatePromiseRequest)
	return v.validateCreatePromise(model, reqTime, resTime, createPromiseReq, res.Status, res.Payload.(*t_api.CreatePromiseResponse))
}

func (v *Validator) ValidateCreatePromiseAndTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	createPromiseAndTaskReq := req.Payload.(*t_api.CreatePromiseAndTaskRequest)
	createPromiseAndTaskRes := res.Payload.(*t_api.CreatePromiseAndTaskResponse)

	switch res.Status {
	case t_api.StatusCreated:
		if model.tasks.get(createPromiseAndTaskRes.Task.Id) != nil {
			return model, fmt.Errorf("task '%s' exists", createPromiseAndTaskRes.Task.Id)
		}

		model = model.Copy()
		model.tasks.set(createPromiseAndTaskRes.Task.Id, createPromiseAndTaskRes.Task)
	}

	promiseRes := &t_api.CreatePromiseResponse{
		Promise: createPromiseAndTaskRes.Promise,
	}

	return v.validateCreatePromise(model, reqTime, resTime, createPromiseAndTaskReq.Promise, res.Status, promiseRes)
}

func (v *Validator) validateCreatePromise(model *Model, reqTime int64, resTime int64, req *t_api.CreatePromiseRequest, status t_api.StatusCode, res *t_api.CreatePromiseResponse) (*Model, error) {
	p := model.promises.get(req.Id)

	switch status {
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
				return model, nil
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
		return model, fmt.Errorf("unexpected resonse status '%d'", status)
	}
}

func (v *Validator) ValidateCompletePromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	completePromiseReq := req.Payload.(*t_api.CompletePromiseRequest)
	p := model.promises.get(completePromiseReq.Id)
	resPromise := res.Payload.(*t_api.CompletePromiseResponse).Promise

	switch res.Status {
	case t_api.StatusCreated:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if p.State != promise.Pending || resPromise.State == promise.Pending || resPromise.State == promise.Timedout {
			return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, resPromise.State, completePromiseReq.Id)
		}
		if completePromiseReq.State == promise.Resolved && resPromise.State != promise.Resolved {
			return model, fmt.Errorf("unexpected state '%s' after resolve for promise '%s'", resPromise.State, completePromiseReq.Id)
		}
		if completePromiseReq.State == promise.Rejected && resPromise.State != promise.Rejected {
			return model, fmt.Errorf("unexpected state '%s' after reject for promise '%s'", resPromise.State, completePromiseReq.Id)
		}
		if completePromiseReq.State == promise.Canceled && resPromise.State != promise.Canceled {
			return model, fmt.Errorf("unexpected state '%s' after cancel for promise '%s'", resPromise.State, completePromiseReq.Id)
		}

		// update model state
		model = model.Copy()
		model.promises.set(completePromiseReq.Id, resPromise)
		return model, nil
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if resPromise.State == promise.Pending {
			return model, fmt.Errorf("unexpected state '%s' after complete for promise '%s'", resPromise.State, completePromiseReq.Id)
		}
		if p.State != resPromise.State {
			// the only way this can happen is if the promise timedout
			if resPromise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(completePromiseReq.Id, resPromise)
				return model, nil
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, resPromise.State, completePromiseReq.Id)
			}
		}
		if !p.IdempotencyKeyForComplete.Match(resPromise.IdempotencyKeyForComplete) && (completePromiseReq.Strict || resPromise.State != promise.Timedout) {
			return model, fmt.Errorf("ikey mismatch (%s, %s) for promise '%s'", p.IdempotencyKeyForCreate, resPromise.IdempotencyKeyForCreate, completePromiseReq.Id)
		} else if completePromiseReq.Strict && p.State != completePromiseReq.State {
			return model, fmt.Errorf("unexpected prior state '%s' when strict true for promise '%s'", p.State, completePromiseReq.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyResolved:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if p.State != promise.Resolved && (p.State != promise.Pending || p.Tags["resonate:timeout"] != "true" || resTime < p.Timeout) {
			return model, fmt.Errorf("promise '%s' not resolved", completePromiseReq.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyRejected:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if p.State != promise.Rejected {
			return model, fmt.Errorf("promise '%s' not rejected", completePromiseReq.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyCanceled:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if p.State != promise.Canceled {
			return model, fmt.Errorf("promise '%s' not canceled", completePromiseReq.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyTimedout:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if p.State != promise.Timedout && (p.State != promise.Pending || p.Tags["resonate:timeout"] == "true" || resTime < p.Timeout) {
			return model, fmt.Errorf("promise '%s' not timedout", completePromiseReq.Id)
		}
		return model, nil
	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", completePromiseReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateCreateCallback(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	createCallbackReq := req.Payload.(*t_api.CreateCallbackRequest)
	createCallbackRes := res.Payload.(*t_api.CreateCallbackResponse)
	p := model.promises.get(createCallbackReq.PromiseId)

	switch res.Status {
	case t_api.StatusCreated:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", createCallbackReq.PromiseId)
		}

		if p.State != promise.Pending {
			return model, fmt.Errorf("promise '%s' must be pending", createCallbackReq.PromiseId)
		}

		if model.callbacks.get(createCallbackRes.Callback.Id) != nil {
			return model, fmt.Errorf("callback '%s' exists", createCallbackRes.Callback.Id)
		}

		model = model.Copy()
		model.callbacks.set(createCallbackRes.Callback.Id, createCallbackRes.Callback)
		return model, nil

	case t_api.StatusOK:
		// If the status is Ok there has to be a promise
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", createCallbackReq.PromiseId)
		}

		// If the promise is completed we don't create a callback but return 200
		if p.State != promise.Pending {
			return model, nil
		}

		// If the promise is pending is possible that it has been timeout so we update the promises model
		// if the promise is timedout we can handle it as if it was completed
		if resTime >= p.Timeout {
			model = model.Copy()
			model.promises.set(p.Id, createCallbackRes.Promise)
			return model, nil
		}

		// otherwise verify the callback was created previously
		if model.callbacks.get(createCallbackReq.Id) == nil {
			return model, fmt.Errorf("callback '%s' must exist", createCallbackReq.Id)
		}

		return model, nil

	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", createCallbackReq.PromiseId)
		}
		return model, nil

	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateReadSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	readScheduleReq := req.Payload.(*t_api.ReadScheduleRequest)
	s := model.schedules.get(readScheduleReq.Id)

	switch res.Status {
	case t_api.StatusOK:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", readScheduleReq.Id)
		}
		return model, nil
	case t_api.StatusScheduleNotFound:
		if s != nil {
			return model, fmt.Errorf("schedule '%s' exists", readScheduleReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateSearchSchedules(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	searchSchedulesReq := req.Payload.(*t_api.SearchSchedulesRequest)
	schedules := res.Payload.(*t_api.SearchSchedulesResponse).Schedules

	switch res.Status {
	case t_api.StatusOK:
		regex := v.regexes[searchSchedulesReq.Id]

		for _, s := range schedules {
			if model.schedules.get(s.Id) == nil {
				return model, fmt.Errorf("schedule '%s' does not exist", s.Id)
			}
			if !regex.MatchString(s.Id) {
				return model, fmt.Errorf("schedule id '%s' does not match search query '%s'", s.Id, searchSchedulesReq.Id)
			}
			if searchSchedulesReq.SortId != nil && *searchSchedulesReq.SortId <= s.SortId {
				return model, fmt.Errorf("unexpected sortId, schedule sortId %d is greater than the request sortId %d", s.SortId, *searchSchedulesReq.SortId)
			}
			for k, v := range searchSchedulesReq.Tags {
				if _v, ok := s.Tags[k]; !ok || v != _v {
					return model, fmt.Errorf("schedule %s has unexpected tag '%s:%s', expected '%s:%s'", s.Id, k, _v, k, v)
				}
			}
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateCreateSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	createScheduleReq := req.Payload.(*t_api.CreateScheduleRequest)
	createScheduleRes := res.Payload.(*t_api.CreateScheduleResponse)
	s := model.schedules.get(createScheduleReq.Id)

	switch res.Status {
	case t_api.StatusCreated:
		if s != nil {
			return model, fmt.Errorf("schedule '%s' exists", createScheduleReq.Id)
		}

		model = model.Copy()
		model.schedules.set(createScheduleReq.Id, createScheduleRes.Schedule)
		return model, nil
	case t_api.StatusOK:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", createScheduleReq.Id)
		}
		if !s.IdempotencyKey.Match(createScheduleRes.Schedule.IdempotencyKey) {
			return model, fmt.Errorf("ikey mismatch (%s, %s) for schedule '%s'", s.IdempotencyKey, createScheduleRes.Schedule.IdempotencyKey, createScheduleReq.IdempotencyKey)
		}
		return model, nil
	case t_api.StatusScheduleAlreadyExists:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", createScheduleReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateDeleteSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	deleteScheduleReq := req.Payload.(*t_api.DeleteScheduleRequest)
	s := model.schedules.get(deleteScheduleReq.Id)

	switch res.Status {
	case t_api.StatusNoContent:
		if s == nil {
			return model, fmt.Errorf("schedule '%s' does not exist", deleteScheduleReq.Id)
		}

		model = model.Copy()
		model.schedules.delete(deleteScheduleReq.Id)
		return model, nil
	case t_api.StatusScheduleNotFound:
		if s != nil {
			return model, fmt.Errorf("schedule '%s' exists", deleteScheduleReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateAcquireLock(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	acquireLockReq := req.Payload.(*t_api.AcquireLockRequest)
	acquireLockRes := res.Payload.(*t_api.AcquireLockResponse)
	l := model.locks.get(acquireLockReq.ResourceId)

	switch res.Status {
	case t_api.StatusCreated:
		if l != nil && l.ExecutionId != acquireLockReq.ExecutionId && l.ExpiresAt >= resTime {
			return model, fmt.Errorf("lock '%s' exists", acquireLockReq.ResourceId)
		}

		model = model.Copy()
		model.locks.set(acquireLockReq.ResourceId, acquireLockRes.Lock)
		return model, nil
	case t_api.StatusLockAlreadyAcquired:
		if l == nil {
			return model, fmt.Errorf("lock '%s' does not exist", acquireLockReq.ResourceId)
		}
		if l.ExecutionId == acquireLockReq.ExecutionId && l.ExpiresAt > resTime {
			return model, fmt.Errorf("lock '%s' should have been acquired", acquireLockReq.ResourceId)
		}

		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateReleaseLock(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	releaseLockReq := req.Payload.(*t_api.ReleaseLockRequest)
	l := model.locks.get(releaseLockReq.ResourceId)

	switch res.Status {
	case t_api.StatusNoContent:
		if l == nil {
			return model, fmt.Errorf("lock '%s' does not exist", releaseLockReq.ResourceId)
		}
		if l.ExecutionId != releaseLockReq.ExecutionId && l.ExpiresAt >= resTime {
			return model, fmt.Errorf("lock '%s' not acquired", releaseLockReq.ResourceId)
		}

		model = model.Copy()
		model.locks.delete(releaseLockReq.ResourceId)
		return model, nil
	case t_api.StatusLockNotFound:
		if l != nil && l.ExecutionId == releaseLockReq.ExecutionId && l.ExpiresAt > resTime {
			return model, fmt.Errorf("lock '%s' exists", releaseLockReq.ResourceId)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateHeartbeatLocks(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	switch res.Status {
	case t_api.StatusOK:
		// TODO(avillega): Disabling heartbeat locks validation until we find a better way to partition them
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

// TASKS

func (v *Validator) ValidateClaimTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	claimTaskReq := req.Payload.(*t_api.ClaimTaskRequest)
	claimTaskRes := res.Payload.(*t_api.ClaimTaskResponse)
	t := model.tasks.get(claimTaskReq.Id)

	switch res.Status {
	case t_api.StatusCreated:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", claimTaskReq.Id)
		}
		if t.State != task.Init && t.State != task.Enqueued {
			return model, fmt.Errorf("task '%s' not claimable", claimTaskReq.Id)
		}
		if claimTaskReq.Counter != t.Counter {
			return model, fmt.Errorf("task '%s' counter mismatch (%d != %d)", claimTaskReq.Id, claimTaskReq.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(claimTaskReq.Id, claimTaskRes.Task)
		return model, nil
	case t_api.StatusTaskAlreadyClaimed:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", claimTaskReq.Id)
		}
		if t.State != task.Claimed {
			return model, fmt.Errorf("task '%s' not claimed", claimTaskReq.Id)
		}

		model = model.Copy()
		model.tasks.set(claimTaskReq.Id, claimTaskRes.Task)
		return model, nil
	case t_api.StatusTaskAlreadyCompleted:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", claimTaskReq.Id)
		}

		model = model.Copy()
		model.tasks.set(claimTaskReq.Id, claimTaskRes.Task)
		return model, nil
	case t_api.StatusTaskInvalidCounter:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", claimTaskReq.Id)
		}

		model = model.Copy()
		model.tasks.set(claimTaskReq.Id, claimTaskRes.Task)
		return model, nil
	case t_api.StatusTaskNotFound:
		if t != nil {
			return model, fmt.Errorf("task '%s' exists", claimTaskReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateCompleteTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	completeTaskReq := req.Payload.(*t_api.CompleteTaskRequest)
	completeTaskRes := res.Payload.(*t_api.CompleteTaskResponse)
	t := model.tasks.get(completeTaskReq.Id)

	switch res.Status {
	case t_api.StatusCreated:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", completeTaskReq.Id)
		}
		if t.State != task.Claimed {
			return model, fmt.Errorf("task '%s' state not claimed", completeTaskReq.Id)
		}
		if completeTaskReq.Counter != t.Counter {
			return model, fmt.Errorf("task '%s' counter mismatch (%d != %d)", completeTaskReq.Id, completeTaskReq.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(completeTaskReq.Id, completeTaskRes.Task)
		return model, nil
	case t_api.StatusOK:
		p := model.promises.get(t.RootPromiseId)

		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", completeTaskReq.Id)
		}

		// TODO: provide description
		if !t.State.In(task.Completed|task.Timedout) && p != nil && p.State == promise.Pending && t.ExpiresAt >= resTime && resTime >= p.Timeout {
			return model, fmt.Errorf("nah")
		}

		model = model.Copy()
		model.tasks.set(completeTaskReq.Id, completeTaskRes.Task)
		return model, nil
	case t_api.StatusTaskInvalidCounter:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", completeTaskReq.Id)
		}
		if completeTaskReq.Counter == t.Counter && t.ExpiresAt >= resTime {
			return model, fmt.Errorf("task '%s' counter match (%d == %d)", completeTaskReq.Id, completeTaskReq.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(completeTaskReq.Id, completeTaskRes.Task)
		return model, nil
	case t_api.StatusTaskInvalidState:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", completeTaskReq.Id)
		}

		model = model.Copy()
		model.tasks.set(completeTaskReq.Id, completeTaskRes.Task)
		return model, nil
	case t_api.StatusTaskNotFound:
		if t != nil {
			return model, fmt.Errorf("task '%s' exists", completeTaskReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateDropTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	dropTaskReq := req.Payload.(*t_api.DropTaskRequest)
	dropTaskRes := res.Payload.(*t_api.DropTaskResponse)
	t := model.tasks.get(dropTaskReq.Id)

	switch res.Status {
	case t_api.StatusCreated:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", dropTaskReq.Id)
		}
		if t.State != task.Claimed {
			return model, fmt.Errorf("task '%s' state not claimed", dropTaskReq.Id)
		}
		if dropTaskReq.Counter != t.Counter {
			return model, fmt.Errorf("task '%s' counter mismatch (%d != %d)", dropTaskReq.Id, dropTaskReq.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(dropTaskReq.Id, dropTaskRes.Task)
		return model, nil
	case t_api.StatusOK:
		p := model.promises.get(t.RootPromiseId)

		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", dropTaskReq.Id)
		}

		// TODO: provide description
		if !t.State.In(task.Init|task.Enqueued|task.Completed|task.Timedout) && p != nil && p.State == promise.Pending && t.ExpiresAt >= resTime && resTime >= p.Timeout {
			return model, fmt.Errorf("nah")
		}

		model = model.Copy()
		model.tasks.set(dropTaskReq.Id, dropTaskRes.Task)
		return model, nil
	case t_api.StatusTaskInvalidCounter:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", dropTaskReq.Id)
		}
		if dropTaskReq.Counter == t.Counter && t.ExpiresAt >= resTime {
			return model, fmt.Errorf("task '%s' counter match (%d == %d)", dropTaskReq.Id, dropTaskReq.Counter, t.Counter)
		}

		model = model.Copy()
		model.tasks.set(dropTaskReq.Id, dropTaskRes.Task)
		return model, nil
	case t_api.StatusTaskNotFound:
		if t != nil {
			return model, fmt.Errorf("task '%s' exists", dropTaskReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateHeartbeatTasks(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	heartbeatTasksReq := req.Payload.(*t_api.HeartbeatTasksRequest)
	heartbeatTasksRes := res.Payload.(*t_api.HeartbeatTasksResponse)

	switch res.Status {
	case t_api.StatusOK:
		var tasks []*task.Task
		var lb, ub int64

		for _, t := range *model.tasks {
			util.Assert(t.value.State != task.Claimed || t.value.ProcessId != nil, "process id must be set if claimed")
			if t.value.State == task.Claimed && *t.value.ProcessId == heartbeatTasksReq.ProcessId {
				ub++
				if t.value.ExpiresAt > resTime && t.value.Timeout > resTime {
					lb++
					tasks = append(tasks, t.value)
				}
			}
		}

		if heartbeatTasksRes.TasksAffected < lb || heartbeatTasksRes.TasksAffected > ub {
			return model, fmt.Errorf("tasks affected (%d) must be between [%d, %d] for pid '%s'", heartbeatTasksRes.TasksAffected, lb, ub, heartbeatTasksReq.ProcessId)
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
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}
