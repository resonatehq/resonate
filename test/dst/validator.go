package dst

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
)

type Validator struct {
	responses map[t_api.Kind]ResponseValidator
}

type ResponseValidator func(*Model, int64, int64, *t_api.Request, *t_api.Response) (*Model, error)

func NewValidator() *Validator {
	return &Validator{
		responses: make(map[t_api.Kind]ResponseValidator),
	}
}

func (v *Validator) AddResponse(kind t_api.Kind, response ResponseValidator) {
	v.responses[kind] = response
}

func (v *Validator) Validate(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	f, ok := v.responses[res.Kind]
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
		if p.State != res.ReadPromise.Promise.State {
			// the only way this can happen is if the promise timedout
			if res.ReadPromise.Promise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(req.ReadPromise.Id, res.ReadPromise.Promise)
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
		regex := regexp.MustCompile(fmt.Sprintf("^%s$", strings.ReplaceAll(req.SearchPromises.Id, "*", ".*")))

		for _, p := range res.SearchPromises.Promises {
			// ignore scheduled promises
			if _, ok := p.Tags["resonate:schedule"]; ok {
				continue
			}

			if model.promises.get(p.Id) == nil {
				return model, fmt.Errorf("promise '%s' does not exist", p.Id)
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
	p := model.promises.get(req.CreatePromise.Id)

	switch res.CreatePromise.Status {
	case t_api.StatusCreated:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", req.CreatePromise.Id)
		}
		if res.CreatePromise.Promise.State != promise.Pending {
			return model, fmt.Errorf("invalid state transition (INIT -> %s) for promise '%s'", res.CreatePromise.Promise.State, req.CreatePromise.Id)
		}

		// update model state
		model = model.Copy()
		model.promises.set(req.CreatePromise.Id, res.CreatePromise.Promise)
		return model, nil
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CreatePromise.Id)
		}
		if p.State != res.CreatePromise.Promise.State {
			// the only way this can happen is if the promise timedout
			if res.CreatePromise.Promise.State == promise.GetTimedoutState(p) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(req.CreatePromise.Id, res.CreatePromise.Promise)
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, res.CreatePromise.Promise.State, req.CreatePromise.Id)
			}
		}
		if !p.IdempotencyKeyForCreate.Match(res.CreatePromise.Promise.IdempotencyKeyForCreate) {
			return model, fmt.Errorf("ikey mismatch (%s, %s) for promise '%s'", p.IdempotencyKeyForCreate, res.CreatePromise.Promise.IdempotencyKeyForCreate, req.CreatePromise.Id)
		} else if req.CreatePromise.Strict && p.State != promise.Pending {
			return model, fmt.Errorf("unexpected prior state '%s' when strict true for promise '%s'", p.State, req.CreatePromise.Id)
		}
		return model, nil
	case t_api.StatusPromiseAlreadyExists:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", req.CreatePromise.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.CreatePromise.Status)
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
		if p.State != promise.Resolved && !(p.State == promise.Pending && p.Tags["resonate:timeout"] == "true" && resTime >= p.Timeout) {
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
		if p.State != promise.Timedout && !(p.State == promise.Pending && p.Tags["resonate:timeout"] != "true" && resTime >= p.Timeout) {
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
		regex := regexp.MustCompile(fmt.Sprintf("^%s$", strings.ReplaceAll(req.SearchSchedules.Id, "*", ".*")))

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
		if res.HeartbeatLocks.LocksAffected == 0 {
			return model, nil
		}

		var lb, ub int64
		for _, l := range *model.locks {
			if l.value.ProcessId == req.HeartbeatLocks.ProcessId {
				if l.value.ExpiresAt >= resTime {
					lb++
				}
				ub++
			}
		}

		if res.HeartbeatLocks.LocksAffected < lb || res.HeartbeatLocks.LocksAffected > ub {
			return model, fmt.Errorf("locks affected (%d) must be between [%d, %d] for pid '%s'", res.HeartbeatLocks.LocksAffected, lb, ub, req.HeartbeatLocks.ProcessId)
		}

		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.HeartbeatLocks.Status)
	}
}
