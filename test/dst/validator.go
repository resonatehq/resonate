package dst

import (
	"fmt"
	"math/rand"
	"regexp"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/callback"
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
	readPromiseReq := req.Data.(*t_api.PromiseGetRequest)
	readPromiseRes := res.Data.(*t_api.PromiseGetResponse)
	p := model.promises.get(readPromiseReq.Id)

	switch res.Status {
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", readPromiseReq.Id)
		}
		if readPromiseRes.Promise.State == promise.Pending && reqTime > p.Timeout {
			return model, fmt.Errorf("promise '%s' should be timedout", p.Id)
		}
		if p.State != readPromiseRes.Promise.State {
			// the only way this can happen is if the promise timedout
			if readPromiseRes.Promise.State == promise.GetTimedoutState(p.Tags) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(readPromiseReq.Id, readPromiseRes.Promise)
				return model, nil
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, readPromiseRes.Promise.State, readPromiseReq.Id)
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
	searchPromisesReq := req.Data.(*t_api.PromiseSearchRequest)
	searchPromisesRes := res.Data.(*t_api.PromiseSearchResponse)

	switch res.Status {
	case t_api.StatusOK:
		regex := v.regexes[searchPromisesReq.Id]

		for _, p := range searchPromisesRes.Promises {
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
	createPromiseReq := req.Data.(*t_api.PromiseCreateRequest)
	createPromiseRes := res.Data.(*t_api.PromiseCreateResponse)

	return v.validateCreatePromise(model, reqTime, resTime, createPromiseReq, res.Status, createPromiseRes)
}

func (v *Validator) ValidateCreatePromiseAndTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	createPromiseAndTaskReq := req.Data.(*t_api.TaskCreateRequest)
	createPromiseAndTaskRes := res.Data.(*t_api.TaskCreateResponse)

	switch res.Status {
	case t_api.StatusCreated:
		if createPromiseAndTaskRes.Task.ExpiresAt < 0 {
			return model, fmt.Errorf("invalid task expiry %d", createPromiseAndTaskRes.Task.ExpiresAt)
		}
		if model.tasks.get(createPromiseAndTaskRes.Task.Id) != nil {
			return model, fmt.Errorf("task '%s' exists", createPromiseAndTaskRes.Task.Id)
		}

		model = model.Copy()
		model.tasks.set(createPromiseAndTaskRes.Task.Id, createPromiseAndTaskRes.Task)
	}

	promiseRes := &t_api.PromiseCreateResponse{
		Promise: createPromiseAndTaskRes.Promise,
	}

	return v.validateCreatePromise(model, reqTime, resTime, createPromiseAndTaskReq.Promise, res.Status, promiseRes)
}

func (v *Validator) validateCreatePromise(model *Model, reqTime int64, resTime int64, req *t_api.PromiseCreateRequest, status t_api.StatusCode, res *t_api.PromiseCreateResponse) (*Model, error) {
	p := model.promises.get(req.Id)

	switch status {
	case t_api.StatusCreated:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", req.Id)
		}
		if res.Promise.Timeout > resTime && res.Promise.State != promise.Pending {
			return model, fmt.Errorf("invalid state transition (INIT -> %s) for promise '%s'", res.Promise.State, req.Id)
		}

		timedoutState := promise.Timedout
		if res.Promise.Tags["resonate:timeout"] == "true" {
			timedoutState = promise.Resolved
		}
		if res.Promise.Timeout <= resTime && !res.Promise.State.In(promise.Pending|timedoutState) {
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
			// the only way this can happen with this test setup is if the promise timedout
			if res.Promise.State == promise.GetTimedoutState(p.Tags) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(req.Id, res.Promise)
				return model, nil
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s' model: %v", p.State, res.Promise.State, req.Id, p)
			}
		}
		return model.Copy(), nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", status)
	}
}

func (v *Validator) ValidateCompletePromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	completePromiseReq := req.Data.(*t_api.PromiseCompleteRequest)
	completePromiseRes := res.Data.(*t_api.PromiseCompleteResponse)
	p := model.promises.get(completePromiseReq.Id)

	switch res.Status {
	case t_api.StatusCreated:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if p.State != promise.Pending || completePromiseRes.Promise.State == promise.Pending || completePromiseRes.Promise.State == promise.Timedout {
			return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, completePromiseRes.Promise.State, completePromiseReq.Id)
		}
		if completePromiseReq.State == promise.Resolved && completePromiseRes.Promise.State != promise.Resolved {
			return model, fmt.Errorf("unexpected state '%s' after resolve for promise '%s'", completePromiseRes.Promise.State, completePromiseReq.Id)
		}
		if completePromiseReq.State == promise.Rejected && completePromiseRes.Promise.State != promise.Rejected {
			return model, fmt.Errorf("unexpected state '%s' after reject for promise '%s'", completePromiseRes.Promise.State, completePromiseReq.Id)
		}
		if completePromiseReq.State == promise.Canceled && completePromiseRes.Promise.State != promise.Canceled {
			return model, fmt.Errorf("unexpected state '%s' after cancel for promise '%s'", completePromiseRes.Promise.State, completePromiseReq.Id)
		}

		// update model state
		model = model.Copy()
		model.promises.set(completePromiseReq.Id, completePromiseRes.Promise)
		return model, nil
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", completePromiseReq.Id)
		}
		if completePromiseRes.Promise.State == promise.Pending {
			return model, fmt.Errorf("unexpected state '%s' after complete for promise '%s'", completePromiseRes.Promise.State, completePromiseReq.Id)
		}
		if p.State != completePromiseRes.Promise.State {
			// the only way this can happen is if the promise timedout
			if completePromiseRes.Promise.State == promise.GetTimedoutState(p.Tags) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(completePromiseReq.Id, completePromiseRes.Promise)
				return model, nil
			} else {
				return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, completePromiseRes.Promise.State, completePromiseReq.Id)
			}
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

func (v *Validator) ValidateRegisterPromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	registerPromiseReq := req.Data.(*t_api.PromiseRegisterRequest)
	registerPromiseRes := res.Data.(*t_api.PromiseRegisterResponse)
	awaited := model.promises.get(registerPromiseReq.Awaited)

	switch res.Status {
	case t_api.StatusOK:
		if awaited == nil {
			return model, fmt.Errorf("promise '%s' does not exist", registerPromiseReq.Awaited)
		}
		if awaited.State != registerPromiseRes.Promise.State {
			if registerPromiseRes.Promise.State == promise.GetTimedoutState(awaited.Tags) && resTime >= awaited.Timeout {
				model = model.Copy()
				model.promises.set(registerPromiseReq.Awaited, registerPromiseRes.Promise)
				return model, nil
			}
			return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", awaited.State, registerPromiseRes.Promise.State, registerPromiseReq.Awaited)
		}

		cbId := util.ResumeId(registerPromiseReq.Awaiter, registerPromiseReq.Awaited)
		// Assume the callback is created, for compatibility with old protocol and its validation
		model = model.Copy()
		model.callbacks.set(cbId, &callback.Callback{
			Id:            cbId,
			PromiseId:     awaited.Id,
			RootPromiseId: registerPromiseReq.Awaiter,
		})
		return model, nil
	case t_api.StatusPromiseNotFound:
		// TODO(avillega) what can we validate?
		return model, nil
	case t_api.StatusPromiseRecvNotFound:
		// TODO(avillega) can we validate this?
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateSubscribePromise(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	subscribePromiseReq := req.Data.(*t_api.PromiseSubscribeRequest)
	subscribePromiseRes := res.Data.(*t_api.PromiseSubscribeResponse)
	p := model.promises.get(subscribePromiseReq.Awaited)

	switch res.Status {
	case t_api.StatusOK:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", subscribePromiseReq.Awaited)
		}
		if subscribePromiseRes.Promise.State == promise.Pending && reqTime > p.Timeout {
			return model, fmt.Errorf("promise '%s' should be timedout", p.Id)
		}
		if p.State != subscribePromiseRes.Promise.State {
			if subscribePromiseRes.Promise.State == promise.GetTimedoutState(p.Tags) && resTime >= p.Timeout {
				model = model.Copy()
				model.promises.set(subscribePromiseReq.Awaited, subscribePromiseRes.Promise)
				return model, nil
			}
			return model, fmt.Errorf("invalid state transition (%s -> %s) for promise '%s'", p.State, subscribePromiseRes.Promise.State, subscribePromiseReq.Awaited)
		}
		cbId := util.NotifyId(p.Id, subscribePromiseReq.Address)
		// Assume the callback is created, for compatibility with old protocol and its validation
		model = model.Copy()
		model.callbacks.set(cbId, &callback.Callback{
			Id:            cbId,
			PromiseId:     p.Id,
			RootPromiseId: p.Id,
		})
		return model, nil
	case t_api.StatusPromiseNotFound:
		if p != nil {
			return model, fmt.Errorf("promise '%s' exists", subscribePromiseReq.Awaited)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

func (v *Validator) ValidateCreateCallback(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	createCallbackReq := req.Data.(*t_api.CallbackCreateRequest)
	createCallbackRes := res.Data.(*t_api.CallbackCreateResponse)
	p := model.promises.get(createCallbackReq.PromiseId)

	switch res.Status {
	case t_api.StatusCreated:
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", createCallbackReq.PromiseId)
		}

		if p.State != promise.Pending {
			return model, fmt.Errorf("promise '%s' must be pending", createCallbackReq.PromiseId)
		}
		// Set the callback unconditionally to play nicely with promiseRegister and promiseSubscribe
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
	readScheduleReq := req.Data.(*t_api.ScheduleGetRequest)
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
	searchSchedulesReq := req.Data.(*t_api.ScheduleSearchRequest)
	schedules := res.Data.(*t_api.ScheduleSearchResponse).Schedules

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
	createScheduleReq := req.Data.(*t_api.ScheduleCreateRequest)
	createScheduleRes := res.Data.(*t_api.ScheduleCreateResponse)
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
		return model, nil
	default:
		return model, fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}
}

func (v *Validator) ValidateDeleteSchedule(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	deleteScheduleReq := req.Data.(*t_api.ScheduleDeleteRequest)
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

// TASKS

func (v *Validator) ValidateClaimTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	claimTaskReq := req.Data.(*t_api.TaskAcquireRequest)
	claimTaskRes := res.Data.(*t_api.TaskAcquireResponse)
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
		if claimTaskRes.Task.ExpiresAt < 0 {
			return model, fmt.Errorf("invalid task expiry %d", claimTaskRes.Task.ExpiresAt)
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
	completeTaskReq := req.Data.(*t_api.TaskCompleteRequest)
	completeTaskRes := res.Data.(*t_api.TaskCompleteResponse)
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
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", completeTaskReq.Id)
		}
		if !completeTaskRes.Task.State.In(task.Completed | task.Timedout) {
			return model, fmt.Errorf("task '%s' not completed", completeTaskReq.Id)
		}

		// the response should have been status created if:
		// - the task was not already completed or timedout
		// - the task has not timedout
		// - the root promise is still pending
		p := model.promises.get(t.RootPromiseId)

		if !t.State.In(task.Completed|task.Timedout) && t.Timeout > resTime && p != nil && p.State == promise.Pending && p.Timeout > resTime {
			return model, fmt.Errorf("task '%s' should have been completed", completeTaskReq.Id)
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
	dropTaskReq := req.Data.(*t_api.TaskReleaseRequest)
	dropTaskRes := res.Data.(*t_api.TaskReleaseResponse)
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
		if dropTaskRes.Task.ExpiresAt < 0 {
			return model, fmt.Errorf("invalid task expiry %d", dropTaskRes.Task.ExpiresAt)
		}

		model = model.Copy()
		model.tasks.set(dropTaskReq.Id, dropTaskRes.Task)
		return model, nil
	case t_api.StatusOK:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", dropTaskReq.Id)
		}

		// the response should have been StatusCreated if:
		// - the task was claimed
		// - the task has not expired or timedout
		// - the root promise is still pending
		p := model.promises.get(t.RootPromiseId)
		if t.State == task.Claimed && t.ExpiresAt >= resTime && t.Timeout >= resTime && p != nil && p.State == promise.Pending && p.Timeout > resTime {
			return model, fmt.Errorf("task '%s' not init", dropTaskReq.Id)
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
	heartbeatTasksReq := req.Data.(*t_api.TaskHeartbeatRequest)
	heartbeatTasksRes := res.Data.(*t_api.TaskHeartbeatResponse)

	switch res.Status {
	case t_api.StatusOK:
		var tasks []*task.Task
		var lb, ub int64

		for _, t := range *model.tasks {
			util.Assert(t.value.State != task.Claimed || t.value.ProcessId != nil, "process id must be set if claimed")
			if t.value.State == task.Claimed && *t.value.ProcessId == heartbeatTasksReq.ProcessId {
				ub++

				p := model.promises.get(t.value.RootPromiseId)
				if p != nil && p.State == promise.Pending && t.value.ExpiresAt > resTime && t.value.Timeout > resTime && p.Timeout > resTime {
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
			t.ExpiresAt = util.ClampAddInt64(reqTime, t.Ttl)
			model.tasks.set(t.Id, t)
		}

		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}

// TODO(avillega) revisit this validation once TasksV2 is implemented
func (v *Validator) ValidateFulfillTask(model *Model, reqTime int64, resTime int64, req *t_api.Request, res *t_api.Response) (*Model, error) {
	fulfillTaskReq := req.Data.(*t_api.TaskFulfillRequest)
	fulfillTaskRes := res.Data.(*t_api.TaskFulfillResponse)
	t := model.tasks.get(fulfillTaskReq.Id)
	p := model.promises.get(fulfillTaskReq.Action.Id)

	switch res.Status {
	case t_api.StatusOK:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", fulfillTaskReq.Id)
		}
		if t.State != task.Claimed {
			return model, fmt.Errorf("task '%s' state not claimed", fulfillTaskReq.Id)
		}
		if p == nil {
			return model, fmt.Errorf("promise '%s' does not exist", fulfillTaskReq.Action.Id)
		}
		if fulfillTaskRes.Promise.State == promise.Pending {
			return model, fmt.Errorf("promise '%s' should not be pending after fulfill", fulfillTaskReq.Action.Id)
		}

		model = model.Copy()
		model.promises.set(fulfillTaskReq.Action.Id, fulfillTaskRes.Promise)
		return model, nil
	case t_api.StatusTaskNotClaimed:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", fulfillTaskReq.Id)
		}
		if t.State == task.Claimed {
			return model, fmt.Errorf("task '%s' is claimed", fulfillTaskReq.Id)
		}
		return model, nil
	case t_api.StatusTaskInvalidVersion:
		if t == nil {
			return model, fmt.Errorf("task '%s' does not exist", fulfillTaskReq.Id)
		}
		if fulfillTaskReq.Version == t.Counter && t.ExpiresAt >= resTime {
			return model, fmt.Errorf("task '%s' version match (%d == %d)", fulfillTaskReq.Id, fulfillTaskReq.Version, t.Counter)
		}
		return model, nil
	case t_api.StatusTaskNotFound:
		if t != nil {
			return model, fmt.Errorf("task '%s' exists", fulfillTaskReq.Id)
		}
		return model, nil
	default:
		return model, fmt.Errorf("unexpected response status '%d'", res.Status)
	}
}
