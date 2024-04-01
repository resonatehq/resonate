// HOW CREATING PROMISES AFFECTS THE MODEL -- ??? naming convention to avoid naming conflict.

package dst

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/lock"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
	"github.com/resonatehq/resonate/pkg/subscription"
	"github.com/resonatehq/resonate/pkg/task"
)

// Model

type Model struct {
	promises  Promises
	schedules Schedules
	locks     Locks
	tasks     Tasks
	cursors   []*t_api.Request
	responses map[t_api.Kind]ResponseValidator
}

type PromiseModel struct {
	id            string
	promise       *promise.Promise
	subscriptions Subscriptions
}

type ScheduleModel struct {
	id       string
	schedule *schedule.Schedule
}

type SubscriptionModel struct {
	id           string
	subscription *subscription.Subscription
}

type LockModel struct {
	id   string
	lock *lock.Lock
}

type TaskModel struct {
	id   string
	task *task.Task
}

type Promises map[string]*PromiseModel
type Schedules map[string]*ScheduleModel
type Subscriptions map[string]*SubscriptionModel
type Locks map[string]*LockModel
type Tasks map[string]*TaskModel
type ResponseValidator func(int64, *t_api.Request, *t_api.Response) error

func (p Promises) Get(id string) (*PromiseModel, error) {
	if _, ok := p[id]; !ok {
		p[id] = &PromiseModel{
			id:            id,
			subscriptions: map[string]*SubscriptionModel{},
		}
	}

	if p[id].promise == nil {
		return nil, errors.New("promise not found")
	}

	return p[id], nil
}

func (s Schedules) Get(id string) (*ScheduleModel, error) {
	if _, ok := s[id]; !ok {
		s[id] = &ScheduleModel{
			id: id,
		}
	}

	if s[id].schedule == nil {
		return nil, errors.New("schedule not found")
	}

	return s[id], nil
}

func (s Subscriptions) Get(id string) (*SubscriptionModel, error) {
	if _, ok := s[id]; !ok {
		s[id] = &SubscriptionModel{
			id: id,
		}
	}

	if s[id].subscription == nil {
		return nil, errors.New("subscription not found")
	}

	return s[id], nil
}

func (l Locks) Get(id string) (*LockModel, error) {
	if _, ok := l[id]; !ok {
		l[id] = &LockModel{
			id: id,
		}
	}

	if l[id].lock == nil {
		return nil, errors.New("lock not found")
	}

	return l[id], nil
}

func (t Tasks) Get(id string) (*TaskModel, error) {
	if _, ok := t[id]; !ok {
		t[id] = &TaskModel{
			id: id,
		}
	}

	if t[id].task == nil {
		return nil, errors.New("task not found")
	}

	return t[id], nil
}

func NewModel() *Model {
	return &Model{
		promises:  map[string]*PromiseModel{},
		schedules: map[string]*ScheduleModel{},
		locks:     map[string]*LockModel{},
		tasks:     map[string]*TaskModel{},
		responses: map[t_api.Kind]ResponseValidator{},
	}
}

func (m *Model) AddResponse(kind t_api.Kind, response ResponseValidator) {
	m.responses[kind] = response
}

func (m *Model) addCursor(next *t_api.Request) {
	m.cursors = append(m.cursors, next)
}

// Validation

func (m *Model) Step(t int64, req *t_api.Request, res *t_api.Response, err error) error {
	if err != nil {
		var resErr *t_api.ResonateError
		if !errors.As(err, &resErr) {
			return fmt.Errorf("unexpected non-resonate error '%v'", err)
		}
		switch resErr.Code() {
		case t_api.ErrAPISubmissionQueueFull:
			return nil
		case t_api.ErrAIOSubmissionQueueFull:
			return nil
		case t_api.ErrAIOStoreFailure:
			return nil
		default:
			return fmt.Errorf("unexpected resonate error '%v'", resErr)
		}
	}

	if req.Kind != res.Kind {
		return fmt.Errorf("unexpected response kind '%d' for request kind '%d'", res.Kind, req.Kind)
	}

	if f, ok := m.responses[req.Kind]; ok {
		return f(t, req, res)
	}

	return fmt.Errorf("unexpected request/response kind '%d'", req.Kind)
}

// PROMISES

func (m *Model) ValidateReadPromise(t int64, req *t_api.Request, res *t_api.Response) error {
	pm, err := m.promises.Get(req.ReadPromise.Id)

	if err != nil {
		return err
	}

	switch res.ReadPromise.Status {
	case t_api.StatusOK:
		if pm.completed() && res.ReadPromise.Promise.State == promise.Pending {
			return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, res.ReadPromise.Promise.State)
		}

		// update model state
		pm.promise = res.ReadPromise.Promise
		return nil
	case t_api.StatusPromiseNotFound:
		if pm.promise != nil {
			return fmt.Errorf("promise exists %s", pm.promise)
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.ReadPromise.Status)
	}
}

func (m *Model) ValidateSearchPromises(t int64, req *t_api.Request, res *t_api.Response) error {
	if res.SearchPromises.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:           t_api.SearchPromises,
			SearchPromises: res.SearchPromises.Cursor.Next,
		})
	}

	switch res.SearchPromises.Status {
	case t_api.StatusOK:
		regex := regexp.MustCompile(fmt.Sprintf("^%s$", strings.ReplaceAll(req.SearchPromises.Id, "*", ".*")))

		for _, p := range res.SearchPromises.Promises {
			pm, err := m.promises.Get(p.Id)
			if err != nil {
				return err
			}

			states := map[promise.State]bool{}
			for _, state := range req.SearchPromises.States {
				states[state] = true
			}

			if !regex.MatchString(p.Id) {
				return fmt.Errorf("promise id '%s' does not match search query '%s'", p.Id, req.SearchPromises.Id)
			}
			if _, ok := states[p.State]; !ok {
				return fmt.Errorf("unexpected state %s, searched for %s", p.State, req.SearchPromises.States)
			}
			if req.SearchPromises.SortId != nil && *req.SearchPromises.SortId <= p.SortId {
				return fmt.Errorf("unexpected sortId, promise sortId %d is greater than the request sortId %d", *req.SearchPromises.SortId, p.SortId)
			}
			if pm.completed() && p.State == promise.Pending {
				return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, p.State)
			}
			for k, v := range req.SearchPromises.Tags {
				if _v, ok := p.Tags[k]; !ok || v != _v {
					return fmt.Errorf("unexpected tag '%s:%s', expected '%s:%s'", k, _v, k, v)
				}
			}

			// update model state
			pm.promise = p
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.SearchPromises.Status)
	}
}

func (m *Model) ValidateCreatePromise(t int64, req *t_api.Request, res *t_api.Response) error {
	pm, err := m.promises.Get(req.CreatePromise.Id)
	if err != nil {
		return err
	}

	switch res.CreatePromise.Status {
	case t_api.StatusOK:
		if pm.promise != nil {
			if !pm.idempotencyKeyForCreateMatch(res.CreatePromise.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", pm.promise.IdempotencyKeyForCreate, res.CreatePromise.Promise.IdempotencyKeyForCreate)
			} else if req.CreatePromise.Strict && pm.promise.State != promise.Pending {
				return fmt.Errorf("unexpected state %s when strict true", pm.promise.State)
			}
		}

		// Update task state.
		router := queuing.CoroutineRouter()
		_, err := router.Match(req.CreatePromise.Id)
		if err != nil {
			if !errors.Is(err, queuing.ErrRouteDoesNotMatchAnyPattern) {
				return err
			}
		}
		if err == nil {
			tm, err := m.tasks.Get(req.CreatePromise.Id)
			if err != nil {
				return err
			}
			tm.task = &task.Task{
				Id:             req.CreatePromise.Id,
				Counter:        1, // updated, first tested.
				PromiseId:      req.CreatePromise.Id,
				ClaimTimeout:   *res.CreatePromise.Promise.CreatedOn,
				PromiseTimeout: req.CreatePromise.Timeout,
				CreatedOn:      *res.CreatePromise.Promise.CreatedOn,
			}

		}

		// Update model state
		pm.promise = res.CreatePromise.Promise
		return nil
	case t_api.StatusCreated:
		if res.CreatePromise.Promise.State != promise.Pending {
			return fmt.Errorf("unexpected state %s after create promise", res.CreatePromise.Promise.State)
		}
		if pm.promise != nil {
			return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, promise.Pending)
		}

		// update model state
		pm.promise = res.CreatePromise.Promise
		return nil
	case t_api.StatusPromiseAlreadyExists:
		return nil
	case t_api.StatusPromiseNotFound:
		return fmt.Errorf("invalid response '%d' for create promise request", res.CreatePromise.Status)
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.CreatePromise.Status)
	}
}

func (m *Model) ValidateCompletePromise(t int64, req *t_api.Request, res *t_api.Response) error {
	pm, err := m.promises.Get(req.CompletePromise.Id)
	if err != nil {
		return err
	}

	switch res.CompletePromise.Status {
	case t_api.StatusOK:
		if pm.completed() {
			if !pm.idempotencyKeyForCompleteMatch(res.CompletePromise.Promise) &&
				(req.CompletePromise.Strict || pm.promise.State != promise.Timedout) {
				return fmt.Errorf("ikey mismatch (%s, %s)", pm.promise.IdempotencyKeyForComplete, res.CompletePromise.Promise.IdempotencyKeyForComplete)
			} else if req.CompletePromise.Strict && pm.promise.State != req.CompletePromise.State {
				return fmt.Errorf("unexpected state %s when strict true", pm.promise.State)
			}
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.CompletePromise.Promise
		return nil
	case t_api.StatusCreated:
		if req.CompletePromise.State == promise.Resolved && res.CompletePromise.Promise.State != promise.Resolved {
			return fmt.Errorf("unexpected state %s after resolve promise", res.CompletePromise.Promise.State)
		}
		if req.CompletePromise.State == promise.Rejected && res.CompletePromise.Promise.State != promise.Rejected {
			return fmt.Errorf("unexpected state %s after reject promise", res.CompletePromise.Promise.State)
		}
		if req.CompletePromise.State == promise.Canceled && res.CompletePromise.Promise.State != promise.Canceled {
			return fmt.Errorf("unexpected state %s after cancel promise", res.CompletePromise.Promise.State)
		}
		if pm.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, promise.Canceled)
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.CompletePromise.Promise
		return nil
	case t_api.StatusPromiseAlreadyResolved, t_api.StatusPromiseAlreadyRejected, t_api.StatusPromiseAlreadyCanceled, t_api.StatusPromiseAlreadyTimedOut:
		return nil
	case t_api.StatusPromiseNotFound:
		if pm.promise != nil {
			return fmt.Errorf("promise exists %s", pm.promise)
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.CompletePromise.Status)
	}
}

// SCHEDULES

func (m *Model) ValidateReadSchedule(t int64, req *t_api.Request, res *t_api.Response) error {
	sm, err := m.schedules.Get(req.ReadSchedule.Id)

	if err != nil {
		return err
	}

	switch res.ReadSchedule.Status {
	case t_api.StatusOK:
		s := res.ReadSchedule.Schedule // schedule response

		if s.NextRunTime < sm.schedule.NextRunTime {
			return fmt.Errorf("unexpected nextRunTime, schedule nextRunTime %d is greater than the request nextRunTime %d", s.NextRunTime, sm.schedule.NextRunTime)
		}
		if (s.LastRunTime != nil && sm.schedule.LastRunTime != nil) && *s.LastRunTime < *sm.schedule.LastRunTime {
			return fmt.Errorf("unexpected lastRunTime, schedule lastRunTime %d is greater than the request lastRunTime %d", s.LastRunTime, sm.schedule.LastRunTime)
		}

		sm.schedule = s
		return nil
	case t_api.StatusScheduleNotFound:
		if sm.schedule != nil {
			return fmt.Errorf("schedule exists %s", sm.schedule)
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.ReadSchedule.Status)
	}
}

func (m *Model) ValidateSearchSchedules(t int64, req *t_api.Request, res *t_api.Response) error {
	if res.SearchSchedules.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:            t_api.SearchSchedules,
			SearchSchedules: res.SearchSchedules.Cursor.Next,
		})
	}

	switch res.SearchSchedules.Status {
	case t_api.StatusOK:
		regex := regexp.MustCompile(fmt.Sprintf("^%s$", strings.ReplaceAll(req.SearchSchedules.Id, "*", ".*")))

		for _, s := range res.SearchSchedules.Schedules {
			sm, err := m.schedules.Get(s.Id)

			if err != nil {
				return err
			}

			if !regex.MatchString(s.Id) {
				return fmt.Errorf("schedule id '%s' does not match search query '%s'", s.Id, req.SearchSchedules.Id)
			}
			if req.SearchSchedules.SortId != nil && *req.SearchSchedules.SortId <= s.SortId {
				return fmt.Errorf("unexpected sortId, schedule sortId %d is greater than the request sortId %d", *req.SearchSchedules.SortId, s.SortId)
			}
			for k, v := range req.SearchSchedules.Tags {
				if _v, ok := s.Tags[k]; !ok || v != _v {
					return fmt.Errorf("unexpected tag '%s:%s', expected '%s:%s'", k, _v, k, v)
				}
			}

			if s.NextRunTime < sm.schedule.NextRunTime {
				return fmt.Errorf("unexpected nextRunTime, schedule nextRunTime %d is greater than the request nextRunTime %d", s.NextRunTime, sm.schedule.NextRunTime)
			}
			if (s.LastRunTime != nil && sm.schedule.LastRunTime != nil) && *s.LastRunTime < *sm.schedule.LastRunTime {
				return fmt.Errorf("unexpected lastRunTime, schedule lastRunTime %d is greater than the request lastRunTime %d", s.LastRunTime, sm.schedule.LastRunTime)
			}

			// update schedule state
			sm.schedule = s
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.SearchPromises.Status)
	}
}

func (m *Model) ValidateCreateSchedule(t int64, req *t_api.Request, res *t_api.Response) error {
	sm, err := m.schedules.Get(req.CreateSchedule.Id)

	if err != nil {
		return err
	}

	switch res.CreateSchedule.Status {
	case t_api.StatusOK:
		if sm.schedule != nil {
			if !sm.idempotencyKeyMatch(res.CreateSchedule.Schedule) {
				return fmt.Errorf("ikey mismatch (%s, %s)", sm.schedule.IdempotencyKey, res.CreateSchedule.Schedule.IdempotencyKey)
			}
		}
		sm.schedule = res.CreateSchedule.Schedule
		return nil
	case t_api.StatusCreated:
		sm.schedule = res.CreateSchedule.Schedule
		return nil
	case t_api.StatusScheduleAlreadyExists:
		return nil
	case t_api.StatusScheduleNotFound:
		return fmt.Errorf("invalid response '%d' for create schedule request", res.CreateSchedule.Status)
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.CreateSchedule.Status)
	}
}

func (m *Model) ValidateDeleteSchedule(t int64, req *t_api.Request, res *t_api.Response) error {
	sm, err := m.schedules.Get(req.DeleteSchedule.Id)

	if err != nil {
		return err
	}

	switch res.DeleteSchedule.Status {
	case t_api.StatusNoContent:
		sm.schedule = nil
		return nil
	case t_api.StatusScheduleNotFound:
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.DeleteSchedule.Status)
	}
}

// SUBSCRIPTIONS

func (m *Model) ValidateReadSubscriptions(t int64, req *t_api.Request, res *t_api.Response) error {
	if res.ReadSubscriptions.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:              t_api.ReadSubscriptions,
			ReadSubscriptions: res.ReadSubscriptions.Cursor.Next,
		})
	}

	switch res.ReadSubscriptions.Status {
	case t_api.StatusOK:
		for _, s := range res.ReadSubscriptions.Subscriptions {
			pm, err := m.promises.Get(s.PromiseId)
			if err != nil {
				return err
			}
			sm, err := pm.subscriptions.Get(s.Id)
			if err != nil {
				return err
			}

			if req.ReadSubscriptions.SortId != nil && *req.ReadSubscriptions.SortId <= s.SortId {
				return fmt.Errorf("unexpected sortId, promise sortId %d is greater than the request sortId %d", *req.ReadSubscriptions.SortId, s.SortId)
			}

			// update model state
			sm.subscription = s
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.SearchPromises.Status)
	}
}

func (m *Model) ValidateCreateSubscription(t int64, req *t_api.Request, res *t_api.Response) error {
	pm, err := m.promises.Get(req.CreateSubscription.PromiseId)
	if err != nil {
		return err
	}
	sm, err := pm.subscriptions.Get(req.CreateSubscription.Id)
	if err != nil {
		return err
	}

	switch res.CreateSubscription.Status {
	case t_api.StatusOK:
		sm.subscription = res.CreateSubscription.Subscription
		return nil
	case t_api.StatusCreated:
		sm.subscription = res.CreateSubscription.Subscription
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.CreateSubscription.Status)
	}
}

func (m *Model) ValidateDeleteSubscription(t int64, req *t_api.Request, res *t_api.Response) error {
	pm, err := m.promises.Get(req.DeleteSubscription.PromiseId)
	if err != nil {
		return err
	}
	sm, err := pm.subscriptions.Get(req.DeleteSubscription.Id)
	if err != nil {
		return err
	}

	switch res.DeleteSubscription.Status {
	case t_api.StatusNoContent:
		sm.subscription = nil
		return nil
	case t_api.StatusSubscriptionNotFound:
		sm.subscription = nil
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.DeleteSubscription.Status)
	}
}

// LOCKS

func (m *Model) ValidateAcquireLock(t int64, req *t_api.Request, res *t_api.Response) error {
	lm, err := m.locks.Get(req.AcquireLock.ResourceId)
	if err != nil {
		return err
	}

	switch res.AcquireLock.Status {
	case t_api.StatusCreated:
		lm.lock = res.AcquireLock.Lock
		return nil
	case t_api.StatusLockAlreadyAcquired:
		if lm.lock == nil {
			return fmt.Errorf("lock %s does not exist", req.AcquireLock.ResourceId)
		}
		if lm.lock.ExecutionId == req.AcquireLock.ExecutionId {
			return fmt.Errorf("lock %s already acquired by executionId %s", req.AcquireLock.ResourceId, req.AcquireLock.ExecutionId)
		}
		return nil
	default:
		return fmt.Errorf("unexpected response status '%d'", res.AcquireLock.Status)
	}
}

func (m *Model) ValidateHeartbeatLocks(t int64, req *t_api.Request, res *t_api.Response) error {
	switch res.HeartbeatLocks.Status {
	case t_api.StatusOK:
		if res.HeartbeatLocks.LocksAffected == 0 {
			return nil
		}

		var count int64
		for _, l := range m.locks {
			if l.lock == nil {
				continue
			}
			if l.lock.ProcessId == req.HeartbeatLocks.ProcessId {
				count++
			}
		}

		if res.HeartbeatLocks.LocksAffected != count {
			// best we can do.
			return nil
		}

		// update local model for processId's locks
		for _, l := range m.locks {
			if l.lock == nil {
				continue
			}
			if l.lock.ProcessId == req.HeartbeatLocks.ProcessId {
				// update local model for processId's locks
				owned, err := m.locks.Get(l.lock.ResourceId)
				if err != nil {
					return err
				}
				owned.lock.ExpiresAt = owned.lock.ExpiresAt + (owned.lock.ExpiryInSeconds * 1000)
			}
		}

		return nil
	default:
		return fmt.Errorf("unexpected response status '%d'", res.HeartbeatLocks.Status)
	}
}

func (m *Model) ValidateReleaseLock(t int64, req *t_api.Request, res *t_api.Response) error {
	lm, err := m.locks.Get(req.ReleaseLock.ResourceId)
	if err != nil {
		return err
	}

	switch res.ReleaseLock.Status {
	case t_api.StatusNoContent:
		if lm.lock == nil {
			return fmt.Errorf("lock %s does not exist", req.ReleaseLock.ResourceId)
		}
		lm.lock = nil
		return nil
	case t_api.StatusLockNotFound:
		if lm.lock != nil {
			if lm.lock.ExecutionId != req.ReleaseLock.ExecutionId {
				return nil
			}

			// if lock belongs to the same executionId it must have timedout.
			if lm.lock.ExpiresAt > t {
				return fmt.Errorf("executionId %s still has the lock for resourceId %s", req.ReleaseLock.ExecutionId, req.ReleaseLock.ResourceId)
			}
			lm.lock = nil
		}

		// ok cause lock does not exist at all for this resourceId.
		return nil
	default:
		return fmt.Errorf("unexpected response status '%d'", res.ReleaseLock.Status)
	}
}

// TASKS

func (m *Model) ValidateClaimTask(t int64, req *t_api.Request, res *t_api.Response) error {
	tm, err := m.tasks.Get(req.ClaimTask.TaskId)
	if err != nil {
		return err
	}

	switch res.ClaimTask.Status {
	case t_api.StatusTaskNotFound:
		if tm.task != nil {
			return fmt.Errorf("task exists %s", tm.task)
		}
		return nil
	// Can't test this because the task is not created in the model cause of the timeout stuff.
	case t_api.StatusLockAlreadyAcquired:
		return nil
	case t_api.StatusTaskAlreadyCompleted:
		if !tm.task.IsCompleted {
			return fmt.Errorf("task %s is not completed", tm.task)
		}
		return nil
	case t_api.StatusTaskWrongCounter:
		if tm.task.Counter == req.CompleteTask.Counter {
			return fmt.Errorf("task counter %d matches request counter %d", tm.task.Counter, req.CompleteTask.Counter)
		}
		return nil
	case t_api.StatusTaskAlreadyTimedOut:
		if tm.task.PromiseTimeout > t {
			return fmt.Errorf("task %s has not yet timed out", tm.task)
		}
		return nil
	case t_api.StatusOK:
		return nil
	default:
		return fmt.Errorf("unexpected response status '%d'", res.ClaimTask.Status)
	}
}

func (m *Model) ValidateCompleteTask(t int64, req *t_api.Request, res *t_api.Response) error {
	tm, err := m.tasks.Get(req.CompleteTask.TaskId)
	if err != nil {
		return err
	}

	switch res.CompleteTask.Status {
	case t_api.StatusTaskNotFound:
		if tm.task != nil {
			return fmt.Errorf("task exists %s", tm.task)
		}
		return nil
	case t_api.StatusTaskAlreadyCompleted:
		if !tm.task.IsCompleted {
			return fmt.Errorf("task %s is not completed", tm.task)
		}
		return nil
	case t_api.StatusTaskWrongCounter:
		if tm.task.Counter == req.CompleteTask.Counter {
			return fmt.Errorf("task counter %d matches request counter %d", tm.task.Counter, req.CompleteTask.Counter)
		}
		return nil
	case t_api.StatusTaskAlreadyTimedOut:
		if tm.task.PromiseTimeout > t {
			return fmt.Errorf("task %s has not yet timed out", tm.task)
		}
		return nil
	case t_api.StatusOK:
		tm.task.IsCompleted = true
		return nil
	default:
		return fmt.Errorf("unexpected response status '%d'", res.ClaimTask.Status)
	}
}

// UTILS

func (m *PromiseModel) idempotencyKeyForCreateMatch(promise *promise.Promise) bool {
	return m.promise.IdempotencyKeyForCreate != nil && promise.IdempotencyKeyForCreate != nil && *m.promise.IdempotencyKeyForCreate == *promise.IdempotencyKeyForCreate
}

func (m *PromiseModel) idempotencyKeyForCompleteMatch(promise *promise.Promise) bool {
	return m.promise.IdempotencyKeyForComplete != nil && promise.IdempotencyKeyForComplete != nil && *m.promise.IdempotencyKeyForComplete == *promise.IdempotencyKeyForComplete
}

func (m *PromiseModel) completed() bool {
	return m.promise != nil && m.promise.State != promise.Pending
}

func (m *ScheduleModel) idempotencyKeyMatch(schedule *schedule.Schedule) bool {
	return m.schedule.IdempotencyKey != nil && schedule.IdempotencyKey != nil && *m.schedule.IdempotencyKey == *schedule.IdempotencyKey
}
