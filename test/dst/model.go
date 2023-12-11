package dst

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

// Model

type Model struct {
	promises  Promises
	cursors   []*t_api.Request
	responses map[t_api.Kind]ResponseValidator
}

type PromiseModel struct {
	id            string
	promise       *promise.Promise
	subscriptions Subscriptions
}

type SubscriptionModel struct {
	id           string
	subscription *subscription.Subscription
}

type Promises map[string]*PromiseModel
type Subscriptions map[string]*SubscriptionModel
type ResponseValidator func(*t_api.Request, *t_api.Response) error

func (p Promises) Get(id string) *PromiseModel {
	if _, ok := p[id]; !ok {
		p[id] = &PromiseModel{
			id:            id,
			subscriptions: map[string]*SubscriptionModel{},
		}
	}

	return p[id]
}

func (s Subscriptions) Get(id string) *SubscriptionModel {
	if _, ok := s[id]; !ok {
		s[id] = &SubscriptionModel{
			id: id,
		}
	}

	return s[id]
}

func NewModel() *Model {
	return &Model{
		promises:  map[string]*PromiseModel{},
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

func (m *Model) Step(req *t_api.Request, res *t_api.Response, err error) error {
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
		default:
			return fmt.Errorf("unexpected resonate error '%v'", resErr)
		}
	}

	if req.Kind != res.Kind {
		return fmt.Errorf("unexpected response kind '%d' for request kind '%d'", res.Kind, req.Kind)
	}

	if f, ok := m.responses[req.Kind]; ok {
		return f(req, res)
	}

	return fmt.Errorf("unexpected request/response kind '%d'", req.Kind)
}

func (m *Model) ValidateReadPromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.ReadPromise.Id)

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

func (m *Model) ValidateSearchPromises(req *t_api.Request, res *t_api.Response) error {
	if res.SearchPromises.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:           t_api.SearchPromises,
			SearchPromises: res.SearchPromises.Cursor.Next,
		})
	}

	switch res.SearchPromises.Status {
	case t_api.StatusOK:
		for _, p := range res.SearchPromises.Promises {
			pm := m.promises.Get(p.Id)

			regex := regexp.MustCompile(fmt.Sprintf("^%s$", strings.ReplaceAll(req.SearchPromises.Q, "*", ".*")))

			states := map[promise.State]bool{}
			for _, state := range req.SearchPromises.States {
				states[state] = true
			}

			if !regex.MatchString(p.Id) {
				return fmt.Errorf("promise id '%s' does not match search query '%s'", p.Id, req.SearchPromises.Q)
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
			if req.SearchPromises.Invocation {
				if value, ok := p.Tags["R-Invocation"]; !ok || value != "true" {
					return fmt.Errorf("unexpected R-Invocation tag value '%s', expected 'true'", value)
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

func (m *Model) ValidatCreatePromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.CreatePromise.Id)

	switch res.CreatePromise.Status {
	case t_api.StatusOK:
		if pm.promise != nil {
			if !pm.idempotencyKeyForCreateMatch(res.CreatePromise.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", pm.promise.IdempotencyKeyForCreate, res.CreatePromise.Promise.IdempotencyKeyForCreate)
			} else if req.CreatePromise.Strict && pm.promise.State != promise.Pending {
				return fmt.Errorf("unexpected state %s when strict true", pm.promise.State)
			}
		}

		// update model state
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

func (m *Model) ValidateCancelPromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.CancelPromise.Id)

	switch res.CancelPromise.Status {
	case t_api.StatusOK:
		if pm.completed() {
			if !pm.idempotencyKeyForCompleteMatch(res.CancelPromise.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", pm.promise.IdempotencyKeyForComplete, res.CancelPromise.Promise.IdempotencyKeyForComplete)
			} else if req.CancelPromise.Strict && pm.promise.State != promise.Canceled {
				return fmt.Errorf("unexpected state %s when strict true", pm.promise.State)
			}
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.CancelPromise.Promise
		return nil
	case t_api.StatusCreated:
		if res.CancelPromise.Promise.State != promise.Canceled {
			return fmt.Errorf("unexpected state %s after cancel promise", res.CancelPromise.Promise.State)
		}
		if pm.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, promise.Canceled)
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.CancelPromise.Promise
		return nil
	case t_api.StatusPromiseAlreadyResolved, t_api.StatusPromiseAlreadyRejected, t_api.StatusPromiseAlreadyCanceled, t_api.StatusPromiseAlreadyTimedOut:
		return nil
	case t_api.StatusPromiseNotFound:
		if pm.promise != nil {
			return fmt.Errorf("promise exists %s", pm.promise)
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.CancelPromise.Status)
	}
}

func (m *Model) ValidateResolvePromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.ResolvePromise.Id)

	switch res.ResolvePromise.Status {
	case t_api.StatusOK:
		if pm.completed() {
			if !pm.idempotencyKeyForCompleteMatch(res.ResolvePromise.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", pm.promise.IdempotencyKeyForComplete, res.ResolvePromise.Promise.IdempotencyKeyForComplete)
			} else if req.ResolvePromise.Strict && pm.promise.State != promise.Resolved {
				return fmt.Errorf("unexpected state %s when strict true", pm.promise.State)
			}
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.ResolvePromise.Promise
		return nil
	case t_api.StatusCreated:
		if res.ResolvePromise.Promise.State != promise.Resolved {
			return fmt.Errorf("unexpected state %s after resolve promise", res.ResolvePromise.Promise.State)
		}
		if pm.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, promise.Resolved)
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.ResolvePromise.Promise
		return nil
	case t_api.StatusPromiseAlreadyResolved, t_api.StatusPromiseAlreadyRejected, t_api.StatusPromiseAlreadyCanceled, t_api.StatusPromiseAlreadyTimedOut:
		return nil
	case t_api.StatusPromiseNotFound:
		if pm.promise != nil {
			return fmt.Errorf("promise exists %s", pm.promise)
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.ResolvePromise.Status)
	}
}

func (m *Model) ValidateRejectPromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.RejectPromise.Id)

	switch res.RejectPromise.Status {
	case t_api.StatusOK: // dst use the 200 for idempotency,, uggghhh
		if pm.completed() {
			if !pm.idempotencyKeyForCompleteMatch(res.RejectPromise.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", pm.promise.IdempotencyKeyForComplete, res.RejectPromise.Promise.IdempotencyKeyForComplete)
			} else if req.RejectPromise.Strict && pm.promise.State != promise.Rejected {
				return fmt.Errorf("unexpected state %s when strict true", pm.promise.State)
			}
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.RejectPromise.Promise
		return nil
	case t_api.StatusCreated:
		if res.RejectPromise.Promise.State != promise.Rejected {
			return fmt.Errorf("unexpected state %s after reject promise", res.RejectPromise.Promise.State)
		}
		if pm.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", pm.promise.State, promise.Rejected)
		}

		// delete all subscriptions
		for _, sm := range pm.subscriptions {
			sm.subscription = nil
		}

		// update model state
		pm.promise = res.RejectPromise.Promise
		return nil
	case t_api.StatusPromiseAlreadyResolved, t_api.StatusPromiseAlreadyRejected, t_api.StatusPromiseAlreadyCanceled, t_api.StatusPromiseAlreadyTimedOut:
		return nil
	case t_api.StatusPromiseNotFound:
		if pm.promise != nil {
			return fmt.Errorf("promise exists %s", pm.promise)
		}
		return nil
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.RejectPromise.Status)
	}
}

func (m *Model) ValidateReadSubscriptions(req *t_api.Request, res *t_api.Response) error {
	if res.ReadSubscriptions.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:              t_api.ReadSubscriptions,
			ReadSubscriptions: res.ReadSubscriptions.Cursor.Next,
		})
	}

	switch res.ReadSubscriptions.Status {
	case t_api.StatusOK:
		for _, s := range res.ReadSubscriptions.Subscriptions {
			pm := m.promises.Get(s.PromiseId)
			sm := pm.subscriptions.Get(s.Id)

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

func (m *Model) ValidateCreateSubscription(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.CreateSubscription.PromiseId)
	sm := pm.subscriptions.Get(req.CreateSubscription.Id)

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

func (m *Model) ValidateDeleteSubscription(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.DeleteSubscription.PromiseId)
	sm := pm.subscriptions.Get(req.DeleteSubscription.Id)

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

func (m *PromiseModel) idempotencyKeyForCreateMatch(promise *promise.Promise) bool {
	return m.promise.IdempotencyKeyForCreate != nil && promise.IdempotencyKeyForCreate != nil && *m.promise.IdempotencyKeyForCreate == *promise.IdempotencyKeyForCreate
}

func (m *PromiseModel) idempotencyKeyForCompleteMatch(promise *promise.Promise) bool {
	return m.promise.IdempotencyKeyForComplete != nil && promise.IdempotencyKeyForComplete != nil && *m.promise.IdempotencyKeyForComplete == *promise.IdempotencyKeyForComplete
}

func (m *PromiseModel) completed() bool {
	return m.promise != nil && m.promise.State != promise.Pending
}
