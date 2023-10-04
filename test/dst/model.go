package dst

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Model struct {
	promises  Promises
	responses map[t_api.Kind]ResponseValidator
	cursors   []*t_api.Request
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

func (m *Model) Step(req *t_api.Request, res *t_api.Response, err error) error {
	if err != nil {
		switch err.Error() {
		case "api submission queue full":
			return nil
		case "subsystem:store:sqlite submission queue full":
			return nil
		case "subsystem:network:dst submission queue full":
			return nil
		default:
			return fmt.Errorf("unexpected error '%v'", err)
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
	return pm.readPromise(req.ReadPromise, res.ReadPromise)
}

func (m *Model) ValidateSearchPromises(req *t_api.Request, res *t_api.Response) error {
	if res.SearchPromises.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:           t_api.SearchPromises,
			SearchPromises: res.SearchPromises.Cursor.Next,
		})
	}

	regex := regexp.MustCompile(fmt.Sprintf("^%s$", strings.ReplaceAll(req.SearchPromises.Q, "*", ".*")))

	states := map[promise.State]bool{}
	for _, state := range req.SearchPromises.States {
		states[state] = true
	}

	for _, p := range res.SearchPromises.Promises {
		if !regex.MatchString(p.Id) {
			return fmt.Errorf("promise id '%s' does not match search query '%s'", p.Id, req.SearchPromises.Q)
		}

		if _, ok := states[p.State]; !ok {
			return fmt.Errorf("unexpected state %s, searched for %s", p.State, req.SearchPromises.States)
		}

		if req.SearchPromises.SortId != nil && *req.SearchPromises.SortId <= p.SortId {
			return fmt.Errorf("unexpected sortId, promise sortId %d is greater than the request sortId %d", *req.SearchPromises.SortId, p.SortId)
		}

		pm := m.promises.Get(p.Id)
		if err := pm.searchPromise(req.SearchPromises, res.SearchPromises, p); err != nil {
			return err
		}
	}
	return nil
}

func (m *Model) ValidatCreatePromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.CreatePromise.Id)
	return pm.createPromise(req.CreatePromise, res.CreatePromise)
}

func (m *Model) ValidateCancelPromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.CancelPromise.Id)
	return pm.cancelPromise(req.CancelPromise, res.CancelPromise)
}

func (m *Model) ValidateResolvePromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.ResolvePromise.Id)
	return pm.resolvePromise(req.ResolvePromise, res.ResolvePromise)
}

func (m *Model) ValidateRejectPromise(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.RejectPromise.Id)
	return pm.rejectPromise(req.RejectPromise, res.RejectPromise)
}

func (m *Model) ValidateReadSubscriptions(req *t_api.Request, res *t_api.Response) error {
	if res.ReadSubscriptions.Cursor != nil {
		m.addCursor(&t_api.Request{
			Kind:              t_api.ReadSubscriptions,
			ReadSubscriptions: res.ReadSubscriptions.Cursor.Next,
		})
	}

	pm := m.promises.Get(req.ReadSubscriptions.PromiseId)
	return pm.readSubscriptions(req.ReadSubscriptions, res.ReadSubscriptions)
}

func (m *Model) ValidateCreateSubscription(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.CreateSubscription.PromiseId)
	return pm.createSubscription(req.CreateSubscription, res.CreateSubscription)
}

func (m *Model) ValidateDeleteSubscription(req *t_api.Request, res *t_api.Response) error {
	pm := m.promises.Get(req.DeleteSubscription.PromiseId)
	return pm.deleteSubscription(req.DeleteSubscription, res.DeleteSubscription)
}

type Promises map[string]*PromiseModel

func (p Promises) Get(id string) *PromiseModel {
	if _, ok := p[id]; !ok {
		p[id] = &PromiseModel{id: id}
	}

	return p[id]
}

type ResponseValidator func(*t_api.Request, *t_api.Response) error

type PromiseModel struct {
	id            string
	promise       *promise.Promise
	subscriptions []*subscription.Subscription
}

func (m *PromiseModel) readPromise(req *t_api.ReadPromiseRequest, res *t_api.ReadPromiseResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
		if m.completed() && res.Promise.State == promise.Pending {
			return fmt.Errorf("invalid state transition (%s -> %s)", m.promise.State, res.Promise.State)
		}
	case t_api.ResponseCreated:
		return fmt.Errorf("invalid response '%d' for read promise request", res.Status)
	case t_api.ResponseForbidden:
		return fmt.Errorf("invalid response '%d' for read promise request", res.Status)
	case t_api.ResponseNotFound:
		if m.promise != nil {
			return fmt.Errorf("promise exists %s", m.promise)
		}
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifyPromise(res.Promise)
}

func (m *PromiseModel) searchPromise(req *t_api.SearchPromisesRequest, res *t_api.SearchPromisesResponse, p *promise.Promise) error {
	switch res.Status {
	case t_api.ResponseOK:
		if m.completed() && p.State == promise.Pending {
			return fmt.Errorf("invalid state transition (%s -> %s)", m.promise.State, p.State)
		}
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifyPromise(p)
}

func (m *PromiseModel) createPromise(req *t_api.CreatePromiseRequest, res *t_api.CreatePromiseResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
		if m.promise != nil {
			if !m.idempotencyKeyForCreateMatch(res.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", m.promise.IdempotencyKeyForCreate, res.Promise.IdempotencyKeyForCreate)
			} else if req.Strict && m.promise.State != promise.Pending {
				return fmt.Errorf("unexpected state %s when strict true", m.promise.State)
			}
		}
	case t_api.ResponseCreated:
		if m.promise != nil {
			return fmt.Errorf("invalid state transition (%s -> %s)", m.promise.State, promise.Pending)
		}
	case t_api.ResponseForbidden:
	case t_api.ResponseNotFound:
		return fmt.Errorf("invalid response '%d' for create promise request", res.Status)
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifyPromise(res.Promise)
}

func (m *PromiseModel) cancelPromise(req *t_api.CancelPromiseRequest, res *t_api.CancelPromiseResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
		if m.completed() {
			if !m.idempotencyKeyForCompleteMatch(res.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", m.promise.IdempotencyKeyForComplete, res.Promise.IdempotencyKeyForComplete)
			} else if req.Strict && m.promise.State != promise.Canceled {
				return fmt.Errorf("unexpected state %s when strict true", m.promise.State)
			}
		}
	case t_api.ResponseCreated:
		if m.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", m.promise.State, promise.Canceled)
		}
	case t_api.ResponseForbidden:
	case t_api.ResponseNotFound:
		if m.promise != nil {
			return fmt.Errorf("promise exists %s", m.promise)
		}
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifyPromise(res.Promise)
}

func (m *PromiseModel) resolvePromise(req *t_api.ResolvePromiseRequest, res *t_api.ResolvePromiseResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
		if m.completed() {
			if !m.idempotencyKeyForCompleteMatch(res.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", m.promise.IdempotencyKeyForComplete, res.Promise.IdempotencyKeyForComplete)
			} else if req.Strict && m.promise.State != promise.Resolved {
				return fmt.Errorf("unexpected state %s when strict true", m.promise.State)
			}
		}
	case t_api.ResponseCreated:
		if m.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", m.promise.State, promise.Resolved)
		}
	case t_api.ResponseForbidden:
	case t_api.ResponseNotFound:
		if m.promise != nil {
			return fmt.Errorf("promise exists %s", m.promise)
		}
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifyPromise(res.Promise)
}

func (m *PromiseModel) rejectPromise(req *t_api.RejectPromiseRequest, res *t_api.RejectPromiseResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
		if m.completed() {
			if !m.idempotencyKeyForCompleteMatch(res.Promise) {
				return fmt.Errorf("ikey mismatch (%s, %s)", m.promise.IdempotencyKeyForComplete, res.Promise.IdempotencyKeyForComplete)
			} else if req.Strict && m.promise.State != promise.Rejected {
				return fmt.Errorf("unexpected state %s when strict true", m.promise.State)
			}
		}
	case t_api.ResponseCreated:
		if m.completed() {
			return fmt.Errorf("invalid state transition (%s -> %s)", m.promise.State, promise.Rejected)
		}
	case t_api.ResponseForbidden:
	case t_api.ResponseNotFound:
		if m.promise != nil {
			return fmt.Errorf("promise exists %s", m.promise)
		}
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifyPromise(res.Promise)
}

func (m *PromiseModel) verifyPromise(p *promise.Promise) error {
	if m.promise != nil && p != nil {
		if m.promise.Id != p.Id ||
			m.promise.Timeout != p.Timeout ||
			(m.promise.IdempotencyKeyForCreate != nil && !m.idempotencyKeyForCreateMatch(p)) ||
			string(m.promise.Param.Data) != string(p.Param.Data) ||
			(m.completed() && m.promise.IdempotencyKeyForComplete != nil && !m.idempotencyKeyForCompleteMatch(p)) ||
			(m.completed() && string(m.promise.Value.Data) != string(p.Value.Data)) {

			return fmt.Errorf("promises do not match (%s, %s)", m.promise, p)
		}
	}

	// otherwise set model promise to response promise
	m.promise = p

	return nil
}

func (m *PromiseModel) readSubscriptions(req *t_api.ReadSubscriptionsRequest, res *t_api.ReadSubscriptionsResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
		for _, subscription := range res.Subscriptions {
			if req.SortId != nil && *req.SortId <= subscription.SortId {
				return fmt.Errorf("unexpected sortId, promise sortId %d is greater than the request sortId %d", *req.SortId, subscription.SortId)
			}

			if err := m.verifySubscription(subscription); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return nil
}

func (m *PromiseModel) createSubscription(req *t_api.CreateSubscriptionRequest, res *t_api.CreateSubscriptionResponse) error {
	switch res.Status {
	case t_api.ResponseOK:
	case t_api.ResponseCreated:
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return m.verifySubscription(res.Subscription)
}

func (m *PromiseModel) deleteSubscription(req *t_api.DeleteSubscriptionRequest, res *t_api.DeleteSubscriptionResponse) error {
	switch res.Status {
	case t_api.ResponseNoContent:
		for i, subscription := range m.subscriptions {
			if req.Id == subscription.Id {
				m.subscriptions = append(m.subscriptions[:i], m.subscriptions[i+1:]...)
				break
			}
		}
	case t_api.ResponseNotFound:
	default:
		return fmt.Errorf("unexpected resonse status '%d'", res.Status)
	}

	return nil
}

func (m *PromiseModel) verifySubscription(subscription *subscription.Subscription) error {
	found := false
	for _, s := range m.subscriptions {
		if s.Id == subscription.Id {
			// TODO: verify

			found = true
			break
		}
	}

	if !found {
		m.subscriptions = append(m.subscriptions, subscription)
	}

	return nil
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
