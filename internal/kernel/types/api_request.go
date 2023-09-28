package types

import (
	"fmt"
	"strconv"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Request struct {
	Kind               APIKind
	ReadPromise        *ReadPromiseRequest
	SearchPromises     *SearchPromisesRequest
	CreatePromise      *CreatePromiseRequest
	CancelPromise      *CancelPromiseRequest
	ResolvePromise     *ResolvePromiseRequest
	RejectPromise      *RejectPromiseRequest
	CompletePromise    *CompletePromiseRequest
	ReadSubscriptions  *ReadSubscriptionsRequest
	CreateSubscription *CreateSubscriptionRequest
	DeleteSubscription *DeleteSubscriptionRequest
}

type ReadPromiseRequest struct {
	Id string `json:"id"`
}

type SearchPromisesRequest struct {
	Q      string          `json:"q"`
	States []promise.State `json:"states"`
	Limit  int             `json:"limit"`
	SortId *int64          `json:"sortId"`
}

type CreatePromiseRequest struct {
	Id      string        `json:"id"`
	Param   promise.Value `json:"param,omitempty"`
	Timeout int64         `json:"timeout"`
	// Subscriptions []*CreateSubscriptionRequest `json:"subscriptions,omitempty"`
	Tags map[string]string `json:"tags,omitempty"`
}

type CancelPromiseRequest struct {
	Id    string        `json:"id"`
	Value promise.Value `json:"value,omitempty"`
}

type ResolvePromiseRequest struct {
	Id    string        `json:"id"`
	Value promise.Value `json:"value,omitempty"`
}

type RejectPromiseRequest struct {
	Id    string        `json:"id"`
	Value promise.Value `json:"value,omitempty"`
}

type CompletePromiseRequest struct {
	Id    string        `json:"id"`
	Value promise.Value `json:"value,omitempty"`
	State promise.State `json:"state"`
}

type ReadSubscriptionsRequest struct {
	PromiseId string `json:"promiseId"`
	Limit     int    `json:"limit"`
	SortId    *int64 `json:"sortId"`
}

type CreateSubscriptionRequest struct {
	Id          string                    `json:"id"`
	PromiseId   string                    `json:"promiseId"`
	Url         string                    `json:"url"`
	RetryPolicy *subscription.RetryPolicy `json:"retryPolicy"`
}

type DeleteSubscriptionRequest struct {
	Id        string `json:"id"`
	PromiseId string `json:"promiseId"`
}

func (r *Request) String() string {
	switch r.Kind {
	case ReadPromise:
		return fmt.Sprintf(
			"ReadPromise(id=%s)",
			r.ReadPromise.Id,
		)
	case SearchPromises:
		sortId := "<nil>"
		if r.SearchPromises.SortId != nil {
			sortId = strconv.FormatInt(*r.SearchPromises.SortId, 10)
		}

		return fmt.Sprintf(
			"SearchPromises(q=%s, states=%s, limit=%d, sortId=%s)",
			r.SearchPromises.Q,
			r.SearchPromises.States,
			r.SearchPromises.Limit,
			sortId,
		)
	case CreatePromise:
		return fmt.Sprintf(
			"CreatePromise(id=%s, ikey=%s, timeout=%d)",
			r.CreatePromise.Id,
			r.CreatePromise.Param.Ikey,
			r.CreatePromise.Timeout,
		)
	case CancelPromise:
		return fmt.Sprintf(
			"CancelPromise(id=%s, ikey=%s)",
			r.CancelPromise.Id,
			r.CancelPromise.Value.Ikey,
		)
	case ResolvePromise:
		return fmt.Sprintf(
			"ResolvePromise(id=%s, ikey=%s)",
			r.ResolvePromise.Id,
			r.ResolvePromise.Value.Ikey,
		)
	case RejectPromise:
		return fmt.Sprintf(
			"RejectPromise(id=%s, ikey=%s)",
			r.RejectPromise.Id,
			r.RejectPromise.Value.Ikey,
		)
	case CompletePromise:
		return fmt.Sprintf(
			"CompletePromise(id=%s, ikey=%s, state=%s)",
			r.CompletePromise.Id,
			r.CompletePromise.Value.Ikey,
			r.CompletePromise.State,
		)
	case ReadSubscriptions:
		sortId := "<nil>"
		if r.ReadSubscriptions.SortId != nil {
			sortId = strconv.FormatInt(*r.ReadSubscriptions.SortId, 10)
		}

		return fmt.Sprintf(
			"ReadSubscriptions(promiseId=%s, limit=%d, sortId=%s)",
			r.ReadSubscriptions.PromiseId,
			r.ReadSubscriptions.Limit,
			sortId,
		)
	case CreateSubscription:
		return fmt.Sprintf(
			"CreateSubscription(id=%s, promiseId=%s, url=%s)",
			r.CreateSubscription.Id,
			r.CreateSubscription.PromiseId,
			r.CreateSubscription.Url,
		)
	case DeleteSubscription:
		return fmt.Sprintf(
			"DeleteSubscription(id=%s, promiseId=%s)",
			r.DeleteSubscription.Id,
			r.DeleteSubscription.PromiseId,
		)
	default:
		return "Request"
	}
}
