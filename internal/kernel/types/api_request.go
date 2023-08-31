package types

import (
	"fmt"

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
	ReadSubscriptions  *ReadSubscriptionsRequest
	CreateSubscription *CreateSubscriptionRequest
	DeleteSubscription *DeleteSubscriptionRequest
}

type ReadPromiseRequest struct {
	Id string `json:"id"`
}

type SearchPromisesRequest struct {
	Q     string        `json:"q"`
	State promise.State `json:"state"`
}

type CreatePromiseRequest struct {
	Id            string                       `json:"id"`
	Param         promise.Value                `json:"param,omitempty"`
	Timeout       int64                        `json:"timeout"`
	Subscriptions []*CreateSubscriptionRequest `json:"subscriptions,omitempty"`
	Tags          map[string]string            `json:"tags,omitempty"`
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

type ReadSubscriptionsRequest struct {
	PromiseId string `json:"promiseId"`
}

type CreateSubscriptionRequest struct {
	PromiseId   string                    `json:"promiseId"`
	Url         string                    `json:"url"`
	RetryPolicy *subscription.RetryPolicy `json:"retryPolicy"`
}

type DeleteSubscriptionRequest struct {
	Id int64 `json:"id"`
}

func (r *Request) String() string {
	switch r.Kind {
	case ReadPromise:
		return fmt.Sprintf(
			"ReadPromise(id=%s)",
			r.ReadPromise.Id,
		)
	case SearchPromises:
		return fmt.Sprintf(
			"SearchPromises(q=%s)",
			r.SearchPromises.Q,
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
	case ReadSubscriptions:
		return fmt.Sprintf(
			"ReadSubscriptions(promiseId=%s)",
			r.ReadSubscriptions.PromiseId,
		)
	case CreateSubscription:
		return fmt.Sprintf(
			"CreateSubscription(promiseId=%s, url=%s)",
			r.CreateSubscription.PromiseId,
			r.CreateSubscription.Url,
		)
	case DeleteSubscription:
		return fmt.Sprintf(
			"DeleteSubscription(id=%d)",
			r.DeleteSubscription.Id,
		)
	default:
		return "Request"
	}
}
