package types

import (
	"fmt"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Response struct {
	Kind               APIKind
	ReadPromise        *ReadPromiseResponse
	SearchPromises     *SearchPromisesResponse
	CreatePromise      *CreatePromiseResponse
	CancelPromise      *CancelPromiseResponse
	ResolvePromise     *ResolvePromiseResponse
	RejectPromise      *RejectPromiseResponse
	ReadSubscriptions  *ReadSubscriptionsResponse
	CreateSubscription *CreateSubscriptionResponse
	DeleteSubscription *DeleteSubscriptionResponse
}

type ResponseStatus int

const (
	ResponseOK        ResponseStatus = 200
	ResponseCreated   ResponseStatus = 201
	ResponseNoContent ResponseStatus = 204
	ResponseForbidden ResponseStatus = 403
	ResponseNotFound  ResponseStatus = 404
)

type ReadPromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type SearchPromisesResponse struct {
	Status   ResponseStatus                 `json:"status"`
	Cursor   *Cursor[SearchPromisesRequest] `json:"cursor,omitempty"`
	Promises []*promise.Promise             `json:"promises,omitempty"`
}

type CreatePromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type CancelPromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type ResolvePromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type RejectPromiseResponse struct {
	Status  ResponseStatus   `json:"status"`
	Promise *promise.Promise `json:"promise,omitempty"`
}

type ReadSubscriptionsResponse struct {
	Status        ResponseStatus                    `json:"status"`
	Cursor        *Cursor[ReadSubscriptionsRequest] `json:"cursor,omitempty"`
	Subscriptions []*subscription.Subscription      `json:"subscriptions,omitempty"`
}

type CreateSubscriptionResponse struct {
	Status       ResponseStatus             `json:"status"`
	Subscription *subscription.Subscription `json:"subscription,omitempty"`
}

type DeleteSubscriptionResponse struct {
	Status ResponseStatus `json:"status"`
}

func (r *Response) String() string {
	switch r.Kind {
	case ReadPromise:
		return fmt.Sprintf(
			"ReadPromise(status=%d, promise=%s)",
			r.ReadPromise.Status,
			r.ReadPromise.Promise,
		)
	case SearchPromises:
		return fmt.Sprintf(
			"SearchPromises(status=%d, cursor=%s, promises=%s)",
			r.SearchPromises.Status,
			r.SearchPromises.Cursor,
			r.SearchPromises.Promises,
		)
	case CreatePromise:
		return fmt.Sprintf(
			"CreatePromise(status=%d, promise=%s)",
			r.CreatePromise.Status,
			r.CreatePromise.Promise,
		)
	case CancelPromise:
		return fmt.Sprintf(
			"CancelPromise(status=%d, promise=%s)",
			r.CancelPromise.Status,
			r.CancelPromise.Promise,
		)
	case ResolvePromise:
		return fmt.Sprintf(
			"ResolvePromise(status=%d, promise=%s)",
			r.ResolvePromise.Status,
			r.ResolvePromise.Promise,
		)
	case RejectPromise:
		return fmt.Sprintf(
			"RejectPromise(status=%d, promise=%s)",
			r.RejectPromise.Status,
			r.RejectPromise.Promise,
		)
	case ReadSubscriptions:
		return fmt.Sprintf(
			"ReadSubscriptions(status=%d, subscriptions=%s)",
			r.ReadSubscriptions.Status,
			r.ReadSubscriptions.Subscriptions,
		)
	case CreateSubscription:
		return fmt.Sprintf(
			"CreateSubscription(status=%d, subscriptions=%s)",
			r.CreateSubscription.Status,
			r.CreateSubscription.Subscription,
		)
	case DeleteSubscription:
		return fmt.Sprintf(
			"DeleteSubscription(status=%d)",
			r.DeleteSubscription.Status,
		)
	default:
		return "Response"
	}
}
