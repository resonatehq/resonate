package t_api

import (
	"fmt"
	"strconv"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Response struct {
	Kind               Kind
	ReadPromise        *ReadPromiseResponse
	SearchPromises     *SearchPromisesResponse
	CreatePromise      *CreatePromiseResponse
	CancelPromise      *CancelPromiseResponse
	ResolvePromise     *ResolvePromiseResponse
	RejectPromise      *RejectPromiseResponse
	ReadSubscriptions  *ReadSubscriptionsResponse
	CreateSubscription *CreateSubscriptionResponse
	DeleteSubscription *DeleteSubscriptionResponse
	Echo               *EchoResponse
}

type ResponseStatus int

func (s ResponseStatus) String() string {
	return strconv.Itoa(int(s))
}

// TODO: DELET THESE
const (
	ResponseOK        ResponseStatus = 200
	ResponseCreated   ResponseStatus = 201
	ResponseNoContent ResponseStatus = 204
	ResponseForbidden ResponseStatus = 403
	ResponseNotFound  ResponseStatus = 404
)

// In our system, errors are separated into two categories - platform errors and application errors.
// Platform errors represent failures at the runtime level, such as database connection issues, file I/O failures,
// or network request problems.These are usually transient issues that are recoverable if retried later.
// Application errors indicate errors code specific to our business logic and use cases. This separation allows us
// to handle the two types differently - platform errors may trigger retries with backoff, while application errors
// should report immediately to the users since these failures are not typically recoverable by simply retrying.
//
// In our Go system, platform errors are represented as typical Go `error` values returned from function calls. For example:
//
// dbResult, dbErr := database.Query("SELECT...")
//
// The dbErr would contain platform errors like connection failures. While application errors are returned in
// the response object, while the `error` return is `nil`.
const (
	// Platform level errors (1000-1999)
	StatusInternalServerError           ResponseStatus = 1000 // map to 500 internal server error (for now, but should be exact)
	StatusAPISubmissionQueueFull        ResponseStatus = 1001 // map to 503 service unavailable
	StatusAIONetworkSubmissionQueueFull ResponseStatus = 1002 // map to 503 service unavailable
	StatusAIOStoreSubmissionQueueFull   ResponseStatus = 1003 // map to 503 service unavailable
	StatusSystemShuttingDown            ResponseStatus = 1004 // map to 503 service unavailable

	// Application level errors (2000-2999)
	StatusOK                     ResponseStatus = 2000 // map to 200 ok
	StatusCreated                ResponseStatus = 2001 // map to 201 created
	StatusNoContent              ResponseStatus = 2002 // map to 204 no content ( delete is special case )
	StatusPromiseAlreadyResolved ResponseStatus = 2003 // map to 403 forbidden
	StatusPromiseAlreadyRejected ResponseStatus = 2004 // map to 403 forbidden
	StatusPromiseAlreadyCanceled ResponseStatus = 2005 // map to 403 forbidden
	StatusPromiseAlreadyTimedOut ResponseStatus = 2006 // map to 403 forbidden
	StatusPromiseNotFound        ResponseStatus = 2007 // map to 404 not found
	StatusPromiseAlreadyExists   ResponseStatus = 2008 // map to 409 conflict
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

type EchoResponse struct {
	Data string `json:"data"`
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
	case Echo:
		return fmt.Sprintf(
			"Echo(data=%s)",
			r.Echo.Data,
		)
	default:
		return "Response"
	}
}
