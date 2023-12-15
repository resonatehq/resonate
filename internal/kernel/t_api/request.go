package t_api

import (
	"fmt"
	"strconv"

	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/subscription"
)

type Request struct {
	Kind               Kind
	ReadPromise        *ReadPromiseRequest
	SearchPromises     *SearchPromisesRequest
	CreatePromise      *CreatePromiseRequest
	CancelPromise      *CancelPromiseRequest
	ResolvePromise     *ResolvePromiseRequest
	RejectPromise      *RejectPromiseRequest
	ReadSubscriptions  *ReadSubscriptionsRequest
	CreateSubscription *CreateSubscriptionRequest
	DeleteSubscription *DeleteSubscriptionRequest
	CreateSchedule     *CreateScheduleRequest
	ReadSchedule       *ReadScheduleRequest
	DeleteSchedule     *DeleteScheduleRequest
	Echo               *EchoRequest
}

// Schedule

type CreateScheduleRequest struct {
	Id           string  `json:"id"`
	Desc         *string `json:"desc,omitempty"`
	Cron         string  `json:"cron"`
	PromiseId    string  `json:"promiseId"`
	PromiseParam *string `json:"promiseParam,omitempty"`
}

type ReadScheduleRequest struct {
	Id string `json:"id"`
}

type DeleteScheduleRequest struct {
	Id string `json:"id"`
}

// Promise

type ReadPromiseRequest struct {
	Id string `json:"id"`
}

type SearchPromisesRequest struct {
	Id     string          `json:"Id"`
	States []promise.State `json:"states"`
	Limit  int             `json:"limit"`
	SortId *int64          `json:"sortId"`
}

type CreatePromiseRequest struct {
	Id             string                  `json:"id"`
	IdempotencyKey *promise.IdempotencyKey `json:"idemptencyKey,omitempty"`
	Strict         bool                    `json:"strict"`
	Param          promise.Value           `json:"param,omitempty"`
	Timeout        int64                   `json:"timeout"`
	Tags           map[string]string       `json:"tags,omitempty"`
}

type CancelPromiseRequest struct {
	Id             string                  `json:"id"`
	IdempotencyKey *promise.IdempotencyKey `json:"idemptencyKey,omitempty"`
	Strict         bool                    `json:"strict"`
	Value          promise.Value           `json:"value,omitempty"`
}

type ResolvePromiseRequest struct {
	Id             string                  `json:"id"`
	IdempotencyKey *promise.IdempotencyKey `json:"idemptencyKey,omitempty"`
	Strict         bool                    `json:"strict"`
	Value          promise.Value           `json:"value,omitempty"`
}

type RejectPromiseRequest struct {
	Id             string                  `json:"id"`
	IdempotencyKey *promise.IdempotencyKey `json:"idemptencyKey,omitempty"`
	Strict         bool                    `json:"strict"`
	Value          promise.Value           `json:"value,omitempty"`
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

type EchoRequest struct {
	Data string `json:"data"`
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
			r.SearchPromises.Id,
			r.SearchPromises.States,
			r.SearchPromises.Limit,
			sortId,
		)
	case CreatePromise:
		return fmt.Sprintf(
			"CreatePromise(id=%s, idempotencyKey=%s, timeout=%d, strict=%t)",
			r.CreatePromise.Id,
			r.CreatePromise.IdempotencyKey,
			r.CreatePromise.Timeout,
			r.CreatePromise.Strict,
		)
	case CancelPromise:
		return fmt.Sprintf(
			"CancelPromise(id=%s, idempotencyKey=%s, strict=%t)",
			r.CancelPromise.Id,
			r.CancelPromise.IdempotencyKey,
			r.CancelPromise.Strict,
		)
	case ResolvePromise:
		return fmt.Sprintf(
			"ResolvePromise(id=%s, idempotencyKey=%s, strict=%t)",
			r.ResolvePromise.Id,
			r.ResolvePromise.IdempotencyKey,
			r.ResolvePromise.Strict,
		)
	case RejectPromise:
		return fmt.Sprintf(
			"RejectPromise(id=%s, idempotencyKey=%s, strict=%t)",
			r.RejectPromise.Id,
			r.RejectPromise.IdempotencyKey,
			r.RejectPromise.Strict,
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
	case CreateSchedule:
		return fmt.Sprintf(
			"CreateSchedule(id=%s, desc=%v, cron=%s)",
			r.CreateSchedule.Id,
			r.CreateSchedule.Desc,
			r.CreateSchedule.Cron,
		)
	case ReadSchedule:
		return fmt.Sprintf(
			"ReadSchedule(id=%s)",
			r.ReadSchedule.Id,
		)
	case DeleteSchedule:
		return fmt.Sprintf(
			"DeleteSchedule(id=%s)",
			r.DeleteSchedule.Id,
		)
	case Echo:
		return fmt.Sprintf(
			"Echo(data=%s)",
			r.Echo.Data,
		)
	default:
		return "Request"
	}
}
