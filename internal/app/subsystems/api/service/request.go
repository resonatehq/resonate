package service

import "github.com/resonatehq/resonate/pkg/promise"

type Header struct {
	RequestId string `header:"request-id"`
}

type SearchPromiseParams struct {
	Id     *string           `form:"id" json:"id,omitempty" binding:"omitempty,min=1"`
	State  string            `form:"state" json:"state,omitempty" binding:"omitempty,oneofcaseinsensitive=pending resolved rejected"`
	Tags   map[string]string `form:"tags" json:"invocation,omitempty"`
	Limit  *int              `form:"limit" json:"limit,omitempty" binding:"omitempty,gt=0,lte=100"`
	Cursor string            `form:"cursor" json:"cursor,omitempty"`
}

type CreatePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type CreatePromiseBody struct {
	Param   *promise.Value    `json:"param"`
	Timeout *int64            `json:"timeout" binding:"required,gte=0"`
	Tags    map[string]string `json:"tags"`
}

type CancelPromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type CancelPromiseBody struct {
	Value promise.Value `json:"value"`
}

type ResolvePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type ResolvePromiseBody struct {
	Value promise.Value `json:"value"`
}

type RejectPromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type RejectPromiseBody struct {
	Value promise.Value `json:"value"`
}
