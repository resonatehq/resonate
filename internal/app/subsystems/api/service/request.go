package service

import "github.com/resonatehq/resonate/pkg/promise"

type Header struct {
	RequestId string `header:"request-id"`
}

type SearchPromiseParams struct {
	Q      string `form:"q" json:"q" binding:"required"`
	State  string `form:"state" json:"state" binding:"oneof=pending resolved rejected"`
	Limit  int    `form:"limit" json:"limit" binding:"gte=0,lte=100"`
	Cursor string `form:"cursor" json:"cursor"`
}

type CreatePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type CreatePromiseBody struct {
	Param   *promise.Value    `json:"param" binding:"required"`
	Timeout *int64            `json:"timeout" binding:"required,gte=0"`
	Tags    map[string]string `json:"tags"`
}

type CancelPromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type CancelPromiseBody struct {
	Value promise.Value `json:"value" binding:"required"`
}

type ResolvePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type ResolvePromiseBody struct {
	Value promise.Value `json:"value" binding:"required"`
}

type RejectPromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type RejectPromiseBody struct {
	Value promise.Value `json:"value" binding:"required"`
}
