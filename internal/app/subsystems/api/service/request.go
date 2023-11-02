package service

import "github.com/resonatehq/resonate/pkg/promise"

type ValidationError struct {
	msg string // description of error
}

type Header struct {
	RequestId string `header:"request-id"`
}

type SearchPromiseParams struct {
	Q      string `form:"q" json:"q"`
	State  string `form:"state" json:"state"`
	Limit  int    `form:"limit" json:"limit"`
	Cursor string `form:"cursor" json:"cursor"`
}

type CreatePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type CreatePromiseBody struct {
	Param   promise.Value     `json:"param"`
	Timeout int64             `json:"timeout"`
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
