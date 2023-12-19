package service

import (
	"github.com/resonatehq/resonate/pkg/promise"
)

// PROMISES

type Header struct {
	RequestId string `header:"request-id"`
}

type SearchPromiseParams struct {
	Id     *string `form:"id" json:"id,omitempty" binding:"omitempty,len=1"`
	State  string  `form:"state" json:"state,omitempty" binding:"omitempty,oneofcaseinsensitive=pending resolved rejected"`
	Limit  *int    `form:"limit" json:"limit,omitempty" binding:"omitempty,gt=0,lte=100"`
	Cursor string  `form:"cursor" json:"cursor,omitempty"`
}

type CreatePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type CompletePromiseHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key,omitempty"`
	Strict         bool                    `header:"strict"`
}

type CompletePromiseBody struct {
	State string        `json:"state" binding:"required,oneofcaseinsensitive=resolved rejected rejected_canceled"`
	Value promise.Value `json:"value"`
}

// SCHEDULE

type CreateScheduleHeader struct {
	RequestId      string                  `header:"request-id"`
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
}

type CreateScheduleBody struct {
	Id             string         `json:"id" binding:"required"`
	Desc           string         `json:"desc,omitempty"`
	Cron           string         `json:"cron" binding:"required"`
	PromiseId      string         `json:"promiseId" binding:"required"`
	PromiseParam   *promise.Value `json:"promiseParam,omitempty"`
	PromiseTimeout int64          `json:"promiseTimeout" binding:"required"`
}
