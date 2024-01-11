package service

import (
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

// PROMISES

type Header struct {
	RequestId string `header:"request-id"`
}

type SearchPromisesParams struct {
	Id     *string           `form:"id" json:"id,omitempty" binding:"omitempty,min=1"`
	State  *string           `form:"state" json:"state,omitempty" binding:"omitempty,oneofcaseinsensitive=pending resolved rejected"`
	Tags   map[string]string `form:"tags" json:"tags,omitempty"`
	Limit  *int              `form:"limit" json:"limit,omitempty" binding:"omitempty,gt=0,lte=100"`
	Cursor *string           `form:"cursor" json:"cursor,omitempty"`
}

type CreatePromiseHeader struct {
	RequestId      string           `header:"request-id"`
	IdempotencyKey *idempotency.Key `header:"idempotency-key"`
	Strict         bool             `header:"strict"`
}

type CompletePromiseHeader struct {
	RequestId      string           `header:"request-id"`
	IdempotencyKey *idempotency.Key `header:"idempotency-key,omitempty"`
	Strict         bool             `header:"strict"`
}

type CompletePromiseBody struct {
	State string        `json:"state" binding:"required,oneofcaseinsensitive=resolved rejected rejected_canceled"`
	Value promise.Value `json:"value"`
}

// SCHEDULE

type SearchSchedulesParams struct {
	Id     *string           `form:"id" json:"id,omitempty" binding:"omitempty,min=1"`
	Tags   map[string]string `form:"tags" json:"tags,omitempty"`
	Limit  *int              `form:"limit" json:"limit,omitempty" binding:"omitempty,gt=0,lte=100"`
	Cursor *string           `form:"cursor" json:"cursor,omitempty"`
}

type CreateScheduleHeader struct {
	RequestId      string           `header:"request-id"`
	IdempotencyKey *idempotency.Key `header:"idempotency-key"`
}

type CreateScheduleBody struct {
	Id             string            `json:"id" binding:"required"`
	Description    string            `json:"desc,omitempty"`
	Cron           string            `json:"cron" binding:"required"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromiseId      string            `json:"promiseId" binding:"required"`
	PromiseTimeout int64             `json:"promiseTimeout" binding:"required"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
}

// LOCKS

type AcquireLockBody struct {
	ResourceId  string `json:"resourceId" binding:"required"`
	ProcessId   string `json:"processId" binding:"required"`
	ExecutionId string `json:"executionId" binding:"required"`
	Timeout     int64  `json:"timeout" binding:"required"`
}

type BulkHeartbeatBody struct {
	ProcessId string `json:"processId" binding:"required"`
	Timeout   int64  `json:"timeout" binding:"required"`
}

type ReleaseLockBody struct {
	ResourceId  string `json:"resourceId" binding:"required"`
	ExecutionId string `json:"executionId" binding:"required"`
}
