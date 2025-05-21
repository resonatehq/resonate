package http

import (
	"encoding/json"
	"errors"

	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"

	"github.com/gin-gonic/gin"
)

// Read

type readPromiseHeader struct {
	RequestId string `header:"request-id"`
}

func (s *server) readPromise(c *gin.Context) {
	var header readPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: &t_api.ReadPromiseRequest{
			Id: extractId(c.Param("id")),
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.ReadPromise != nil, "result must not be nil")
	c.JSON(s.code(res.ReadPromise.Status), res.ReadPromise.Promise)
}

// Search

type searchPromisesHeader struct {
	RequestId string `header:"request-id"`
}

type searchPromisesParams struct {
	Id     *string           `form:"id" json:"id" binding:"omitempty,min=1"`
	State  *string           `form:"state" json:"state" binding:"omitempty,oneofcaseinsensitive=pending resolved rejected"`
	Tags   map[string]string `form:"tags" json:"tags,omitempty"`
	Limit  *int              `form:"limit" json:"limit" binding:"omitempty,gte=0,lte=100"`
	Cursor *string           `form:"cursor" json:"cursor,omitempty"`
}

func (s *server) searchPromises(c *gin.Context) {
	var header searchPromisesHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var params searchPromisesParams
	if err := c.ShouldBindQuery(&params); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	// tags needs to be parsed manually
	// see: https://github.com/gin-gonic/gin/issues/2606
	params.Tags = c.QueryMap("tags")

	req, err := s.api.SearchPromises(
		util.SafeDeref(params.Id),
		util.SafeDeref(params.State),
		params.Tags,
		util.SafeDeref(params.Limit),
		util.SafeDeref(params.Cursor),
	)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: req,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.SearchPromises != nil, "result must not be nil")
	c.JSON(s.code(res.SearchPromises.Status), gin.H{
		"promises": res.SearchPromises.Promises,
		"cursor":   res.SearchPromises.Cursor,
	})
}

// Create

type createPromiseHeader struct {
	RequestId      string           `header:"request-id"`
	IdempotencyKey *idempotency.Key `header:"idempotency-key"`
	Strict         bool             `header:"strict"`
}

type createPromiseBody struct {
	Id      string            `json:"id" binding:"required"`
	Param   promise.Value     `json:"param"`
	Timeout int64             `json:"timeout"`
	Tags    map[string]string `json:"tags,omitempty"`
}

func (s *server) createPromise(c *gin.Context) {
	var header createPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body createPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: &t_api.CreatePromiseRequest{
			Id:             body.Id,
			IdempotencyKey: header.IdempotencyKey,
			Strict:         header.Strict,
			Param:          body.Param,
			Timeout:        body.Timeout,
			Tags:           body.Tags,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CreatePromise != nil, "result must not be nil")
	c.JSON(s.code(res.CreatePromise.Status), res.CreatePromise.Promise)
}

type createPromiseAndTaskBody struct {
	Promise createPromiseBody     `json:"promise" binding:"required"`
	Task    createPromiseTaskBody `json:"task" binding:"required"`
}

type createPromiseTaskBody struct {
	ProcessId string `json:"processId" binding:"required"`
	Ttl       int    `json:"ttl" binding:"min=0"`
}

func (s *server) createPromiseAndTask(c *gin.Context) {
	var header createPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body createPromiseAndTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: &t_api.CreatePromiseAndTaskRequest{
			Promise: &t_api.CreatePromiseRequest{
				Id:             body.Promise.Id,
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Param:          body.Promise.Param,
				Timeout:        body.Promise.Timeout,
				Tags:           body.Promise.Tags,
			},
			Task: &t_api.CreateTaskRequest{
				PromiseId: body.Promise.Id,
				ProcessId: body.Task.ProcessId,
				Ttl:       body.Task.Ttl,
				Timeout:   body.Promise.Timeout,
			},
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CreatePromiseAndTask != nil, "result must not be nil")
	c.JSON(s.code(res.CreatePromiseAndTask.Status), gin.H{
		"promise": res.CreatePromiseAndTask.Promise,
		"task":    res.CreatePromiseAndTask.Task,
	})
}

// Complete

type completePromiseHeader struct {
	RequestId      string           `header:"request-id"`
	IdempotencyKey *idempotency.Key `header:"idempotency-key"`
	Strict         bool             `header:"strict"`
}

type completePromiseBody struct {
	State promise.State `json:"state" binding:"required"`
	Value promise.Value `json:"value"`
}

func (s *server) completePromise(c *gin.Context) {
	var header completePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body completePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	if !body.State.In(promise.Resolved | promise.Rejected | promise.Canceled) {
		err := api.RequestValidationError(errors.New("the field state must be one of resolved, rejected, rejected_canceled"))
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: &t_api.CompletePromiseRequest{
			Id:             extractId(c.Param("id")),
			IdempotencyKey: header.IdempotencyKey,
			Strict:         header.Strict,
			State:          body.State,
			Value:          body.Value,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CompletePromise != nil, "result must not be nil")
	c.JSON(s.code(res.CompletePromise.Status), res.CompletePromise.Promise)
}

// Callback

type createCallbackHeader struct {
	RequestId string `header:"request-id"`
}

type createCallbackBody struct {
	PromiseId     string          `json:"promiseId"`
	RootPromiseId string          `json:"rootPromiseId" binding:"required"`
	Recv          json.RawMessage `json:"recv" binding:"required"`
	Timeout       int64           `json:"timeout"`
}

func (s *server) createCallback(c *gin.Context) {
	var header createCallbackHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body createCallbackBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	// The parameter takes priority, but once we remove the deprecated route we
	// can remove promise id from the body and retrieve the information solely
	// from the route parameter.
	if c.Param("id") != "" {
		body.PromiseId = extractId(c.Param("id"))
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: &t_api.CreateCallbackRequest{
			Id:        util.ResumeId(body.RootPromiseId, body.PromiseId),
			PromiseId: body.PromiseId,
			Recv:      body.Recv,
			Mesg:      &message.Mesg{Type: "resume", Root: body.RootPromiseId, Leaf: body.PromiseId},
			Timeout:   body.Timeout,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CreateCallback != nil, "result must not be nil")
	c.JSON(s.code(res.CreateCallback.Status), gin.H{
		"callback": res.CreateCallback.Callback,
		"promise":  res.CreateCallback.Promise,
	})
}

// Subscribe

type createSubscriptionHeader struct {
	RequestId string `header:"request-id"`
}

type createSubscriptionBody struct {
	Id        string          `json:"Id" binding:"required"`
	PromiseId string          `json:"promiseId"`
	Recv      json.RawMessage `json:"recv" binding:"required"`
	Timeout   int64           `json:"timeout"`
}

func (s *server) createSubscription(c *gin.Context) {
	var header createSubscriptionHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body createSubscriptionBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	// The parameter takes priority, but once we remove the deprecated route we
	// can remove promise id from the body and retrieve the information solely
	// from the route parameter.
	if c.Param("id") != "" {
		body.PromiseId = extractId(c.Param("id"))
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: &t_api.CreateCallbackRequest{
			Id:        util.NotifyId(body.PromiseId, body.Id),
			PromiseId: body.PromiseId,
			Recv:      body.Recv,
			Mesg:      &message.Mesg{Type: "notify", Root: body.PromiseId},
			Timeout:   body.Timeout,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CreateCallback != nil, "result must not be nil")
	c.JSON(s.code(res.CreateCallback.Status), gin.H{
		"callback": res.CreateCallback.Callback,
		"promise":  res.CreateCallback.Promise,
	})
}
