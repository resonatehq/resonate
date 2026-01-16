package http

import (
	"encoding/json"
	"errors"

	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.PromiseGetRequest{
			Id: extractId(c.Param("id")),
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	c.JSON(s.code(res.Status), res.AsPromiseGetResponse().Promise)
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: req,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	searchPromise := res.AsPromiseSearchResponse()
	c.JSON(s.code(res.Status), gin.H{
		"promises": searchPromise.Promises,
		"cursor":   searchPromise.Cursor,
	})
}

// Create

type createPromiseHeader struct {
	RequestId   string `header:"request-id"`
	Traceparent string `header:"traceparent"`
	Tracestate  string `header:"tracestate"`
	TaskId      string `header:"task-id"`
	TaskCounter int64  `header:"task-counter"`
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

	metadata := map[string]string{}
	if header.Traceparent != "" {
		metadata["traceparent"] = header.Traceparent
		if header.Tracestate != "" {
			metadata["tracestate"] = header.Tracestate
		}
	}

	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.PromiseCreateRequest{
			Id:      body.Id,
			Param:   body.Param,
			Timeout: body.Timeout,
			Tags:    body.Tags,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	c.JSON(s.code(res.Status), res.AsPromiseCreateResponse().Promise)
}

type createPromiseAndTaskBody struct {
	Promise createPromiseBody     `json:"promise" binding:"required"`
	Task    createPromiseTaskBody `json:"task" binding:"required"`
}

type createPromiseTaskBody struct {
	ProcessId string `json:"processId" binding:"required"`
	Ttl       int64  `json:"ttl" binding:"min=0"`
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

	metadata := map[string]string{}
	if header.Traceparent != "" {
		metadata["traceparent"] = header.Traceparent
		if header.Tracestate != "" {
			metadata["tracestate"] = header.Tracestate
		}
	}

	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.TaskCreateRequest{
			Promise: &t_api.PromiseCreateRequest{
				Id:      body.Promise.Id,
				Param:   body.Promise.Param,
				Timeout: body.Promise.Timeout,
				Tags:    body.Promise.Tags,
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

	createPromiseAndTask := res.AsTaskCreateResponse()
	c.JSON(s.code(res.Status), gin.H{
		"promise": createPromiseAndTask.Promise,
		"task":    createPromiseAndTask.Task,
	})
}

// Complete

type completePromiseHeader struct {
	RequestId   string `header:"request-id"`
	TaskId      string `header:"task-id"`
	TaskCounter int64  `header:"task-counter"`
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.PromiseCompleteRequest{
			Id:    extractId(c.Param("id")),
			State: body.State,
			Value: body.Value,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	c.JSON(s.code(res.Status), res.AsPromiseCompleteResponse().Promise)
}

// Callback

type createCallbackHeader struct {
	RequestId   string `header:"request-id"`
	Traceparent string `header:"traceparent"`
	Tracestate  string `header:"tracestate"`
	TaskId      string `header:"task-id"`
	TaskCounter int64  `header:"task-counter"`
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

	head := map[string]string{}
	if header.Traceparent != "" {
		head["traceparent"] = header.Traceparent
		if header.Tracestate != "" {
			head["tracestate"] = header.Tracestate
		}
	}

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.PromiseRegisterRequest{
			Id:        util.ResumeId(body.RootPromiseId, body.PromiseId),
			PromiseId: body.PromiseId,
			Recv:      body.Recv,
			Mesg:      &message.Mesg{Type: "resume", Head: head, Root: body.RootPromiseId, Leaf: body.PromiseId},
			Timeout:   body.Timeout,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	createCallback := res.AsPromiseRegisterResponse()
	c.JSON(s.code(res.Status), gin.H{
		"callback": createCallback.Callback,
		"promise":  createCallback.Promise,
	})
}

// Subscribe

type createSubscriptionHeader struct {
	RequestId   string `header:"request-id"`
	Traceparent string `header:"traceparent"`
	Tracestate  string `header:"tracestate"`
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

	head := map[string]string{}
	if header.Traceparent != "" {
		head["traceparent"] = header.Traceparent
		if header.Tracestate != "" {
			head["tracestate"] = header.Tracestate
		}
	}
	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.PromiseRegisterRequest{
			Id:        util.NotifyId(body.PromiseId, body.Id),
			PromiseId: body.PromiseId,
			Recv:      body.Recv,
			Mesg:      &message.Mesg{Type: "notify", Head: head, Root: body.PromiseId},
			Timeout:   body.Timeout,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	createCallback := res.AsPromiseRegisterResponse()
	c.JSON(s.code(res.Status), gin.H{
		"callback": createCallback.Callback,
		"promise":  createCallback.Promise,
	})
}
