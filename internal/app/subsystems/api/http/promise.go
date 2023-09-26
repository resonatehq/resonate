package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

func (s *server) readPromise(c *gin.Context) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind: types.ReadPromise,
			ReadPromise: &types.ReadPromiseRequest{
				Id: c.Param("id"),
			},
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.ReadPromise != nil, "response must not be nil")
	c.JSON(cqe.Completion.ReadPromise.Status.HttpStatus(), cqe.Completion.ReadPromise.Promise)
}

type SearchPromiseParams struct {
	Q      string `form:"q" json:"q"`
	State  string `form:"state" json:"state"`
	Limit  int    `form:"limit" json:"limit"`
	Cursor string `form:"cursor" json:"cursor"`
}

func (s *server) searchPromises(c *gin.Context) {
	var params SearchPromiseParams
	var searchPromises *types.SearchPromisesRequest

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if params.Cursor != "" {
		cursor, err := types.NewCursor[types.SearchPromisesRequest](params.Cursor)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": err.Error(),
			})
			return
		}
		searchPromises = cursor.Next
	} else {
		// validate
		if params.Q == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "query must be provided",
			})
			return
		}

		var states []promise.State
		switch strings.ToLower(params.State) {
		case "":
			states = []promise.State{
				promise.Pending,
				promise.Resolved,
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			}
		case "pending":
			states = []promise.State{
				promise.Pending,
			}
		case "resolved":
			states = []promise.State{
				promise.Resolved,
			}
		case "rejected":
			states = []promise.State{
				promise.Rejected,
				promise.Timedout,
				promise.Canceled,
			}
		default:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "state must be one of: pending, resolved, rejected",
			})
			return
		}

		limit := params.Limit
		if params.Limit < 0 || params.Limit > 100 {
			limit = 100
		}

		searchPromises = &types.SearchPromisesRequest{
			Q:      params.Q,
			States: states,
			Limit:  limit,
		}
	}

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:           types.SearchPromises,
			SearchPromises: searchPromises,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.SearchPromises != nil, "response must not be nil")
	c.JSON(cqe.Completion.SearchPromises.Status.HttpStatus(), gin.H{
		"cursor":   cqe.Completion.SearchPromises.Cursor,
		"promises": cqe.Completion.SearchPromises.Promises,
	})
}

func (s *server) createPromise(c *gin.Context) {
	var createPromise *types.CreatePromiseRequest
	if err := c.ShouldBindJSON(&createPromise); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	createPromise.Id = c.Param("id")

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:          types.CreatePromise,
			CreatePromise: createPromise,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.CreatePromise != nil, "response must not be nil")
	c.JSON(cqe.Completion.CreatePromise.Status.HttpStatus(), cqe.Completion.CreatePromise.Promise)
}

func (s *server) resolvePromise(c *gin.Context) {
	var resolvePromise *types.ResolvePromiseRequest
	if err := c.ShouldBindJSON(&resolvePromise); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resolvePromise.Id = c.Param("id")

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:           types.ResolvePromise,
			ResolvePromise: resolvePromise,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.ResolvePromise != nil, "response must not be nil")
	c.JSON(cqe.Completion.ResolvePromise.Status.HttpStatus(), cqe.Completion.ResolvePromise.Promise)
}

func (s *server) rejectPromise(c *gin.Context) {
	var rejectPromise *types.RejectPromiseRequest
	if err := c.ShouldBindJSON(&rejectPromise); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	rejectPromise.Id = c.Param("id")

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:          types.RejectPromise,
			RejectPromise: rejectPromise,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.RejectPromise != nil, "response must not be nil")
	c.JSON(cqe.Completion.RejectPromise.Status.HttpStatus(), cqe.Completion.RejectPromise.Promise)
}

func (s *server) cancelPromise(c *gin.Context) {
	var cancelPromise *types.CancelPromiseRequest
	if err := c.ShouldBindJSON(&cancelPromise); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cancelPromise.Id = c.Param("id")

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:          types.CancelPromise,
			CancelPromise: cancelPromise,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.CancelPromise != nil, "response must not be nil")
	c.JSON(cqe.Completion.CancelPromise.Status.HttpStatus(), cqe.Completion.CancelPromise.Promise)
}

func (s *server) completePromise(c *gin.Context) {
	var completePromise *types.CompletePromiseRequest
	if err := c.ShouldBindJSON(&completePromise); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	if !completePromise.State.In(promise.Resolved | promise.Rejected | promise.Canceled) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "state must be one of resolved, rejected, or canceled",
		})
		return
	}

	completePromise.Id = c.Param("id")

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:            types.CompletePromise,
			CompletePromise: completePromise,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.CompletePromise != nil, "response must not be nil")
	c.JSON(cqe.Completion.CompletePromise.Status.HttpStatus(), cqe.Completion.CompletePromise.Promise)
}
