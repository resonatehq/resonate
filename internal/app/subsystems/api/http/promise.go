package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/promise"
)

// Read Promise

func (s *server) readPromise(c *gin.Context) {
	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind: t_api.ReadPromise,
			ReadPromise: &t_api.ReadPromiseRequest{
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
	c.JSON(int(cqe.Completion.ReadPromise.Status), cqe.Completion.ReadPromise.Promise)
}

// Search Promise

type SearchPromiseParams struct {
	Q      string `form:"q" json:"q"`
	State  string `form:"state" json:"state"`
	Limit  int    `form:"limit" json:"limit"`
	Cursor string `form:"cursor" json:"cursor"`
}

func (s *server) searchPromises(c *gin.Context) {
	var params SearchPromiseParams
	var searchPromises *t_api.SearchPromisesRequest

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	if params.Cursor != "" {
		cursor, err := t_api.NewCursor[t_api.SearchPromisesRequest](params.Cursor)
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
		if limit <= 0 || limit > 100 {
			limit = 100
		}

		searchPromises = &t_api.SearchPromisesRequest{
			Q:      params.Q,
			States: states,
			Limit:  limit,
		}
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind:           t_api.SearchPromises,
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
	c.JSON(int(cqe.Completion.SearchPromises.Status), gin.H{
		"cursor":   cqe.Completion.SearchPromises.Cursor,
		"promises": cqe.Completion.SearchPromises.Promises,
	})
}

// Create Promise

type createPromiseHeader struct {
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type createPromiseBody struct {
	Param   promise.Value     `json:"param"`
	Timeout int64             `json:"timeout"`
	Tags    map[string]string `json:"tags"`
}

func (s *server) createPromise(c *gin.Context) {
	var header createPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *createPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind: t_api.CreatePromise,
			CreatePromise: &t_api.CreatePromiseRequest{
				Id:             c.Param("id"),
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Param:          body.Param,
				Timeout:        body.Timeout,
				Tags:           body.Tags,
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

	util.Assert(cqe.Completion.CreatePromise != nil, "response must not be nil")
	c.JSON(int(cqe.Completion.CreatePromise.Status), cqe.Completion.CreatePromise.Promise)
}

// Cancel Promise

type cancelPromiseHeader struct {
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type cancelPromiseBody struct {
	Value promise.Value `json:"value"`
}

func (s *server) cancelPromise(c *gin.Context) {
	var header cancelPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *cancelPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind: t_api.CancelPromise,
			CancelPromise: &t_api.CancelPromiseRequest{
				Id:             c.Param("id"),
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Value:          body.Value,
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

	util.Assert(cqe.Completion.CancelPromise != nil, "response must not be nil")
	c.JSON(int(cqe.Completion.CancelPromise.Status), cqe.Completion.CancelPromise.Promise)
}

// Resolve Promise

type resolvePromiseHeader struct {
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type resolvePromiseBody struct {
	Value promise.Value `json:"value"`
}

func (s *server) resolvePromise(c *gin.Context) {
	var header resolvePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *resolvePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind: t_api.ResolvePromise,
			ResolvePromise: &t_api.ResolvePromiseRequest{
				Id:             c.Param("id"),
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Value:          body.Value,
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

	util.Assert(cqe.Completion.ResolvePromise != nil, "response must not be nil")
	c.JSON(int(cqe.Completion.ResolvePromise.Status), cqe.Completion.ResolvePromise.Promise)
}

// Reject Promise

type rejectPromiseHeader struct {
	IdempotencyKey *promise.IdempotencyKey `header:"idempotency-key"`
	Strict         bool                    `header:"strict"`
}

type rejectPromiseBody struct {
	Value promise.Value `json:"value"`
}

func (s *server) rejectPromise(c *gin.Context) {
	var header rejectPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *rejectPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind: t_api.RejectPromise,
			RejectPromise: &t_api.RejectPromiseRequest{
				Id:             c.Param("id"),
				IdempotencyKey: header.IdempotencyKey,
				Strict:         header.Strict,
				Value:          body.Value,
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

	util.Assert(cqe.Completion.RejectPromise != nil, "response must not be nil")
	c.JSON(int(cqe.Completion.RejectPromise.Status), cqe.Completion.RejectPromise.Promise)
}
