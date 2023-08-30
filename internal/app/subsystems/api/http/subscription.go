package http

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/resonatehq/resonate/internal/util"
)

func (s *server) readSubscriptions(c *gin.Context) {
	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind: types.ReadSubscriptions,
			ReadSubscriptions: &types.ReadSubscriptionsRequest{
				PromiseId: c.Param("id"),
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

	util.Assert(cqe.Completion.ReadSubscriptions != nil, "response must not be nil")
	c.JSON(cqe.Completion.ReadSubscriptions.Status.HttpStatus(), cqe.Completion.ReadSubscriptions.Subscriptions)
}

func (s *server) createSubscription(c *gin.Context) {
	var createSubscription *types.CreateSubscriptionRequest
	if err := c.ShouldBindJSON(&createSubscription); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind:               types.CreateSubscription,
			CreateSubscription: createSubscription,
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

	util.Assert(cqe.Completion.CreateSubscription != nil, "response must not be nil")
	c.JSON(cqe.Completion.CreateSubscription.Status.HttpStatus(), cqe.Completion.CreateSubscription.Subscription)
}

func (s *server) deleteSubscription(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}

	cq := make(chan *bus.CQE[types.Request, types.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[types.Request, types.Response]{
		Kind: "http",
		Submission: &types.Request{
			Kind: types.DeleteSubscription,
			DeleteSubscription: &types.DeleteSubscriptionRequest{
				Id: id,
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

	util.Assert(cqe.Completion.DeleteSubscription != nil, "response must not be nil")
	c.JSON(cqe.Completion.DeleteSubscription.Status.HttpStatus(), nil)
}
