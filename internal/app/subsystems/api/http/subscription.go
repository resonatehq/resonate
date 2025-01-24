package http

import (
	"encoding/json"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// Create
type createSubscriptionHeader struct {
	RequestId string `header:"request-id"`
}

type createSubscriptionBody struct {
	Id        string          `json:"Id" binding:"required"`
	PromiseId string          `json:"promiseId" binding:"required"`
	Timeout   int64           `json:"timeout"`
	Recv      json.RawMessage `json:"recv" binding:"required"`
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

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.CreateSubscription,
		CreateSubscription: &t_api.CreateSubscriptionRequest{
			Id:        body.Id,
			PromiseId: body.PromiseId,
			Timeout:   body.Timeout,
			Recv:      body.Recv,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CreateSubscription != nil, "result must not be nil")
	c.JSON(s.code(res.CreateSubscription.Status), gin.H{
		"callback": res.CreateSubscription.Callback,
		"promise":  res.CreateSubscription.Promise,
	})
}
