package http

import (
	"encoding/json"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// Create

type createCallbackHeader struct {
	RequestId string `header:"request-id"`
}

type createCallbackBody struct {
	PromiseId     string          `json:"promiseId" binding:"required"`
	RootPromiseId string          `json:"rootPromiseId" binding:"required"`
	Timeout       int64           `json:"timeout" binding:"required"`
	Recv          json.RawMessage `json:"recv" binding:"required"`
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

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.CreateCallback,
		CreateCallback: &t_api.CreateCallbackRequest{
			PromiseId:     body.PromiseId,
			RootPromiseId: body.RootPromiseId,
			Timeout:       body.Timeout,
			Recv:          body.Recv,
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
