package http

import (
	"encoding/json"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// Create
type createSuscriptionHeader struct {
	RequestId string `header:"request-id"`
}

type createSuscriptionBody struct {
	Id        string          `json:"Id" binding:"required"`
	PromiseId string          `json:"promiseId" binding:"required"`
	Timeout   int64           `json:"timeout"`
	Recv      json.RawMessage `json:"recv" binding:"required"`
}

func (s *server) createSuscription(c *gin.Context) {
	var header createSuscriptionHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body createSuscriptionBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.CreateSuscription,
		CreateSuscription: &t_api.CreateSuscriptionRequest{
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

	util.Assert(res.CreateSuscription != nil, "result must not be nil")
	c.JSON(s.code(res.CreateSuscription.Status), gin.H{
		"callback": res.CreateSuscription.Callback,
		"promise":  res.CreateSuscription.Promise,
	})
}
