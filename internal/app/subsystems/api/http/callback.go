package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
)

// CREATE

func (s *server) createCallback(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *service.CreateCallbackBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.CreateCallback(&header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), gin.H{
		"callback": res.Callback,
		"promise":  res.Promise,
	})
}
