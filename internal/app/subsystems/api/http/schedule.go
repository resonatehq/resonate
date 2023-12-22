package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
)

// CREATE

func (s *server) createSchedule(c *gin.Context) {
	var header service.CreateScheduleHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.CreateScheduleBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.CreateSchedule(header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Schedule)
}

// READ

func (s *server) readSchedule(c *gin.Context) {
	id := extractId(c.Param("id"))

	res, err := s.service.ReadSchedule(id)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(res.Status.HTTP(), res.Schedule)
}

// UPDATE

// todo: pause, resume, trigger

// DELETE

func (s *server) deleteSchedule(c *gin.Context) {
	id := extractId(c.Param("id"))

	res, err := s.service.DeleteSchedule(id)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(res.Status.HTTP(), nil)
}
