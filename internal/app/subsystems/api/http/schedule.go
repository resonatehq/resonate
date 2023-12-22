package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
)

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

// SEARCH

func (s *server) searchSchedules(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var params service.SearchSchedulesParams
	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	// tags needs to be parsed manually
	// see: https://github.com/gin-gonic/gin/issues/2606
	params.Tags = c.QueryMap("tags")

	resp, err := s.service.SearchSchedules(&header, &params)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor":    resp.Cursor,
		"schedules": resp.Schedules,
	})
}

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
