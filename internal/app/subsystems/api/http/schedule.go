package http

import (
	"errors"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/api"
)

// CREATE

func (s *server) createSchedule(c *gin.Context) {

	// todo: header

	// var body *service.CreateScheduleBody
	// if err := c.ShouldBindJSON(&body); err != nil {
	// 	c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
	// 	return
	// }

	resp, err := s.service.CreateSchedule("my-schedule")
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

	// todo: header

	res, err := s.service.ReadSchedule(c.Param("id"))
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

// pause, resume, trigger

// DELETE

func (s *server) deleteSchedule(c *gin.Context) {

	// todo: header

	res, err := s.service.DeleteSchedule(c.Param("id"))
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
