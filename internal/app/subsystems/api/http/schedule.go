package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
)

// READ

func (s *server) readSchedule(c *gin.Context) {
	id := extractId(c.Param("id"))

	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.ReadSchedule(id, &header)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), res.Schedule)
}

// SEARCH

func (s *server) searchSchedules(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var params service.SearchSchedulesParams
	if err := c.ShouldBindQuery(&params); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	// tags needs to be parsed manually
	// see: https://github.com/gin-gonic/gin/issues/2606
	params.Tags = c.QueryMap("tags")

	res, err := s.service.SearchSchedules(&header, &params)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), gin.H{
		"cursor":    res.Cursor,
		"schedules": res.Schedules,
	})
}

// CREATE

func (s *server) createSchedule(c *gin.Context) {
	var header service.CreateScheduleHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *service.CreateScheduleBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.CreateSchedule(header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), res.Schedule)
}

// DELETE

func (s *server) deleteSchedule(c *gin.Context) {
	id := extractId(c.Param("id"))

	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.DeleteSchedule(id, &header)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), nil)
}
