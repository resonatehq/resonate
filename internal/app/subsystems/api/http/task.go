package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// CLAIM

func (s *server) claimTask(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *service.ClaimTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.ClaimTask(&header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	if res.Status == t_api.StatusCreated {
		util.Assert(res.Task != nil, "task must be non nil")
		util.Assert(res.Task.Message != nil, "message must be non nil")

		c.Data(s.code(res.Status), "application/json", res.Task.Message.Data)
	} else {
		c.JSON(s.code(res.Status), nil)
	}
}

// COMPLETE

func (s *server) completeTask(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *service.CompleteTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.CompleteTask(&header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), nil)
}

// HEARTBEAT

func (s *server) heartbeatTasks(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *service.HeartbeatTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.HeartbeatTasks(&header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), gin.H{
		"tasksAffected": res.TasksAffected,
	})
}
