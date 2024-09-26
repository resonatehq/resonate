package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// Claim

type claimTaskHeader struct {
	RequestId string `header:"request-id"`
}

type claimTaskBody struct {
	Id        string `json:"id" binding:"required"`
	ProcessId string `json:"processId" binding:"required"`
	Counter   int    `json:"counter"`
	Frequency int    `json:"frequency" binding:"required"`
}

func (s *server) claimTask(c *gin.Context) {
	var header claimTaskHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body claimTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.ClaimTask,
		ClaimTask: &t_api.ClaimTaskRequest{
			Id:        body.Id,
			ProcessId: body.ProcessId,
			Counter:   body.Counter,
			Frequency: body.Frequency,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.ClaimTask != nil, "result must not be nil")
	util.Assert(res.ClaimTask.Status != t_api.StatusCreated || (res.ClaimTask.Task != nil && res.ClaimTask.Task.Mesg != nil), "task and mesg must not be nil if created")

	var r gin.H
	if res.ClaimTask.Status == t_api.StatusCreated {
		r = gin.H{
			"type":     res.ClaimTask.Task.Mesg.Type,
			"promises": res.ClaimTask.Task.Mesg.Promises,
		}
	}

	c.JSON(s.code(res.ClaimTask.Status), r)
}

// Complete

type completeTaskHeader struct {
	RequestId string `header:"request-id"`
}

type completeTaskBody struct {
	Id      string `json:"id" binding:"required"`
	Counter int    `json:"counter"`
}

func (s *server) completeTask(c *gin.Context) {
	var header completeTaskHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body completeTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.CompleteTask,
		CompleteTask: &t_api.CompleteTaskRequest{
			Id:      body.Id,
			Counter: body.Counter,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CompleteTask != nil, "result must not be nil")
	c.JSON(s.code(res.CompleteTask.Status), nil)
}

// Heartbeat

type heartbeatTasksHeader struct {
	RequestId string `header:"request-id"`
}

type heartbeatTaskBody struct {
	ProcessId string `json:"processId" binding:"required"`
}

func (s *server) heartbeatTasks(c *gin.Context) {
	var header heartbeatTasksHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body heartbeatTaskBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.HeartbeatTasks,
		HeartbeatTasks: &t_api.HeartbeatTasksRequest{
			ProcessId: body.ProcessId,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.HeartbeatTasks != nil, "result must not be nil")
	c.JSON(s.code(res.HeartbeatTasks.Status), gin.H{
		"tasksAffected": res.HeartbeatTasks.TasksAffected,
	})
}
