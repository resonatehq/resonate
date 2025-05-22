package http

import (
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
)

// Claim

type claimTaskHeader struct {
	RequestId string `header:"request-id"`
}

type claimTaskBody struct {
	Id        string `json:"id" binding:"required"`
	Counter   int    `json:"counter" binding:"required"`
	ProcessId string `json:"processId" binding:"required"`
	Ttl       int    `json:"ttl" binding:"min=0"`
}

func (s *server) claimTask(c *gin.Context) {
	var header claimTaskHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var claimTask *t_api.ClaimTaskRequest

	if c.Request.Method == "GET" {
		counter, err := strconv.Atoi(c.Param("counter"))
		if err != nil {
			err := api.RequestValidationError(errors.New("he field counter must be a number"))
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		claimTask = &t_api.ClaimTaskRequest{
			Id:        c.Param("id"),
			Counter:   counter,
			ProcessId: s.api.TaskProcessId(c.Param("id"), counter),
			Ttl:       int(s.config.TaskFrequency.Milliseconds()),
		}
	} else {
		var body claimTaskBody
		if err := c.ShouldBindJSON(&body); err != nil {
			err := api.RequestValidationError(err)
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		claimTask = &t_api.ClaimTaskRequest{
			Id:        body.Id,
			Counter:   body.Counter,
			ProcessId: body.ProcessId,
			Ttl:       body.Ttl,
		}
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: claimTask,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.ClaimTask != nil, "result must not be nil")
	util.Assert(res.ClaimTask.Status != t_api.StatusCreated || (res.ClaimTask.Task != nil && res.ClaimTask.Task.Mesg != nil), "task and mesg must not be nil if created")

	if res.ClaimTask.Status == t_api.StatusCreated {
		promises := gin.H{
			"root": gin.H{
				"id":   res.ClaimTask.Task.Mesg.Root,
				"href": res.ClaimTask.RootPromiseHref,
				"data": res.ClaimTask.RootPromise,
			},
		}
		if res.ClaimTask.Task.Mesg.Type == message.Resume {
			promises["leaf"] = gin.H{
				"id":   res.ClaimTask.Task.Mesg.Leaf,
				"href": res.ClaimTask.LeafPromiseHref,
				"data": res.ClaimTask.LeafPromise,
			}
		}

		c.JSON(s.code(res.ClaimTask.Status), gin.H{
			"type":     res.ClaimTask.Task.Mesg.Type,
			"promises": promises,
		})
		return
	}

	c.JSON(s.code(res.ClaimTask.Status), nil)
}

// Complete

type completeTaskHeader struct {
	RequestId string `header:"request-id"`
}

type completeTaskBody struct {
	Id      string `json:"id" binding:"required"`
	Counter int    `json:"counter" binding:"required"`
}

func (s *server) completeTask(c *gin.Context) {
	var header completeTaskHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var completeTask *t_api.CompleteTaskRequest

	if c.Request.Method == "GET" {
		counter, err := strconv.Atoi(c.Param("counter"))
		if err != nil {
			err := api.RequestValidationError(errors.New("the field counter must be a number"))
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		completeTask = &t_api.CompleteTaskRequest{
			Id:      c.Param("id"),
			Counter: counter,
		}
	} else {
		var body completeTaskBody
		if err := c.ShouldBindJSON(&body); err != nil {
			err := api.RequestValidationError(err)
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		completeTask = &t_api.CompleteTaskRequest{
			Id:      body.Id,
			Counter: body.Counter,
		}
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: completeTask,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CompleteTask != nil, "result must not be nil")
	c.JSON(s.code(res.CompleteTask.Status), res.CompleteTask.Task)
}

// Drop tasks
type dropTaskHeader struct {
	RequestId string `header:"request-id"`
}

type dropTaskBody struct {
	Id      string `json:"id" binding:"required"`
	Counter int    `json:"counter" binding:"required"`
}

func (s *server) dropTask(c *gin.Context) {
	var header dropTaskHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var dropTask *t_api.DropTaskRequest

	if c.Request.Method == "GET" {
		counter, err := strconv.Atoi(c.Param("counter"))
		if err != nil {
			err := api.RequestValidationError(errors.New("the field counter must be a number"))
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		dropTask = &t_api.DropTaskRequest{
			Id:      c.Param("id"),
			Counter: counter,
		}

	} else {
		var body dropTaskBody
		if err := c.ShouldBindJSON(&body); err != nil {
			err := api.RequestValidationError(err)
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		dropTask = &t_api.DropTaskRequest{
			Id:      body.Id,
			Counter: body.Counter,
		}
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: dropTask,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.DropTask != nil, "result must not be nil")
	c.Status(s.code(res.DropTask.Status))
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

	var heartbeatTasks *t_api.HeartbeatTasksRequest

	if c.Request.Method == "GET" {
		counter, err := strconv.Atoi(c.Param("counter"))
		if err != nil {
			err := api.RequestValidationError(errors.New("the field counter must be a number"))
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		heartbeatTasks = &t_api.HeartbeatTasksRequest{
			ProcessId: s.api.TaskProcessId(c.Param("id"), counter),
		}
	} else {
		var body heartbeatTaskBody
		if err := c.ShouldBindJSON(&body); err != nil {
			err := api.RequestValidationError(err)
			c.JSON(s.code(err.Code), gin.H{"error": err})
			return
		}

		heartbeatTasks = &t_api.HeartbeatTasksRequest{
			ProcessId: body.ProcessId,
		}
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Payload: heartbeatTasks,
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
