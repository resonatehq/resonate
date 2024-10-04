package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

// Acquire

type acquireLockHeader struct {
	RequestId string `header:"request-id"`
}

type acquireLockBody struct {
	ResourceId  string `json:"resourceId" binding:"required"`
	ExecutionId string `json:"executionId" binding:"required"`
	ProcessId   string `json:"processId" binding:"required"`
	Ttl         int64  `json:"ttl"`
}

func (s *server) acquireLock(c *gin.Context) {
	var header acquireLockHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body acquireLockBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.AcquireLock,
		AcquireLock: &t_api.AcquireLockRequest{
			ResourceId:  body.ResourceId,
			ExecutionId: body.ExecutionId,
			ProcessId:   body.ProcessId,
			Ttl:         body.Ttl,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.AcquireLock != nil, "result must not be nil")
	c.JSON(s.code(res.AcquireLock.Status), res.AcquireLock.Lock)
}

// Release

type releaseLockHeader struct {
	RequestId string `header:"request-id"`
}

type releaseLockBody struct {
	ResourceId  string `json:"resourceId" binding:"required"`
	ExecutionId string `json:"executionId" binding:"required"`
}

func (s *server) releaseLock(c *gin.Context) {
	var header releaseLockHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body releaseLockBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.ReleaseLock,
		ReleaseLock: &t_api.ReleaseLockRequest{
			ResourceId:  body.ResourceId,
			ExecutionId: body.ExecutionId,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.ReleaseLock != nil, "result must not be nil")
	c.JSON(s.code(res.ReleaseLock.Status), nil)
}

// Heartbeat

type heartbeatLocksHeader struct {
	RequestId string `header:"request-id"`
}

type heartbeatLocksBody struct {
	ProcessId string `json:"processId" binding:"required"`
}

func (s *server) heartbeatLocks(c *gin.Context) {
	var header heartbeatLocksHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body heartbeatLocksBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.HeartbeatLocks,
		HeartbeatLocks: &t_api.HeartbeatLocksRequest{
			ProcessId: body.ProcessId,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.HeartbeatLocks != nil, "result must not be nil")
	c.JSON(s.code(res.HeartbeatLocks.Status), gin.H{
		"locksAffected": res.HeartbeatLocks.LocksAffected,
	})
}
