package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

// Acquire

type acquireLockHeader struct {
	RequestId string `header:"request-id"`
}

type acquireLockBody struct {
	ResourceId  string `json:"resourceId" binding:"required"`
	ExecutionId string `json:"executionId" binding:"required"`
	ProcessId   string `json:"processId" binding:"required"`
	Ttl         int64  `json:"ttl" binding:"min=0"`
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Metadata: metadata,
		Payload: &t_api.AcquireLockRequest{
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

	acquireLock := res.AsAcquireLockResponse()
	c.JSON(s.code(res.Status), acquireLock.Lock)
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Metadata: metadata,
		Payload: &t_api.ReleaseLockRequest{
			ResourceId:  body.ResourceId,
			ExecutionId: body.ExecutionId,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res.AsReleaseLockResponse() // Serves as a type assertion
	c.JSON(s.code(res.Status), nil)
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Metadata: metadata,
		Payload: &t_api.HeartbeatLocksRequest{
			ProcessId: body.ProcessId,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	heartbeatLocks := res.AsHeartbeatLocksResponse()
	c.JSON(s.code(res.Status), gin.H{
		"locksAffected": heartbeatLocks.LocksAffected,
	})
}
