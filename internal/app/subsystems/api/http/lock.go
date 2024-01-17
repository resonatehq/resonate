package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
)

// ACQUIRE

func (s *server) acquireLock(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.AcquireLockBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.AcquireLock(&header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Lock)
}

// HEARTBEAT

func (s *server) heartbeatLocks(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.HeartbeatBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.Heartbeat(&header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), struct{ int64 }{resp.LocksAffected})
}

// RELEASE

func (s *server) releaseLock(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.ReleaseLockBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.ReleaseLock(&header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), nil)
}
