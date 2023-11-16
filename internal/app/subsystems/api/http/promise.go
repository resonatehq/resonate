package http

import (
	"errors"
	"net/http"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"

	"github.com/gin-gonic/gin"
)

// Read Promise
func (s *server) readPromise(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.ReadPromise(c.Param("id"), &header)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.StatusCode(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(http.StatusOK, resp.Promise)
}

// Search Promise
func (s *server) searchPromises(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var params service.SearchPromiseParams
	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.SearchPromises(&header, &params)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.StatusCode(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(http.StatusOK, gin.H{
		"cursor":   resp.Cursor,
		"promises": resp.Promises,
	})
}

// Create Promise
func (s *server) createPromise(c *gin.Context) {
	var header service.CreatePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.CreatePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.CreatePromise(c.Param("id"), &header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.StatusCode(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Promise)
}

// Cancel Promise
func (s *server) cancelPromise(c *gin.Context) {
	var header service.CancelPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.CancelPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.CancelPromise(c.Param("id"), &header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.StatusCode(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Promise)
}

// Resolve Promise
func (s *server) resolvePromise(c *gin.Context) {
	var header service.ResolvePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.ResolvePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.ResolvePromise(c.Param("id"), &header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.StatusCode(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Promise)
}

// Reject Promise
func (s *server) rejectPromise(c *gin.Context) {
	var header service.RejectPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.RejectPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.RejectPromise(c.Param("id"), &header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.StatusCode(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Promise)
}
