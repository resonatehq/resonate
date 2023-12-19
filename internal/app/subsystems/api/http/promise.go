package http

import (
	"errors"
	"net/http"
	"strings"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"

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
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
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
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
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

	var body *promise.Promise
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.CreatePromise(&header, body)
	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Promise)
}

// Complete Promise

func (s *server) completePromise(c *gin.Context) {
	var header service.CompletePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	var body *service.CompletePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	id := c.Param("id")

	var (
		resp *t_api.CompletePromiseResponse
		err  error
	)

	switch strings.ToUpper(body.State) {
	case promise.Resolved.String():
		resp, err = s.service.ResolvePromise(id, &header, body)
	case promise.Rejected.String():
		resp, err = s.service.RejectPromise(id, &header, body)
	case promise.Canceled.String():
		resp, err = s.service.CancelPromise(id, &header, body)
	default:
		c.JSON(http.StatusBadRequest, api.HandleValidationError(errors.New("invalid state")))
	}

	if err != nil {
		var apiErr *api.APIErrorResponse
		if errors.As(err, &apiErr) {
			c.JSON(apiErr.APIError.Code.HTTP(), apiErr)
			return
		}
		panic(err)
	}

	c.JSON(resp.Status.HTTP(), resp.Promise)
}
