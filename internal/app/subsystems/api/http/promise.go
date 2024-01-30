package http

import (
	"errors"
	"net/http"

	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"

	"github.com/gin-gonic/gin"
)

// Read Promise

func (s *server) readPromise(c *gin.Context) {
	id := extractId(c.Param("id"))

	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	resp, err := s.service.ReadPromise(id, &header)
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

	var params service.SearchPromisesParams
	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(err))
		return
	}

	// tags needs to be parsed manually
	// see: https://github.com/gin-gonic/gin/issues/2606
	params.Tags = c.QueryMap("tags")

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
	id := extractId(c.Param("id"))

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

	var (
		resp *t_api.CompletePromiseResponse
		err  error
	)

	if !body.State.In(promise.Resolved | promise.Rejected | promise.Canceled) {
		c.JSON(http.StatusBadRequest, api.HandleValidationError(errors.New("invalid state")))
		return
	}

	resp, err = s.service.CompletePromise(id, body.State, &header, body)
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
