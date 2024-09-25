package http

import (
	"errors"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/pkg/promise"

	"github.com/gin-gonic/gin"
)

// Read Promise

func (s *server) readPromise(c *gin.Context) {
	id := extractId(c.Param("id"))

	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.ReadPromise(id, &header)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), res.Promise)
}

// Search Promise

func (s *server) searchPromises(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var params service.SearchPromisesParams
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

	res, err := s.service.SearchPromises(&header, &params)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), gin.H{
		"cursor":   res.Cursor,
		"promises": res.Promises,
	})
}

// Create Promise

func (s *server) createPromise(c *gin.Context) {
	var header service.CreatePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *promise.Promise
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.CreatePromise(&header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), res.Promise)
}

// Complete Promise

func (s *server) completePromise(c *gin.Context) {
	id := extractId(c.Param("id"))

	var header service.CompletePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	var body *service.CompletePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := service.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	if !body.State.In(promise.Resolved | promise.Rejected | promise.Canceled) {
		err := service.RequestValidationError(errors.New("invalid state"))
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	res, err := s.service.CompletePromise(id, body.State, &header, body)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{
			"error": err,
		})
		return
	}

	c.JSON(s.code(res.Status), res.Promise)
}
