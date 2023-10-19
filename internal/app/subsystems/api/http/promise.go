package http

import (
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Read Promise

func (s *server) readPromise(c *gin.Context) {
	resp, err := s.service.ReadPromise(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(int(resp.Status), resp.Promise)
}

// Search Promise

func (s *server) searchPromises(c *gin.Context) {
	var params service.SearchPromiseParams

	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resp, err := s.service.SearchPromises(&params)

	if err != nil {
		if verr, ok := err.(*service.ValidationError); ok {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": verr.Error(),
			})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err.Error(),
			})
		}
		return
	}
	c.JSON(int(resp.Status), gin.H{
		"cursor":   resp.Cursor,
		"promises": resp.Promises,
	})
}

// Create Promise
func (s *server) createPromise(c *gin.Context) {
	var header service.CreatePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *service.CreatePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resp, err := s.service.CreatePromise(c.Param("id"), &header, body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(int(resp.Status), resp.Promise)
}

// Cancel Promise
func (s *server) cancelPromise(c *gin.Context) {
	var header service.CancelPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *service.CancelPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	resp, err := s.service.CancelPromise(c.Param("id"), &header, body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(int(resp.Status), resp.Promise)
}

// Resolve Promise

func (s *server) resolvePromise(c *gin.Context) {
	var header service.ResolvePromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *service.ResolvePromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resp, err := s.service.ResolvePromise(c.Param("id"), &header, body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(int(resp.Status), resp.Promise)
}

// Reject Promise
func (s *server) rejectPromise(c *gin.Context) {
	var header service.RejectPromiseHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *service.RejectPromiseBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	resp, err := s.service.RejectPromise(c.Param("id"), &header, body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(int(resp.Status), resp.Promise)
}
