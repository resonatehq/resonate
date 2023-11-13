package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
)

// Search Subscription

func (s *server) searchSubscriptions(c *gin.Context) {
	var header service.Header
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var params service.SearchSubscriptionsParams
	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resp, err := s.service.SearchSubscriptions(c.Param("id"), &header, &params)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(int(resp.Status), resp.Subscriptions)
}

// Create Subscription

func (s *server) createSubscription(c *gin.Context) {
	var header service.CreateSubscriptionHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	var body *service.CreateSubscriptionBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resp, err := s.service.CreateSubscription(c.Param("id"), header, body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(int(resp.Status), resp.Subscription)
}

// Delete Subscription

func (s *server) deleteSubscription(c *gin.Context) {
	var header service.DeleteSubscriptionHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	resp, err := s.service.DeleteSubscription(c.Param("id"), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(int(resp.Status), nil)
}
