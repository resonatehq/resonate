package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

func (s *server) echo(c *gin.Context) {
	var req *t_api.EchoRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	cq := make(chan *bus.CQE[t_api.Request, t_api.Response])
	defer close(cq)

	s.api.Enqueue(&bus.SQE[t_api.Request, t_api.Response]{
		Tags: "http",
		Submission: &t_api.Request{
			Kind: t_api.Echo,
			Echo: req,
		},
		Callback: s.sendOrPanic(cq),
	})

	cqe := <-cq
	if cqe.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": cqe.Error.Error(),
		})
		return
	}

	util.Assert(cqe.Completion.Echo != nil, "response must not be nil")
	c.JSON(200, cqe.Completion.Echo)
}
