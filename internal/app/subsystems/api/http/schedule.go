package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

// Read

type readScheduleHeader struct {
	RequestId string `header:"request-id"`
}

func (s *server) readSchedule(c *gin.Context) {
	var header readScheduleHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.ReadSchedule,
		ReadSchedule: &t_api.ReadScheduleRequest{
			Id: extractId(c.Param("id")),
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.ReadSchedule != nil, "result must not be nil")
	c.JSON(s.code(res.ReadSchedule.Status), res.ReadSchedule.Schedule)
}

// Search

type searchSchedulesHeader struct {
	RequestId string `header:"request-id"`
}

type searchSchedulesParams struct {
	Id     *string           `form:"id" json:"id,omitempty" binding:"omitempty,min=1"`
	Tags   map[string]string `form:"tags" json:"tags,omitempty"`
	Limit  *int              `form:"limit" json:"limit,omitempty" binding:"omitempty,gt=0,lte=100"`
	Cursor *string           `form:"cursor" json:"cursor,omitempty"`
}

func (s *server) searchSchedules(c *gin.Context) {
	var header searchSchedulesHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var params searchSchedulesParams
	if err := c.ShouldBindQuery(&params); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	// tags needs to be parsed manually
	// see: https://github.com/gin-gonic/gin/issues/2606
	params.Tags = c.QueryMap("tags")

	req, err := s.api.SearchSchedules(
		util.SafeDeref(params.Id),
		params.Tags,
		util.SafeDeref(params.Limit),
		util.SafeDeref(params.Cursor),
	)
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind:            t_api.SearchSchedules,
		SearchSchedules: req,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.SearchSchedules != nil, "result must not be nil")
	c.JSON(s.code(res.SearchSchedules.Status), gin.H{
		"schedules": res.SearchSchedules.Schedules,
		"cursor":    res.SearchSchedules.Cursor,
	})
}

// Create

type createScheduleHeader struct {
	RequestId      string           `header:"request-id"`
	IdempotencyKey *idempotency.Key `header:"idempotency-key"`
}

type createScheduleBody struct {
	Id             string            `json:"id" binding:"required"`
	Description    string            `json:"desc,omitempty"`
	Cron           string            `json:"cron" binding:"required"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromiseId      string            `json:"promiseId" binding:"required"`
	PromiseTimeout int64             `json:"promiseTimeout" binding:"required"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
}

func (s *server) createSchedule(c *gin.Context) {
	var header createScheduleHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	var body createScheduleBody
	if err := c.ShouldBindJSON(&body); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	if err := s.api.ValidateCron(body.Cron); err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.CreateSchedule,
		CreateSchedule: &t_api.CreateScheduleRequest{
			Id:             body.Id,
			Description:    body.Description,
			Cron:           body.Cron,
			Tags:           body.Tags,
			PromiseId:      body.PromiseId,
			PromiseTimeout: body.PromiseTimeout,
			PromiseParam:   body.PromiseParam,
			PromiseTags:    body.PromiseTags,
			IdempotencyKey: header.IdempotencyKey,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.CreateSchedule != nil, "result must not be nil")
	c.JSON(s.code(res.CreateSchedule.Status), res.CreateSchedule.Schedule)
}

// Delete

type deleteScheduleHeader struct {
	RequestId string `header:"request-id"`
}

func (s *server) deleteSchedule(c *gin.Context) {
	var header deleteScheduleHeader
	if err := c.ShouldBindHeader(&header); err != nil {
		err := api.RequestValidationError(err)
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Kind: t_api.DeleteSchedule,
		DeleteSchedule: &t_api.DeleteScheduleRequest{
			Id: extractId(c.Param("id")),
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	util.Assert(res.DeleteSchedule != nil, "result must not be nil")
	c.JSON(s.code(res.DeleteSchedule.Status), nil)
}
