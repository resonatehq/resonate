package http

import (
	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.ScheduleGetRequest{
			Id: extractId(c.Param("id")),
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	c.JSON(s.code(res.Status), res.AsScheduleGetResponse().Schedule)
}

// Search

type searchSchedulesHeader struct {
	RequestId string `header:"request-id"`
}

type searchSchedulesParams struct {
	Id     *string           `form:"id" json:"id,omitempty" binding:"omitempty,min=1"`
	Tags   map[string]string `form:"tags" json:"tags,omitempty"`
	Limit  *int              `form:"limit" json:"limit,omitempty" binding:"omitempty,gte=0,lte=100"`
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: req,
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	searchSchedules := res.AsScheduleSearchResponse()
	c.JSON(s.code(res.Status), gin.H{
		"schedules": searchSchedules.Schedules,
		"cursor":    searchSchedules.Cursor,
	})
}

// Create

type createScheduleHeader struct {
	RequestId string `header:"request-id"`
}

type createScheduleBody struct {
	Id             string            `json:"id" binding:"required"`
	Description    string            `json:"desc,omitempty"`
	Cron           string            `json:"cron" binding:"required"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromiseId      string            `json:"promiseId" binding:"required"`
	PromiseTimeout int64             `json:"promiseTimeout"`
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.ScheduleCreateRequest{
			Id:             body.Id,
			Description:    body.Description,
			Cron:           body.Cron,
			Tags:           body.Tags,
			PromiseId:      body.PromiseId,
			PromiseTimeout: body.PromiseTimeout,
			PromiseParam:   body.PromiseParam,
			PromiseTags:    body.PromiseTags,
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	c.JSON(s.code(res.Status), res.AsScheduleCreateResponse().Schedule)
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

	metadata := map[string]string{}
	if auth := c.GetString("authorization"); auth != "" {
		metadata["authorization"] = auth
	}
	res, err := s.api.Process(header.RequestId, &t_api.Request{
		Head: metadata,
		Data: &t_api.ScheduleDeleteRequest{
			Id: extractId(c.Param("id")),
		},
	})
	if err != nil {
		c.JSON(s.code(err.Code), gin.H{"error": err})
		return
	}

	_ = res.AsScheduleDeleteResponse() // Serves as a type assertion
	c.JSON(s.code(res.Status), nil)
}
