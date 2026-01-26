package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/promise"
	"github.com/resonatehq/resonate/pkg/schedule"
)

type Req struct {
	Kind string          `json:"kind" binding:"required"`
	Head Head            `json:"head" binding:"required"`
	Data json.RawMessage `json:"data" binding:"required"`
}

type Head struct {
	Auth   string `json:"auth,omitempty"`
	CorrId string `json:"corrId"`
}

type Res struct {
	Kind string         `json:"kind"`
	Head map[string]any `json:"head"`
	Data any            `json:"data,omitempty"`
}

type promiseGetData struct {
	Id string `json:"id" binding:"required"`
}

type promiseCreateData struct {
	Id        string            `json:"id" binding:"required"`
	Param     promise.Value     `json:"param,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	TimeoutAt int64             `json:"timeoutAt" binding:"required"`
}

type promiseSettleData struct {
	Id    string        `json:"id" binding:"required"`
	State promise.State `json:"state" binding:"required"`
	Value promise.Value `json:"value,omitempty"`
}

type promiseRegisterData struct {
	Awaiter string `json:"awaiter" binding:"required"`
	Awaited string `json:"awaited" binding:"required"`
}

type promiseSubscribeData struct {
	Awaited string `json:"awaited" binding:"required"`
	Address string `json:"address" binding:"required"`
}

type taskCreateData struct {
	Pid    string            `json:"pid" binding:"required"`
	Ttl    int64             `json:"ttl" binding:"required"`
	Action promiseCreateData `json:"action" binding:"required"`
}

type taskAcquireData struct {
	Id      string `json:"id" binding:"required"`
	Version int    `json:"version" binding:"required"`
	Pid     string `json:"pid" binding:"required"`
	Ttl     int64  `json:"ttl" binding:"required"`
}

type taskReleaseData struct {
	Id      string `json:"id" binding:"required"`
	Version int    `json:"version" binding:"required"`
}

type taskFulfillData struct {
	Id      string            `json:"id" binding:"required"`
	Version int               `json:"version" binding:"required"`
	Action  promiseSettleData `json:"action" binding:"required"`
}

type taskSuspendData struct {
	Id      string                `json:"id" binding:"required"`
	Version int                   `json:"version" binding:"required"`
	Actions []promiseRegisterData `json:"actions" binding:"required,dive"`
}

type taskHeartbeatData struct {
	Pid string `json:"pid" binding:"required"`
}

type scheduleGetData struct {
	Id string `json:"id" binding:"required"`
}

type scheduleCreateData struct {
	Id             string            `json:"id" binding:"required"`
	Cron           string            `json:"cron" binding:"required"`
	PromiseId      string            `json:"promiseId" binding:"required"`
	PromiseTimeout int64             `json:"promiseTimeout" binding:"required"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
}

type scheduleDeleteData struct {
	Id string `json:"id" binding:"required"`
}

func (s *server) process(c *gin.Context) {
	var req Req
	if err := c.ShouldBindJSON(&req); err != nil {
		kind := "error"
		if req.Kind != "" {
			kind = req.Kind
		}
		c.JSON(http.StatusBadRequest, Res{
			Kind: kind,
			Head: map[string]any{"corrId": "", "status": 400},
			Data: err.Error(),
		})
		return
	}

	metadata := map[string]string{}
	if req.Head.Auth != "" {
		metadata["authorization"] = req.Head.Auth
	}

	payload, err := s.bindRequest(req.Kind, req.Data)
	if err != nil {
		c.JSON(http.StatusBadRequest, Res{
			Kind: req.Kind,
			Head: map[string]any{"corrId": req.Head.CorrId, "status": 400},
			Data: err.Error(),
		})
		return
	}

	res, apiErr := s.api.Process(req.Head.CorrId, &t_api.Request{
		Head: metadata,
		Data: payload,
	})
	if apiErr != nil {
		c.JSON(s.code(apiErr.Code), Res{
			Kind: req.Kind,
			Head: map[string]any{"corrId": req.Head.CorrId, "status": s.code(apiErr.Code)},
			Data: apiErr.Error(),
		})
		return
	}

	protoRes := s.mapResponse(req.Kind, res)
	protoRes.Head["corrId"] = req.Head.CorrId
	c.JSON(http.StatusOK, protoRes)
}

func (s *server) bindRequest(kind string, data json.RawMessage) (t_api.RequestPayload, error) {
	switch kind {
	case "promise.get":
		d, err := bind[promiseGetData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.PromiseGetRequest{Id: d.Id}, nil

	case "promise.create":
		d, err := bind[promiseCreateData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.PromiseCreateRequest{
			Id:      d.Id,
			Param:   d.Param,
			Timeout: d.TimeoutAt,
			Tags:    d.Tags,
		}, nil

	case "promise.settle":
		d, err := bind[promiseSettleData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.PromiseCompleteRequest{
			Id:    d.Id,
			State: d.State,
			Value: d.Value,
		}, nil

	case "promise.register":
		d, err := bind[promiseRegisterData](data)
		if err != nil {
			return nil, err
		}

		return &t_api.PromiseRegisterRequest{
			Awaiter: d.Awaiter,
			Awaited: d.Awaited,
		}, nil

	case "promise.subscribe":
		d, err := bind[promiseSubscribeData](data)
		if err != nil {
			return nil, err
		}

		return &t_api.PromiseSubscribeRequest{
			Awaited: d.Awaited,
			Address: d.Address,
		}, nil

	case "task.create":
		d, err := bind[taskCreateData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.TaskCreateRequest{
			Promise: &t_api.PromiseCreateRequest{
				Id:      d.Action.Id,
				Param:   d.Action.Param,
				Timeout: d.Action.TimeoutAt,
				Tags:    d.Action.Tags,
			},
			Task: &t_api.CreateTaskRequest{
				PromiseId: d.Action.Id,
				ProcessId: d.Pid,
				Ttl:       d.Ttl,
				Timeout:   d.Action.TimeoutAt,
			},
		}, nil

	case "task.acquire":
		d, err := bind[taskAcquireData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.TaskAcquireRequest{
			Id:        d.Id,
			Counter:   d.Version,
			ProcessId: d.Pid,
			Ttl:       d.Ttl,
		}, nil

	case "task.release":
		d, err := bind[taskReleaseData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.TaskReleaseRequest{
			Id:      d.Id,
			Counter: d.Version,
		}, nil

	case "task.fulfill":
		d, err := bind[taskFulfillData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.TaskFulfillRequest{
			Id:      d.Id,
			Version: d.Version,
			Action: t_api.PromiseCompleteRequest{
				Id:    d.Action.Id,
				State: d.Action.State,
				Value: d.Action.Value,
			},
		}, nil

	case "task.suspend":
		d, err := bind[taskSuspendData](data)
		if err != nil {
			return nil, err
		}
		actions := make([]t_api.PromiseRegisterRequest, len(d.Actions))
		for i, a := range d.Actions {
			actions[i] = t_api.PromiseRegisterRequest{
				Awaiter: a.Awaiter,
				Awaited: a.Awaited,
			}
		}
		return &t_api.TaskSuspendRequest{
			Id:      d.Id,
			Version: d.Version,
			Actions: actions,
		}, nil

	case "task.heartbeat":
		d, err := bind[taskHeartbeatData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.TaskHeartbeatRequest{
			ProcessId: d.Pid,
		}, nil

	case "schedule.get":
		d, err := bind[scheduleGetData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.ScheduleGetRequest{Id: d.Id}, nil

	case "schedule.create":
		d, err := bind[scheduleCreateData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.ScheduleCreateRequest{
			Id:             d.Id,
			Cron:           d.Cron,
			PromiseId:      d.PromiseId,
			PromiseTimeout: d.PromiseTimeout,
			PromiseParam:   d.PromiseParam,
			PromiseTags:    d.PromiseTags,
		}, nil

	case "schedule.delete":
		d, err := bind[scheduleDeleteData](data)
		if err != nil {
			return nil, err
		}
		return &t_api.ScheduleDeleteRequest{Id: d.Id}, nil

	default:
		return nil, api.RequestValidationError(fmt.Errorf("unknown kind: %s", kind))
	}
}

func (s *server) mapResponse(kind string, res *t_api.Response) *Res {
	status := int(res.Status) / 100

	switch kind {
	case "promise.get":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"promise": mapPromise(res.AsPromiseGetResponse().Promise)},
		}

	case "promise.create":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"promise": mapPromise(res.AsPromiseCreateResponse().Promise)},
		}

	case "promise.settle":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"promise": mapPromise(res.AsPromiseCompleteResponse().Promise)},
		}

	case "promise.register":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"promise": mapPromise(res.AsPromiseRegisterResponse().Promise)},
		}

	case "promise.subscribe":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"promise": mapPromise(res.AsPromiseSubscribeResponse().Promise)},
		}

	case "task.create":
		r := res.AsTaskCreateResponse()
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{
				"task":    map[string]any{"id": r.Task.Id, "version": r.Task.Counter},
				"promise": mapPromise(r.Promise),
			},
		}

	case "task.acquire":
		r := res.AsTaskAcquireResponse()
		data := map[string]any{}
		if r.LeafPromise != nil && r.RootPromise != nil && r.LeafPromise.Id != r.RootPromise.Id {
			data["kind"] = "resume"
			data["data"] = map[string]any{
				"invoked": mapPromise(r.RootPromise),
				"awaited": mapPromise(r.LeafPromise),
			}
		} else {
			data["kind"] = "invoke"
			data["data"] = map[string]any{
				"invoked": mapPromise(r.LeafPromise),
			}
		}
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: data,
		}

	case "task.release":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
		}

	case "task.fulfill":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"promise": mapPromise(res.AsTaskFulfillResponse().Promise)},
		}

	case "task.suspend":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
		}

	case "task.heartbeat":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
		}

	case "schedule.get":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"schedule": mapSchedule(res.AsScheduleGetResponse().Schedule)},
		}

	case "schedule.create":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
			Data: map[string]any{"schedule": mapSchedule(res.AsScheduleCreateResponse().Schedule)},
		}

	case "schedule.delete":
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
		}

	default:
		return &Res{
			Kind: kind,
			Head: map[string]any{"status": status},
		}
	}
}

type scheduleResponse struct {
	Id             string            `json:"id"`
	Cron           string            `json:"cron"`
	PromiseId      string            `json:"promiseId"`
	PromiseTimeout int64             `json:"promiseTimeout"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
	CreatedAt      int64             `json:"createdAt"`
	NextRunAt      int64             `json:"nextRunAt"`
	LastRunAt      *int64            `json:"lastRunAt,omitempty"`
}

func mapSchedule(s *schedule.Schedule) *scheduleResponse {
	if s == nil {
		return nil
	}
	resp := &scheduleResponse{
		Id:             s.Id,
		Cron:           s.Cron,
		PromiseId:      s.PromiseId,
		PromiseTimeout: s.PromiseTimeout,
		PromiseTags:    s.PromiseTags,
		PromiseParam:   s.PromiseParam,
		CreatedAt:      s.CreatedOn,
		NextRunAt:      s.NextRunTime,
		LastRunAt:      s.LastRunTime,
	}
	return resp
}

type promiseResponse struct {
	Id        string            `json:"id"`
	State     promise.State     `json:"state"`
	Param     promise.Value     `json:"param,omitempty"`
	Value     promise.Value     `json:"value,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	TimeoutAt int64             `json:"timeoutAt"`
	CreatedAt *int64            `json:"createdAt,omitempty"`
	SettledAt *int64            `json:"settledAt,omitempty"`
}

// TODO(avillega): This could become unnecesary once we deprecate support for old api
func mapPromise(p *promise.Promise) *promiseResponse {
	if p == nil {
		return nil
	}
	resp := &promiseResponse{
		Id:        p.Id,
		State:     p.State,
		Param:     p.Param,
		Value:     p.Value,
		Tags:      p.Tags,
		TimeoutAt: p.Timeout,
		CreatedAt: p.CreatedOn,
		SettledAt: p.CompletedOn,
	}
	return resp
}

func bind[T any](data json.RawMessage) (*T, error) {
	var d T
	if err := binding.JSON.BindBody(data, &d); err != nil {
		return nil, api.RequestValidationError(err)
	}
	return &d, nil
}
