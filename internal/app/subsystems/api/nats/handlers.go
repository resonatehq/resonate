package nats

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/message"
	"github.com/resonatehq/resonate/pkg/promise"
)

// Promise Handlers

func (s *server) handleReadPromise(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.ReadPromiseRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleSearchPromises(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.SearchPromisesRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleCreatePromise(msg *nats.Msg, natsReq *NatsRequest) {
	var payloadRaw struct {
		Id             string            `json:"id"`
		IdempotencyKey string            `json:"idempotencyKey,omitempty"`
		Strict         bool              `json:"strict"`
		Param          promise.Value     `json:"param"`
		Timeout        int64             `json:"timeout"`
		Tags           map[string]string `json:"tags,omitempty"`
	}

	if err := json.Unmarshal(natsReq.Payload, &payloadRaw); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	var idempotencyKey *idempotency.Key
	if payloadRaw.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(payloadRaw.IdempotencyKey))
	}

	payload := &t_api.CreatePromiseRequest{
		Id:             payloadRaw.Id,
		IdempotencyKey: idempotencyKey,
		Strict:         payloadRaw.Strict,
		Param:          payloadRaw.Param,
		Timeout:        payloadRaw.Timeout,
		Tags:           payloadRaw.Tags,
	}

	s.processRequest(msg, natsReq, payload)
}

func (s *server) handleCreatePromiseAndTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payloadRaw struct {
		Id             string            `json:"id"`
		IdempotencyKey string            `json:"idempotencyKey,omitempty"`
		Strict         bool              `json:"strict"`
		Param          promise.Value     `json:"param"`
		Timeout        int64             `json:"timeout"`
		Tags           map[string]string `json:"tags,omitempty"`
		TaskProcessId  string            `json:"taskProcessId,omitempty"`
		TaskTtl        int64             `json:"taskTtl,omitempty"`
		TaskTimeout    int64             `json:"taskTimeout,omitempty"`
	}

	if err := json.Unmarshal(natsReq.Payload, &payloadRaw); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	var idempotencyKey *idempotency.Key
	if payloadRaw.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(payloadRaw.IdempotencyKey))
	}

	payload := &t_api.CreatePromiseAndTaskRequest{
		Promise: &t_api.CreatePromiseRequest{
			Id:             payloadRaw.Id,
			IdempotencyKey: idempotencyKey,
			Strict:         payloadRaw.Strict,
			Param:          payloadRaw.Param,
			Timeout:        payloadRaw.Timeout,
			Tags:           payloadRaw.Tags,
		},
		Task: &t_api.CreateTaskRequest{
			PromiseId: payloadRaw.Id,
			ProcessId: payloadRaw.TaskProcessId,
			Ttl:       payloadRaw.TaskTtl,
			Timeout:   payloadRaw.TaskTimeout,
		},
	}

	s.processRequest(msg, natsReq, payload)
}

func (s *server) handleCompletePromise(msg *nats.Msg, natsReq *NatsRequest) {
	var payloadRaw struct {
		Id             string            `json:"id"`
		IdempotencyKey string            `json:"idempotencyKey,omitempty"`
		Strict         bool              `json:"strict"`
		State          string            `json:"state"`
		Value          promise.Value     `json:"value"`
	}

	if err := json.Unmarshal(natsReq.Payload, &payloadRaw); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	var idempotencyKey *idempotency.Key
	if payloadRaw.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(payloadRaw.IdempotencyKey))
	}

	var state promise.State
	switch payloadRaw.State {
	case "RESOLVED":
		state = promise.Resolved
	case "REJECTED":
		state = promise.Rejected
	case "REJECTED_CANCELED":
		state = promise.Canceled
	case "REJECTED_TIMEDOUT":
		state = promise.Timedout
	default:
		s.respondError(msg, 400, fmt.Sprintf("invalid state: %s", payloadRaw.State))
		return
	}

	payload := &t_api.CompletePromiseRequest{
		Id:             payloadRaw.Id,
		IdempotencyKey: idempotencyKey,
		Strict:         payloadRaw.Strict,
		State:          state,
		Value:          payloadRaw.Value,
	}

	s.processRequest(msg, natsReq, payload)
}

func (s *server) handleCreateCallback(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.CreateCallbackRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleCreateSubscription(msg *nats.Msg, natsReq *NatsRequest) {
	var payloadRaw struct {
		Id        string          `json:"id"`
		PromiseId string          `json:"promiseId"`
		Recv      json.RawMessage `json:"recv"`
		Timeout   int64           `json:"timeout"`
	}

	if err := json.Unmarshal(natsReq.Payload, &payloadRaw); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	head := map[string]string{}
	if natsReq.Metadata != nil {
		if traceparent, ok := natsReq.Metadata["traceparent"]; ok {
			head["traceparent"] = traceparent
			if tracestate, ok := natsReq.Metadata["tracestate"]; ok {
				head["tracestate"] = tracestate
			}
		}
	}

	payload := &t_api.CreateCallbackRequest{
		Id:        util.NotifyId(payloadRaw.PromiseId, payloadRaw.Id),
		PromiseId: payloadRaw.PromiseId,
		Recv:      payloadRaw.Recv,
		Mesg:      &message.Mesg{Type: "notify", Head: head, Root: payloadRaw.PromiseId},
		Timeout:   payloadRaw.Timeout,
	}

	s.processRequest(msg, natsReq, payload)
}

// Schedule Handlers

func (s *server) handleReadSchedule(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.ReadScheduleRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleSearchSchedules(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.SearchSchedulesRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleCreateSchedule(msg *nats.Msg, natsReq *NatsRequest) {
	var payloadRaw struct {
		Id             string            `json:"id"`
		Description    string            `json:"description,omitempty"`
		Cron           string            `json:"cron"`
		Tags           map[string]string `json:"tags,omitempty"`
		PromiseId      string            `json:"promiseId"`
		PromiseTimeout int64             `json:"promiseTimeout"`
		PromiseParam   promise.Value     `json:"promiseParam"`
		PromiseTags    map[string]string `json:"promiseTags,omitempty"`
		IdempotencyKey string            `json:"idempotencyKey,omitempty"`
	}

	if err := json.Unmarshal(natsReq.Payload, &payloadRaw); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	var idempotencyKey *idempotency.Key
	if payloadRaw.IdempotencyKey != "" {
		idempotencyKey = util.ToPointer(idempotency.Key(payloadRaw.IdempotencyKey))
	}

	payload := &t_api.CreateScheduleRequest{
		Id:             payloadRaw.Id,
		Description:    payloadRaw.Description,
		Cron:           payloadRaw.Cron,
		Tags:           payloadRaw.Tags,
		PromiseId:      payloadRaw.PromiseId,
		PromiseTimeout: payloadRaw.PromiseTimeout,
		PromiseParam:   payloadRaw.PromiseParam,
		PromiseTags:    payloadRaw.PromiseTags,
		IdempotencyKey: idempotencyKey,
	}

	s.processRequest(msg, natsReq, payload)
}

func (s *server) handleDeleteSchedule(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.DeleteScheduleRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

// Lock Handlers

func (s *server) handleAcquireLock(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.AcquireLockRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleReleaseLock(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.ReleaseLockRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleHeartbeatLocks(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.HeartbeatLocksRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

// Task Handlers

func (s *server) handleClaimTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payloadRaw struct {
		Id        string `json:"id"`
		Counter   int    `json:"counter"`
		ProcessId string `json:"processId"`
		Ttl       int64  `json:"ttl"`
	}

	if err := json.Unmarshal(natsReq.Payload, &payloadRaw); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	payload := &t_api.ClaimTaskRequest{
		Id:        payloadRaw.Id,
		Counter:   payloadRaw.Counter,
		ProcessId: payloadRaw.ProcessId,
		Ttl:       payloadRaw.Ttl,
	}

	s.processRequest(msg, natsReq, payload)
}

func (s *server) handleCompleteTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.CompleteTaskRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleDropTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.DropTaskRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}

func (s *server) handleHeartbeatTasks(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.HeartbeatTasksRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid payload: %v", err))
		return
	}

	s.processRequest(msg, natsReq, &payload)
}
