package nats

import (
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/resonatehq/resonate/pkg/message"
)

// Promise Handlers

func (s *server) handleReadPromise(msg *nats.Msg, natsReq *NatsRequest) {
	var payload ReadPromisePayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.ReadPromiseRequest{
		Id: payload.ID,
	})

	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.ReadPromiseResponse)
	responseBytes, err := json.Marshal(responseData.Promise)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleSearchPromises(msg *nats.Msg, natsReq *NatsRequest) {
	var payload SearchPromisesPayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	req, error := s.api.SearchPromises(
		payload.ID,
		util.SafeDeref(payload.State),
		nil,
		util.SafeDeref(payload.Limit),
		util.SafeDeref(payload.Cursor),
	)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	res, error := s.processRequest(natsReq, req)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.SearchPromisesResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"promises": responseData.Promises,
		"cursor":   responseData.Cursor,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCreatePromise(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CreatePromisePayload

	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CreatePromiseRequest{
		Id:             payload.ID,
		IdempotencyKey: payload.IKey,
		Strict:         payload.Strict,
		Param:          payload.Param,
		Timeout:        payload.Timeout,
		Tags:           payload.Tags,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CreatePromiseResponse)
	responseBytes, err := json.Marshal(responseData.Promise)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCreatePromiseAndTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CreatePromiseAndTaskPayload

	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CreatePromiseAndTaskRequest{
		Promise: &t_api.CreatePromiseRequest{
			Id:             payload.Promise.ID,
			IdempotencyKey: payload.IKey,
			Strict:         payload.Strict,
			Param:          payload.Promise.Param,
			Timeout:        payload.Promise.Timeout,
			Tags:           payload.Promise.Tags,
		},
		Task: &t_api.CreateTaskRequest{
			PromiseId: payload.Promise.ID,
			ProcessId: payload.Task.ProcessID,
			Ttl:       payload.Task.TTL,
			Timeout:   payload.Promise.Timeout,
		},
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CreatePromiseAndTaskResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"promise": responseData.Promise,
		"task":    responseData.Task,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCompletePromise(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CompletePromisePayload

	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CompletePromiseRequest{
		Id:             payload.ID,
		IdempotencyKey: payload.IKey,
		Strict:         payload.Strict,
		State:          payload.State,
		Value:          payload.Value,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CompletePromiseResponse)
	responseBytes, err := json.Marshal(responseData.Promise)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCreateCallback(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CreateCallbackPayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CreateCallbackRequest{
		Id:        util.ResumeId(payload.RootPromiseID, payload.PromiseID),
		PromiseId: payload.PromiseID,
		Recv:      payload.Recv,
		Mesg:      &message.Mesg{Type: "resume", Head: nil, Root: payload.RootPromiseID, Leaf: payload.PromiseID},
		Timeout:   payload.Timeout,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CreateCallbackResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"callback": responseData.Callback,
		"promise":  responseData.Promise,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCreateSubscription(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CreateSubscriptionPayload

	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CreateCallbackRequest{
		Id:        util.NotifyId(payload.PromiseID, payload.ID),
		PromiseId: payload.PromiseID,
		Recv:      payload.Recv,
		Mesg:      &message.Mesg{Type: "notify", Head: nil, Root: payload.PromiseID},
		Timeout:   payload.Timeout,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CreateCallbackResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"callback": responseData.Callback,
		"promise":  responseData.Promise,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

// Schedule Handlers

func (s *server) handleReadSchedule(msg *nats.Msg, natsReq *NatsRequest) {
	var payload ReadSchedulePayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.ReadScheduleRequest{
		Id: payload.ID,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.ReadScheduleResponse)
	responseBytes, err := json.Marshal(responseData.Schedule)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleSearchSchedules(msg *nats.Msg, natsReq *NatsRequest) {
	var payload SearchSchedulesPayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	req, error := s.api.SearchSchedules(
		util.SafeDeref(payload.ID),
		nil,
		util.SafeDeref(payload.Limit),
		util.SafeDeref(payload.Cursor),
	)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	res, error := s.processRequest(natsReq, req)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.SearchSchedulesResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"schedules": responseData.Schedules,
		"cursor":    responseData.Cursor,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCreateSchedule(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CreateSchedulePayload

	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CreateScheduleRequest{
		Id:             payload.ID,
		Description:    payload.Description,
		Cron:           payload.Cron,
		Tags:           payload.Tags,
		PromiseId:      payload.PromiseID,
		PromiseTimeout: payload.PromiseTimeout,
		PromiseParam:   payload.PromiseParam,
		PromiseTags:    payload.PromiseTags,
		IdempotencyKey: payload.IKey,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CreateScheduleResponse)
	responseBytes, err := json.Marshal(responseData.Schedule)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleDeleteSchedule(msg *nats.Msg, natsReq *NatsRequest) {
	var payload DeleteSchedulePayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	_, error := s.processRequest(natsReq, &t_api.DeleteScheduleRequest{
		Id: payload.ID,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	s.sendReply(msg, natsReq, nil)
}

// Lock Handlers

func (s *server) handleAcquireLock(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.AcquireLockRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &payload)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.AcquireLockResponse)
	responseBytes, err := json.Marshal(responseData.Lock)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleReleaseLock(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.ReleaseLockRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	_, error := s.processRequest(natsReq, &payload)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	s.sendReply(msg, natsReq, nil)
}

func (s *server) handleHeartbeatLocks(msg *nats.Msg, natsReq *NatsRequest) {
	var payload t_api.HeartbeatLocksRequest
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &payload)
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.HeartbeatLocksResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"locksAffected": responseData.LocksAffected,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

// Task Handlers

func (s *server) handleClaimTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payload ClaimTaskPayload

	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.ClaimTaskRequest{
		Id:        payload.ID,
		Counter:   payload.Counter,
		ProcessId: payload.ProcessID,
		Ttl:       payload.TTL,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.ClaimTaskResponse)

	promises := map[string]any{
		"root": map[string]any{
			"id":   responseData.Task.Mesg.Root,
			"href": responseData.RootPromiseHref,
			"data": responseData.RootPromise,
		},
	}
	if responseData.Task.Mesg.Type == message.Resume {
		promises["leaf"] = map[string]any{
			"id":   responseData.Task.Mesg.Leaf,
			"href": responseData.LeafPromiseHref,
			"data": responseData.LeafPromise,
		}
	}

	responseBytes, err := json.Marshal(map[string]any{
		"type":     responseData.Task.Mesg.Type,
		"promises": promises,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleCompleteTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payload CompleteTaskPayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.CompleteTaskRequest{
		Id:      payload.ID,
		Counter: payload.Counter,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.CompleteTaskResponse)
	responseBytes, err := json.Marshal(responseData.Task)
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}

func (s *server) handleDropTask(msg *nats.Msg, natsReq *NatsRequest) {
	var payload DropTaskPayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	_, error := s.processRequest(natsReq, &t_api.DropTaskRequest{
		Id:      payload.ID,
		Counter: payload.Counter,
	})

	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	s.sendReply(msg, natsReq, nil)
}

func (s *server) handleHeartbeatTasks(msg *nats.Msg, natsReq *NatsRequest) {
	var payload HeartbeatTasksPayload
	if err := json.Unmarshal(natsReq.Payload, &payload); err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(natsReq, &t_api.HeartbeatTasksRequest{
		ProcessId: payload.ProcessID,
	})
	if error != nil {
		s.respondError(msg, natsReq, error)
		return
	}

	responseData := res.(*t_api.HeartbeatTasksResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"tasksAffected": responseData.TasksAffected,
	})
	if err != nil {
		s.respondError(msg, natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(msg, natsReq, responseBytes)
}
