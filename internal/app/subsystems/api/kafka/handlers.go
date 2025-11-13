package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/pkg/message"
)

// Promise Handlers

func (s *server) handleReadPromise(kafkaReq *KafkaRequest) {
	var payload t_api.ReadPromiseRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.ReadPromiseResponse)
	responseBytes, err := json.Marshal(responseData.Promise)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleSearchPromises(kafkaReq *KafkaRequest) {
	var payload t_api.SearchPromisesRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.SearchPromisesResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"promises": responseData.Promises,
		"cursor":   responseData.Cursor,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCreatePromise(kafkaReq *KafkaRequest) {
	var payload t_api.CreatePromiseRequest

	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CreatePromiseResponse)
	responseBytes, err := json.Marshal(responseData.Promise)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCreatePromiseAndTask(kafkaReq *KafkaRequest) {
	var payload t_api.CreatePromiseAndTaskRequest

	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}
	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CreatePromiseAndTaskResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"promise": responseData.Promise,
		"task":    responseData.Task,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCompletePromise(kafkaReq *KafkaRequest) {
	var payload t_api.CompletePromiseRequest

	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CompletePromiseResponse)
	responseBytes, err := json.Marshal(responseData.Promise)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCreateCallback(kafkaReq *KafkaRequest) {
	var payload t_api.CreateCallbackRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CreateCallbackResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"callback": responseData.Callback,
		"promise":  responseData.Promise,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCreateSubscription(kafkaReq *KafkaRequest) {
	var payload t_api.CreateCallbackRequest

	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CreateCallbackResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"callback": responseData.Callback,
		"promise":  responseData.Promise,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

// Schedule Handlers

func (s *server) handleReadSchedule(kafkaReq *KafkaRequest) {
	var payload t_api.ReadScheduleRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.ReadScheduleResponse)
	responseBytes, err := json.Marshal(responseData.Schedule)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleSearchSchedules(kafkaReq *KafkaRequest) {
	var payload t_api.SearchSchedulesRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.SearchSchedulesResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"schedules": responseData.Schedules,
		"cursor":    responseData.Cursor,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCreateSchedule(kafkaReq *KafkaRequest) {
	var payload t_api.CreateScheduleRequest

	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CreateScheduleResponse)
	responseBytes, err := json.Marshal(responseData.Schedule)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleDeleteSchedule(kafkaReq *KafkaRequest) {
	var payload t_api.DeleteScheduleRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	_, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	s.sendReply(kafkaReq, nil)
}

// Lock Handlers

func (s *server) handleAcquireLock(kafkaReq *KafkaRequest) {
	var payload t_api.AcquireLockRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.AcquireLockResponse)
	responseBytes, err := json.Marshal(responseData.Lock)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleReleaseLock(kafkaReq *KafkaRequest) {
	var payload t_api.ReleaseLockRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	_, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	s.sendReply(kafkaReq, nil)
}

func (s *server) handleHeartbeatLocks(kafkaReq *KafkaRequest) {
	var payload t_api.HeartbeatLocksRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.HeartbeatLocksResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"locksAffected": responseData.LocksAffected,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

// Task Handlers

func (s *server) handleClaimTask(kafkaReq *KafkaRequest) {
	var payload t_api.ClaimTaskRequest

	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
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
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleCompleteTask(kafkaReq *KafkaRequest) {
	var payload t_api.CompleteTaskRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.CompleteTaskResponse)
	responseBytes, err := json.Marshal(responseData.Task)
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}

func (s *server) handleDropTask(kafkaReq *KafkaRequest) {
	var payload t_api.DropTaskRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	_, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	s.sendReply(kafkaReq, nil)
}

func (s *server) handleHeartbeatTasks(kafkaReq *KafkaRequest) {
	var payload t_api.HeartbeatTasksRequest
	if err := json.Unmarshal(kafkaReq.Payload, &payload); err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("invalid payload: %v", err),
		})
		return
	}

	res, error := s.processRequest(kafkaReq, &payload)
	if error != nil {
		s.respondError(kafkaReq, error)
		return
	}

	responseData := res.(*t_api.HeartbeatTasksResponse)
	responseBytes, err := json.Marshal(map[string]any{
		"tasksAffected": responseData.TasksAffected,
	})
	if err != nil {
		s.respondError(kafkaReq, &ErrorResponse{
			Code:    400,
			Message: fmt.Sprintf("failed to encode response envelope: %v", err),
		})
		return
	}

	s.sendReply(kafkaReq, responseBytes)
}
