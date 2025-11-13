package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	i_api "github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type Config struct {
	Addr    string        `flag:"addr" desc:"nats server address" default:"nats://localhost:4222"`
	Subject string        `flag:"subject" desc:"nats subject name" default:"resonate.server"`
	Timeout time.Duration `flag:"timeout" desc:"nats server graceful shutdown timeout" default:"10s"`
}

type Nats struct {
	config         *Config
	conn           *nats.Conn
	subscription   *nats.Subscription
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	// Connect to NATS server
	conn, err := nats.Connect(config.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	n := &Nats{
		config:         config,
		conn:           conn,
		subscription:   nil,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}

	server := &server{api: api.New(a, "nats"), config: config, nats: n}

	// Subscribe to single subject
	sub, err := conn.Subscribe(config.Subject, server.handleRequest)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to subscribe to %s: %w", config.Subject, err)
	}
	n.subscription = sub

	slog.Info("nats subscribed", "subject", config.Subject)

	return n, nil
}

func (n *Nats) String() string {
	return "nats"
}

func (n *Nats) Kind() string {
	return "nats"
}

func (n *Nats) Addr() string {
	return n.conn.ConnectedUrl()
}

func (n *Nats) Start(errors chan<- error) {
	slog.Info("starting nats server", "addr", n.config.Addr, "subject", n.config.Subject)

	// Wait for shutdown signal
	<-n.shutdownCtx.Done()
}

func (n *Nats) Stop() error {
	slog.Info("stopping nats server")

	// Cancel shutdown context
	n.shutdownCancel()

	// Unsubscribe
	if n.subscription != nil {
		if err := n.subscription.Unsubscribe(); err != nil {
			slog.Warn("failed to unsubscribe", "error", err)
		}
	}

	// Drain and close connection
	if err := n.conn.Drain(); err != nil {
		slog.Warn("failed to drain connection", "error", err)
	}
	n.conn.Close()

	return nil
}

// handleRequest routes incoming messages to the appropriate handler based on operation
func (s *server) handleRequest(msg *nats.Msg) {
	var natsReq NatsRequest
	if err := json.Unmarshal(msg.Data, &natsReq); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("invalid request format: %v", err))
		return
	}

	// Route based on operation
	switch natsReq.Operation {
	// Promises
	case "promises.read":
		s.handleReadPromise(msg, &natsReq)
	case "promises.search":
		s.handleSearchPromises(msg, &natsReq)
	case "promises.create":
		s.handleCreatePromise(msg, &natsReq)
	case "promises.createtask":
		s.handleCreatePromiseAndTask(msg, &natsReq)
	case "promises.complete":
		s.handleCompletePromise(msg, &natsReq)
	case "promises.callback":
		s.handleCreateCallback(msg, &natsReq)
	case "promises.subscribe":
		s.handleCreateSubscription(msg, &natsReq)

	// Schedules
	case "schedules.read":
		s.handleReadSchedule(msg, &natsReq)
	case "schedules.search":
		s.handleSearchSchedules(msg, &natsReq)
	case "schedules.create":
		s.handleCreateSchedule(msg, &natsReq)
	case "schedules.delete":
		s.handleDeleteSchedule(msg, &natsReq)

	// Locks
	case "locks.acquire":
		s.handleAcquireLock(msg, &natsReq)
	case "locks.release":
		s.handleReleaseLock(msg, &natsReq)
	case "locks.heartbeat":
		s.handleHeartbeatLocks(msg, &natsReq)

	// Tasks
	case "tasks.claim":
		s.handleClaimTask(msg, &natsReq)
	case "tasks.complete":
		s.handleCompleteTask(msg, &natsReq)
	case "tasks.drop":
		s.handleDropTask(msg, &natsReq)
	case "tasks.heartbeat":
		s.handleHeartbeatTasks(msg, &natsReq)

	default:
		s.respondError(msg, 400, fmt.Sprintf("unknown operation: %s", natsReq.Operation))
	}
}

type server struct {
	api    *api.API
	config *Config
	nats   *Nats
}

// NatsRequest wraps a t_api request with NATS-specific metadata
type NatsRequest struct {
	Operation string            `json:"operation"`
	RequestId string            `json:"requestId,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Payload   json.RawMessage   `json:"payload"`
}

// NatsResponse wraps a response or error for NATS
type NatsResponse struct {
	Success  bool            `json:"success"`
	Response json.RawMessage `json:"response,omitempty"`
	Error    *ErrorResponse  `json:"error,omitempty"`
}

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *server) log(operation string, err error) {
	slog.Debug("nats", "operation", operation, "error", err)
}

func (s *server) code(status t_api.StatusCode) int {
	return int(status) / 100
}

// Helper function to process requests
func (s *server) processRequest(msg *nats.Msg, req *NatsRequest, payload t_api.RequestPayload) {
	defer s.log(req.Operation, nil)

	// Validate payload
	if err := payload.Validate(); err != nil {
		s.respondError(msg, 400, fmt.Sprintf("validation error: %v", err))
		return
	}

	// Process the request
	res, apiErr := s.api.Process(req.RequestId, &t_api.Request{
		Metadata: req.Metadata,
		Payload:  payload,
	})

	if apiErr != nil {
		s.respondError(msg, s.code(apiErr.Code), apiErr.Error())
		return
	}

	// Encode and send response
	responseData, err := json.Marshal(res)
	if err != nil {
		s.respondError(msg, 500, fmt.Sprintf("failed to encode response: %v", err))
		return
	}

	response := &NatsResponse{
		Success:  true,
		Response: responseData,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		s.respondError(msg, 500, fmt.Sprintf("failed to encode response envelope: %v", err))
		return
	}

	if err := msg.Respond(responseBytes); err != nil {
		slog.Error("failed to respond", "error", err, "operation", req.Operation)
	}
}

func (s *server) respondError(msg *nats.Msg, code int, message string) {
	response := &NatsResponse{
		Success: false,
		Error: &ErrorResponse{
			Code:    code,
			Message: message,
		},
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
		return
	}

	if err := msg.Respond(responseBytes); err != nil {
		slog.Error("failed to respond with error", "error", err)
	}
}
