package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	i_api "github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/idempotency"
	"github.com/resonatehq/resonate/pkg/promise"
)

// ----------------- PROMISES -----------------

type CreatePromisePayload struct {
	ID      string            `json:"id"`
	Timeout int64             `json:"timeout"`
	Param   promise.Value     `json:"param,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	IKey    *idempotency.Key  `json:"iKey,omitempty"`
	Strict  bool              `json:"strict,omitempty"`
}

type CreatePromiseAndTaskPayload struct {
	Promise CreatePromisePayload `json:"promise"`
	Task    CreateTaskPayload    `json:"task"`
	IKey    *idempotency.Key     `json:"iKey,omitempty"`
	Strict  bool                 `json:"strict,omitempty"`
}

type CreateTaskPayload struct {
	ProcessID string `json:"processId"`
	TTL       int64  `json:"ttl"`
}

type ReadPromisePayload struct {
	ID string `json:"id"`
}

type CompletePromisePayload struct {
	ID     string           `json:"id"`
	State  promise.State    `json:"state"`
	Value  promise.Value    `json:"value,omitempty"`
	IKey   *idempotency.Key `json:"iKey,omitempty"`
	Strict bool             `json:"strict,omitempty"`
}

type CreateCallbackPayload struct {
	PromiseID     string          `json:"promiseId"`
	RootPromiseID string          `json:"rootPromiseId"`
	Timeout       int64           `json:"timeout"`
	Recv          json.RawMessage `json:"recv"`
}

type CreateSubscriptionPayload struct {
	ID        string          `json:"id"`
	PromiseID string          `json:"promiseId"`
	Timeout   int64           `json:"timeout"`
	Recv      json.RawMessage `json:"recv"`
}

type SearchPromisesPayload struct {
	ID     string  `json:"id"`
	State  *string `json:"state,omitempty"`
	Limit  *int    `json:"limit,omitempty"`
	Cursor *string `json:"cursor,omitempty"`
}

// ----------------- SCHEDULES -----------------

type CreateSchedulePayload struct {
	ID             string            `json:"id,omitempty"`
	Description    string            `json:"description,omitempty"`
	Cron           string            `json:"cron,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
	PromiseID      string            `json:"promiseId,omitempty"`
	PromiseTimeout int64             `json:"promiseTimeout,omitempty"`
	PromiseParam   promise.Value     `json:"promiseParam,omitempty"`
	PromiseTags    map[string]string `json:"promiseTags,omitempty"`
	IKey           *idempotency.Key  `json:"iKey,omitempty"`
}

type ReadSchedulePayload struct {
	ID string `json:"id"`
}

type DeleteSchedulePayload struct {
	ID string `json:"id"`
}

type SearchSchedulesPayload struct {
	ID     *string `json:"id"`
	Limit  *int    `json:"limit,omitempty"`
	Cursor *string `json:"cursor,omitempty"`
}

// ----------------- TASKS -----------------

type ClaimTaskPayload struct {
	ID        string `json:"id"`
	Counter   int    `json:"counter"`
	ProcessID string `json:"processId"`
	TTL       int64  `json:"ttl"`
}

type CompleteTaskPayload struct {
	ID      string `json:"id"`
	Counter int    `json:"counter"`
}

type DropTaskPayload struct {
	ID      string `json:"id"`
	Counter int    `json:"counter"`
}

type HeartbeatTasksPayload struct {
	ProcessID string `json:"processId"`
}

type Config struct {
	Servers []string      `flag:"servers" desc:"nats server addresses" default:"localhost:4222"`
	Subject string        `flag:"subject" desc:"nats request subject" default:"resonate.requests"`
	Queue   string        `flag:"queue" desc:"nats queue group for load balancing" default:"resonate-servers"`
	Timeout time.Duration `flag:"timeout" desc:"nats server graceful shutdown timeout" default:"10s"`
}

func (c *Config) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string, prefix string, keyPrefix string) {
	cmdUtil.Bind(c, cmd, flg, vip, name, prefix, keyPrefix)
}

func (c *Config) Decode(value any, decodeHook mapstructure.DecodeHookFunc) error {
	decoderConfig := &mapstructure.DecoderConfig{
		Result:     c,
		DecodeHook: decodeHook,
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return err
	}

	if err := decoder.Decode(value); err != nil {
		return err
	}

	return nil
}

func (c *Config) New(a i_api.API, _ *metrics.Metrics) (i_api.Subsystem, error) {
	return New(a, c)
}

type Nats struct {
	config         *Config
	api            *api.API
	conn           *nats.Conn
	subscription   *nats.Subscription
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	servers := strings.Join(config.Servers, ",")

	// Connect to NATS
	conn, err := nats.Connect(servers)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	n := &Nats{
		config:         config,
		api:            api.New(a, "nats"),
		conn:           conn,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}

	slog.Debug("nats initialized",
		"servers", config.Servers,
		"subject", config.Subject,
		"queue", config.Queue,
	)

	return n, nil
}

func (n *Nats) String() string {
	return "nats"
}

func (n *Nats) Kind() string {
	return "nats"
}

func (n *Nats) Addr() string {
	return fmt.Sprintf("%v", n.config.Servers)
}

func (n *Nats) Start(errors chan<- error) {
	slog.Info("starting nats subscriber", "subject", n.config.Subject, "queue", n.config.Queue)

	server := &server{
		api:    n.api,
		config: n.config,
		nats:   n,
	}

	// Subscribe with queue group for load balancing
	sub, err := n.conn.QueueSubscribe(n.config.Subject, n.config.Queue, func(msg *nats.Msg) {
		server.handleRequest(msg)
	})
	if err != nil {
		slog.Error("nats subscription error", "error", err)
		errors <- err
		return
	}

	n.subscription = sub

	// Block until shutdown
	<-n.shutdownCtx.Done()
}

func (n *Nats) Stop() error {
	// Cancel shutdown context
	n.shutdownCancel()

	// Drain subscription
	if n.subscription != nil {
		if err := n.subscription.Drain(); err != nil {
			slog.Warn("failed to drain subscription", "error", err)
		}
	}

	// Close connection
	if n.conn != nil {
		n.conn.Close()
	}

	return nil
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
	Operation string          `json:"operation"`
	Success   bool            `json:"success"`
	Response  json.RawMessage `json:"response,omitempty"`
	Error     *api.Error      `json:"error,omitempty"`
}

func (s *server) handleRequest(msg *nats.Msg) {
	var natsReq NatsRequest
	if err := json.Unmarshal(msg.Data, &natsReq); err != nil {
		slog.Warn("failed to unmarshal nats message",
			"error", err,
			"subject", msg.Subject,
		)
		s.respondErrorDirect(msg, "unknown", &api.Error{
			Code:    400,
			Message: fmt.Sprintf("invalid request format: %v", err),
		})
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
		s.respondError(msg, &natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("unknown operation: %s", natsReq.Operation),
		})
	}
}

func (s *server) log(operation string, err error) {
	slog.Debug("nats", "operation", operation, "error", err)
}

// Helper function to process requests
func (s *server) processRequest(natsReq *NatsRequest, payload t_api.RequestPayload) (t_api.ResponsePayload, *api.Error) {
	defer s.log(natsReq.Operation, nil)

	// Process the request
	res, apiErr := s.api.Process(natsReq.RequestId, &t_api.Request{
		Metadata: natsReq.Metadata,
		Payload:  payload,
	})

	if apiErr != nil {
		return nil, apiErr
	}

	return res.Payload, nil
}

func (s *server) respondErrorDirect(msg *nats.Msg, operation string, error *api.Error) {
	response := &NatsResponse{
		Operation: operation,
		Success:   false,
		Error:     error,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
		return
	}

	s.send(msg, responseBytes)
}

func (s *server) respondError(msg *nats.Msg, natsReq *NatsRequest, error *api.Error) {
	response := &NatsResponse{
		Operation: natsReq.Operation,
		Success:   false,
		Error:     error,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
		return
	}

	s.send(msg, responseBytes)
}

func (s *server) sendReply(msg *nats.Msg, natsReq *NatsRequest, data []byte) {
	response := &NatsResponse{
		Operation: natsReq.Operation,
		Success:   true,
		Response:  data,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode response", "error", err)
		return
	}

	s.send(msg, responseBytes)
}

func (s *server) send(msg *nats.Msg, value []byte) {
	// Use NATS request-reply pattern - respond to the reply subject
	if msg.Reply == "" {
		slog.Warn("no reply subject in nats message, cannot send response")
		return
	}

	if err := s.nats.conn.Publish(msg.Reply, value); err != nil {
		slog.Error("failed to send reply", "error", err, "reply", msg.Reply)
		return
	}

	slog.Debug("sent reply", "reply", msg.Reply)
}
