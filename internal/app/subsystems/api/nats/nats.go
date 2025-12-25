package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/go-viper/mapstructure/v2"
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
	IKey   *idempotency.Key  `json:"iKey,omitempty"`
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
	URL     string        `flag:"url" desc:"nats server URL" default:"nats://localhost:4222"`
	Subject string        `flag:"subject" desc:"nats request subject" default:"resonate"`
	Target  string        `flag:"target" desc:"target identifier for this server" default:"resonate.server"`
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

type NATS struct {
	config         *Config
	api            *api.API
	nc             *natsgo.Conn
	sub            *natsgo.Subscription
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	nc, err := natsgo.Connect(config.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	n := &NATS{
		config:         config,
		api:            api.New(a, "nats"),
		nc:             nc,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}

	slog.Debug("nats subsystem initialized",
		"url", config.URL,
		"subject", config.Subject,
		"target", config.Target,
	)

	return n, nil
}

func (n *NATS) String() string {
	return "nats"
}

func (n *NATS) Kind() string {
	return "nats"
}

func (n *NATS) Addr() string {
	return n.config.URL
}

func (n *NATS) Start(errors chan<- error) {
	slog.Info("starting nats consumer", "subject", n.config.Subject, "target", n.config.Target)

	server := &server{
		api:    n.api,
		config: n.config,
		nats:   n,
	}

	sub, err := n.nc.Subscribe(n.config.Subject, func(msg *natsgo.Msg) {
		server.handleRequest(msg)
	})
	if err != nil {
		slog.Error("nats subscription error", "error", err)
		errors <- err
		return
	}
	n.sub = sub

	<-n.shutdownCtx.Done()
}

func (n *NATS) Stop() error {
	n.shutdownCancel()

	if n.sub != nil {
		if err := n.sub.Unsubscribe(); err != nil {
			slog.Warn("failed to unsubscribe", "error", err)
		}
	}

	if n.nc != nil {
		n.nc.Close()
	}

	return nil
}

type server struct {
	api    *api.API
	config *Config
	nats   *NATS
}

type NATSRequest struct {
	Target        string            `json:"target"`
	ReplyTo       string            `json:"replyTo"`
	CorrelationId string            `json:"correlationId"`
	Operation     string            `json:"operation"`
	RequestId     string            `json:"requestId,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Payload       json.RawMessage   `json:"payload"`
}

type NATSResponse struct {
	Target        string          `json:"target"`
	CorrelationId string          `json:"correlationId"`
	Operation     string          `json:"operation"`
	Success       bool            `json:"success"`
	Response      json.RawMessage `json:"response,omitempty"`
	Error         *api.Error      `json:"error,omitempty"`
}

func (s *server) handleRequest(msg *natsgo.Msg) {
	var natsReq NATSRequest
	if err := json.Unmarshal(msg.Data, &natsReq); err != nil {
		slog.Warn("failed to unmarshal nats message",
			"error", err,
			"subject", msg.Subject,
		)
		return
	}

	if natsReq.Target != s.config.Target {
		slog.Debug("discarding message with wrong target",
			"expected", s.config.Target,
			"actual", natsReq.Target,
			"operation", natsReq.Operation,
		)
		return
	}

	switch natsReq.Operation {
	case "promises.read":
		s.handleReadPromise(&natsReq)
	case "promises.search":
		s.handleSearchPromises(&natsReq)
	case "promises.create":
		s.handleCreatePromise(&natsReq)
	case "promises.createtask":
		s.handleCreatePromiseAndTask(&natsReq)
	case "promises.complete":
		s.handleCompletePromise(&natsReq)
	case "promises.callback":
		s.handleCreateCallback(&natsReq)
	case "promises.subscribe":
		s.handleCreateSubscription(&natsReq)
	case "schedules.read":
		s.handleReadSchedule(&natsReq)
	case "schedules.search":
		s.handleSearchSchedules(&natsReq)
	case "schedules.create":
		s.handleCreateSchedule(&natsReq)
	case "schedules.delete":
		s.handleDeleteSchedule(&natsReq)
	case "locks.acquire":
		s.handleAcquireLock(&natsReq)
	case "locks.release":
		s.handleReleaseLock(&natsReq)
	case "locks.heartbeat":
		s.handleHeartbeatLocks(&natsReq)
	case "tasks.claim":
		s.handleClaimTask(&natsReq)
	case "tasks.complete":
		s.handleCompleteTask(&natsReq)
	case "tasks.drop":
		s.handleDropTask(&natsReq)
	case "tasks.heartbeat":
		s.handleHeartbeatTasks(&natsReq)
	default:
		s.respondError(&natsReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("unknown operation: %s", natsReq.Operation),
		})
	}
}

func (s *server) log(operation string, err error) {
	slog.Debug("nats", "operation", operation, "error", err)
}

func (s *server) processRequest(natsReq *NATSRequest, payload t_api.RequestPayload) (t_api.ResponsePayload, *api.Error) {
	defer s.log(natsReq.Operation, nil)

	res, apiErr := s.api.Process(natsReq.RequestId, &t_api.Request{
		Metadata: natsReq.Metadata,
		Payload:  payload,
	})

	if apiErr != nil {
		return nil, apiErr
	}

	return res.Payload, nil
}

func (s *server) respondError(natsReq *NATSRequest, error *api.Error) {
	response := &NATSResponse{
		Target:        natsReq.ReplyTo,
		CorrelationId: natsReq.CorrelationId,
		Operation:     natsReq.Operation,
		Success:       false,
		Error:         error,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
		return
	}

	s.send(natsReq.ReplyTo, responseBytes)
}

func (s *server) sendReply(natsReq *NATSRequest, data []byte) {
	response := &NATSResponse{
		Target:        natsReq.ReplyTo,
		CorrelationId: natsReq.CorrelationId,
		Operation:     natsReq.Operation,
		Success:       true,
		Response:      data,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode response", "error", err)
		return
	}

	s.send(natsReq.ReplyTo, responseBytes)
}

func (s *server) send(subject string, value []byte) {
	if subject == "" {
		slog.Warn("no reply subject provided")
		return
	}

	if err := s.nats.nc.Publish(subject, value); err != nil {
		slog.Error("failed to send nats message", "error", err, "subject", subject)
	}
}
