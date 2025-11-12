package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	i_api "github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type Config struct {
	Brokers       []string      `flag:"brokers" desc:"kafka broker addresses" default:"localhost:9092"`
	Topic         string        `flag:"topic" desc:"kafka topic to consume from" default:"resonate.requests"`
	Target        string        `flag:"target" desc:"target identifier for this server" default:"resonate.server"`
	ConsumerGroup string        `flag:"consumer-group" desc:"kafka consumer group" default:"resonate-servers"`
	Timeout       time.Duration `flag:"timeout" desc:"kafka server graceful shutdown timeout" default:"10s"`
}

type Kafka struct {
	config         *Config
	api            *api.API
	consumerGroup  sarama.ConsumerGroup
	producer       sarama.SyncProducer
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	// Configure Sarama
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V3_0_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup(config.Brokers, config.ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer group: %w", err)
	}

	// Create producer for sending replies
	producer, err := sarama.NewSyncProducer(config.Brokers, saramaConfig)
	if err != nil {
		consumerGroup.Close()
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	k := &Kafka{
		config:         config,
		api:            api.New(a, "kafka"),
		consumerGroup:  consumerGroup,
		producer:       producer,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}

	slog.Info("kafka initialized", "brokers", config.Brokers, "topic", config.Topic, "target", config.Target, "consumerGroup", config.ConsumerGroup)

	return k, nil
}

func (k *Kafka) String() string {
	return "kafka"
}

func (k *Kafka) Kind() string {
	return "kafka"
}

func (k *Kafka) Addr() string {
	return fmt.Sprintf("%v", k.config.Brokers)
}

func (k *Kafka) Start(errors chan<- error) {
	slog.Info("starting kafka consumer", "topic", k.config.Topic, "target", k.config.Target)

	// Note: api will be set when this subsystem is added via AddSubsystem
	handler := &consumerGroupHandler{
		config: k.config,
		kafka:  k,
	}

	// Consume messages in a loop (handles rebalancing)
	go func() {
		for {
			select {
			case <-k.shutdownCtx.Done():
				return
			default:
				if err := k.consumerGroup.Consume(k.shutdownCtx, []string{k.config.Topic}, handler); err != nil {
					select {
					case <-k.shutdownCtx.Done():
						return
					default:
						slog.Error("kafka consumer error", "error", err)
						errors <- err
						return
					}
				}
			}
		}
	}()

	// Wait for shutdown signal
	<-k.shutdownCtx.Done()
}

func (k *Kafka) Stop() error {
	slog.Info("stopping kafka server")

	// Cancel shutdown context
	k.shutdownCancel()

	// Close consumer group
	if err := k.consumerGroup.Close(); err != nil {
		slog.Warn("failed to close consumer group", "error", err)
	}

	// Close producer
	if err := k.producer.Close(); err != nil {
		slog.Warn("failed to close producer", "error", err)
	}

	return nil
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	config *Config
	kafka  *Kafka
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	server := &server{
		api:     h.kafka.api,
		config:  h.config,
		kafka:   h.kafka,
		session: session,
	}

	for {
		select {
		case <-session.Context().Done():
			return nil
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}
			server.handleRequest(message)
			session.MarkMessage(message, "")
		}
	}
}

type server struct {
	api     *api.API
	config  *Config
	kafka   *Kafka
	session sarama.ConsumerGroupSession
}

// KafkaRequest wraps a t_api request with Kafka-specific metadata
type KafkaRequest struct {
	Target        string            `json:"target"`
	ReplyTo       ReplyTo           `json:"replyTo"`
	CorrelationId string            `json:"correlationId"`
	Operation     string            `json:"operation"`
	RequestId     string            `json:"requestId,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Payload       json.RawMessage   `json:"payload"`
}

type ReplyTo struct {
	Topic        string `json:"topic"`
	Target       string `json:"target"`
	PartitionKey string `json:"partitionKey,omitempty"`
}

// KafkaResponse wraps a response or error for Kafka
type KafkaResponse struct {
	Target        string          `json:"target"`
	CorrelationId string          `json:"correlationId"`
	Success       bool            `json:"success"`
	Response      json.RawMessage `json:"response,omitempty"`
	Error         *ErrorResponse  `json:"error,omitempty"`
}

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (s *server) handleRequest(msg *sarama.ConsumerMessage) {
	var kafkaReq KafkaRequest
	if err := json.Unmarshal(msg.Value, &kafkaReq); err != nil {
		slog.Warn("failed to unmarshal kafka message", "error", err, "partition", msg.Partition, "offset", msg.Offset)
		return
	}

	// Filter by target - discard if not for this server
	if kafkaReq.Target != s.config.Target {
		slog.Debug("discarding message with wrong target", "expected", s.config.Target, "actual", kafkaReq.Target, "operation", kafkaReq.Operation)
		return
	}

	// Validate replyTo
	if kafkaReq.ReplyTo.Topic == "" || kafkaReq.ReplyTo.Target == "" {
		slog.Warn("missing replyTo information", "correlationId", kafkaReq.CorrelationId)
		s.respondError(&kafkaReq, 400, "missing replyTo.topic or replyTo.target")
		return
	}

	// Route based on operation
	switch kafkaReq.Operation {
	// Promises
	case "promises.read":
		s.handleReadPromise(&kafkaReq)
	case "promises.search":
		s.handleSearchPromises(&kafkaReq)
	case "promises.create":
		s.handleCreatePromise(&kafkaReq)
	case "promises.createtask":
		s.handleCreatePromiseAndTask(&kafkaReq)
	case "promises.complete":
		s.handleCompletePromise(&kafkaReq)
	case "promises.callback":
		s.handleCreateCallback(&kafkaReq)
	case "promises.subscribe":
		s.handleCreateSubscription(&kafkaReq)

	// Schedules
	case "schedules.read":
		s.handleReadSchedule(&kafkaReq)
	case "schedules.search":
		s.handleSearchSchedules(&kafkaReq)
	case "schedules.create":
		s.handleCreateSchedule(&kafkaReq)
	case "schedules.delete":
		s.handleDeleteSchedule(&kafkaReq)

	// Locks
	case "locks.acquire":
		s.handleAcquireLock(&kafkaReq)
	case "locks.release":
		s.handleReleaseLock(&kafkaReq)
	case "locks.heartbeat":
		s.handleHeartbeatLocks(&kafkaReq)

	// Tasks
	case "tasks.claim":
		s.handleClaimTask(&kafkaReq)
	case "tasks.complete":
		s.handleCompleteTask(&kafkaReq)
	case "tasks.drop":
		s.handleDropTask(&kafkaReq)
	case "tasks.heartbeat":
		s.handleHeartbeatTasks(&kafkaReq)

	default:
		s.respondError(&kafkaReq, 400, fmt.Sprintf("unknown operation: %s", kafkaReq.Operation))
	}
}

func (s *server) log(operation string, err error) {
	slog.Debug("kafka", "operation", operation, "error", err)
}

func (s *server) code(status t_api.StatusCode) int {
	return int(status) / 100
}

// Helper function to process requests
func (s *server) processRequest(kafkaReq *KafkaRequest, payload t_api.RequestPayload) {
	defer s.log(kafkaReq.Operation, nil)

	// Validate payload
	if err := payload.Validate(); err != nil {
		s.respondError(kafkaReq, 400, fmt.Sprintf("validation error: %v", err))
		return
	}

	// Process the request
	res, apiErr := s.api.Process(kafkaReq.RequestId, &t_api.Request{
		Metadata: kafkaReq.Metadata,
		Payload:  payload,
	})

	if apiErr != nil {
		s.respondError(kafkaReq, s.code(apiErr.Code), apiErr.Error())
		return
	}

	// Encode and send response
	responseData, err := json.Marshal(res)
	if err != nil {
		s.respondError(kafkaReq, 500, fmt.Sprintf("failed to encode response: %v", err))
		return
	}

	response := &KafkaResponse{
		Target:        kafkaReq.ReplyTo.Target,
		CorrelationId: kafkaReq.CorrelationId,
		Success:       true,
		Response:      responseData,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		s.respondError(kafkaReq, 500, fmt.Sprintf("failed to encode response envelope: %v", err))
		return
	}

	// Send reply to specified topic
	s.sendReply(kafkaReq.ReplyTo, responseBytes)
}

func (s *server) respondError(kafkaReq *KafkaRequest, code int, message string) {
	response := &KafkaResponse{
		Target:        kafkaReq.ReplyTo.Target,
		CorrelationId: kafkaReq.CorrelationId,
		Success:       false,
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

	s.sendReply(kafkaReq.ReplyTo, responseBytes)
}

func (s *server) sendReply(replyTo ReplyTo, data []byte) {
	msg := &sarama.ProducerMessage{
		Topic: replyTo.Topic,
		Value: sarama.ByteEncoder(data),
	}

	// Use partition key if specified
	if replyTo.PartitionKey != "" {
		msg.Key = sarama.StringEncoder(replyTo.PartitionKey)
	}

	partition, offset, err := s.kafka.producer.SendMessage(msg)
	if err != nil {
		slog.Error("failed to send reply", "error", err, "topic", replyTo.Topic, "target", replyTo.Target)
		return
	}

	slog.Debug("sent reply", "topic", replyTo.Topic, "target", replyTo.Target, "partition", partition, "offset", offset)
}
