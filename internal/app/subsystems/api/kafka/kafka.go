package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	consumer       *kafka.Consumer
	producer       *kafka.Producer
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	bootstrapServers := strings.Join(config.Brokers, ",")

	// Create admin client
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka admin client: %w", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	// Create topic if it doesn't exist
	if _, err := admin.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: config.Topic, NumPartitions: 1}}); err != nil {
		return nil, fmt.Errorf("failed to create topic %q: %w", config.Topic, err)
	}

	// Create consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"allow.auto.create.topics": true,
		"group.id":                 config.ConsumerGroup,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	if err := consumer.Subscribe(config.Topic, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic %q: %w", config.Topic, err)
	}

	// Create producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"allow.auto.create.topics": true,
		"acks":                     "all",
		"retries":                  3,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	k := &Kafka{
		config:         config,
		api:            api.New(a, "kafka"),
		consumer:       consumer,
		producer:       producer,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}

	slog.Debug("kafka initialized",
		"brokers", config.Brokers,
		"topic", config.Topic,
		"target", config.Target,
		"consumerGroup", config.ConsumerGroup,
	)

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

	server := &server{
		api:    k.api,
		config: k.config,
		kafka:  k,
	}

	for {
		select {
		case <-k.shutdownCtx.Done():
			return
		default:
			msg, err := k.consumer.ReadMessage(-1)
			if err != nil {
				slog.Error("kafka consumer error", "error", err)
				errors <- err
				return
			}

			server.handleRequest(msg)

			if _, err := k.consumer.CommitMessage(msg); err != nil {
				slog.Warn("failed to commit message", "error", err)
			}
		}
	}
}

func (k *Kafka) Stop() error {
	// Cancel shutdown context
	k.shutdownCancel()

	if err := k.consumer.Close(); err != nil {
		slog.Warn("failed to close consumer", "error", err)
	}

	// Ensure all outstanding messages are flushed before closing producer
	// (optional but closer to SyncProducer semantics)
	k.producer.Flush(int(k.config.Timeout.Milliseconds()))

	k.producer.Close()

	return nil
}

type server struct {
	api    *api.API
	config *Config
	kafka  *Kafka
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
	Topic  string  `json:"topic"`
	Target string  `json:"target"`
	Key    *string `json:"key,omitempty"`
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

func (s *server) handleRequest(msg *kafka.Message) {
	var kafkaReq KafkaRequest
	if err := json.Unmarshal(msg.Value, &kafkaReq); err != nil {
		topic := ""
		if msg.TopicPartition.Topic != nil {
			topic = *msg.TopicPartition.Topic
		}
		slog.Warn("failed to unmarshal kafka message",
			"error", err,
			"topic", topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msg.TopicPartition.Offset,
		)
		return
	}

	// Filter by target - discard if not for this server
	if kafkaReq.Target != s.config.Target {
		slog.Debug("discarding message with wrong target",
			"expected", s.config.Target,
			"actual", kafkaReq.Target,
			"operation", kafkaReq.Operation,
		)
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
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &replyTo.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: data,
	}

	if replyTo.Key != nil {
		msg.Key = []byte(*replyTo.Key)
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	if err := s.kafka.producer.Produce(msg, deliveryChan); err != nil {
		slog.Error("failed to send reply", "error", err, "topic", replyTo.Topic, "target", replyTo.Target)
		return
	}

	ev := <-deliveryChan
	m, ok := ev.(*kafka.Message)
	if !ok {
		slog.Error("unexpected event type from producer", "event", ev)
		return
	}

	if m.TopicPartition.Error != nil {
		slog.Error("failed to deliver reply",
			"error", m.TopicPartition.Error,
			"topic", replyTo.Topic,
			"target", replyTo.Target,
		)
		return
	}

	slog.Debug("sent reply",
		"topic", replyTo.Topic,
		"target", replyTo.Target,
		"partition", m.TopicPartition.Partition,
		"offset", m.TopicPartition.Offset,
	)
}
