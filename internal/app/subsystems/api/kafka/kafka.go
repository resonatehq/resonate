package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	Brokers       []string      `flag:"brokers" desc:"kafka broker addresses" default:"localhost:9092"`
	Topic         string        `flag:"topic" desc:"kafka request topic" default:"resonate"`
	Target        string        `flag:"target" desc:"target identifier for this server" default:"resonate.server"`
	ConsumerGroup string        `flag:"consumer-group" desc:"kafka consumer group" default:"resonate-servers"`
	Timeout       time.Duration `flag:"timeout" desc:"kafka server graceful shutdown timeout" default:"10s"`
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

	// Create consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"allow.auto.create.topics": true,
		"auto.offset.reset":        "earliest",
		"bootstrap.servers":        bootstrapServers,
		"enable.auto.commit":       false,
		"group.id":                 config.ConsumerGroup,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	if err := consumer.Subscribe(config.Topic, nil); err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic %q: %w", config.Topic, err)
	}

	// Create producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"acks":                     "all",
		"allow.auto.create.topics": true,
		"bootstrap.servers":        bootstrapServers,
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
				// if topic does not exist, continue
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrUnknownTopicOrPart {
					slog.Warn("kafka topic does not exist, retrying", "topic", k.config.Topic)

					// it appears that the second call to ReadMessage will block
					// until the topic exists, so don't sleep (!!)
					continue
				}

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
	Topic     string  `json:"topic"`
	Target    string  `json:"target"`
	Partition *int32  `json:"partition,omitempty"`
	Key       *string `json:"key,omitempty"`
}

// KafkaResponse wraps a response or error for Kafka
type KafkaResponse struct {
	Target        string          `json:"target"`
	CorrelationId string          `json:"correlationId"`
	Operation     string          `json:"operation"`
	Success       bool            `json:"success"`
	Response      json.RawMessage `json:"response,omitempty"`
	Error         *api.Error      `json:"error,omitempty"`
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
		s.respondError(&kafkaReq, &api.Error{
			Code:    400,
			Message: fmt.Sprintf("unknown operation: %s", kafkaReq.Operation),
		})
	}
}

func (s *server) log(operation string, err error) {
	slog.Debug("kafka", "operation", operation, "error", err)
}

// Helper function to process requests
func (s *server) processRequest(kafkaReq *KafkaRequest, payload t_api.RequestPayload) (t_api.ResponsePayload, *api.Error) {
	defer s.log(kafkaReq.Operation, nil)

	// Process the request
	res, apiErr := s.api.Process(kafkaReq.RequestId, &t_api.Request{
		Metadata: kafkaReq.Metadata,
		Payload:  payload,
	})

	if apiErr != nil {
		return nil, apiErr
	}

	return res.Payload, nil
}

func (s *server) respondError(kafkaReq *KafkaRequest, error *api.Error) {
	response := &KafkaResponse{
		Target:        kafkaReq.ReplyTo.Target,
		CorrelationId: kafkaReq.CorrelationId,
		Operation:     kafkaReq.Operation,
		Success:       false,
		Error:         error,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
		return
	}

	s.send(kafkaReq, responseBytes)
}

func (s *server) sendReply(kafkaReq *KafkaRequest, data []byte) {
	response := &KafkaResponse{
		Target:        kafkaReq.ReplyTo.Target,
		CorrelationId: kafkaReq.CorrelationId,
		Operation:     kafkaReq.Operation,
		Success:       true,
		Response:      data,
	}

	responseBytes, err := json.Marshal(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
		return
	}

	s.send(kafkaReq, responseBytes)
}

func (s *server) send(kafkaReq *KafkaRequest, value []byte) {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &kafkaReq.ReplyTo.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}

	if kafkaReq.ReplyTo.Partition != nil {
		msg.TopicPartition.Partition = *kafkaReq.ReplyTo.Partition
	}
	if kafkaReq.ReplyTo.Key != nil {
		msg.Key = []byte(*kafkaReq.ReplyTo.Key)
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	if err := s.kafka.producer.Produce(msg, deliveryChan); err != nil {
		slog.Error("failed to send reply", "error", err, "topic", kafkaReq.ReplyTo.Topic, "target", kafkaReq.ReplyTo.Target)
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
			"topic", kafkaReq.ReplyTo.Topic,
			"target", kafkaReq.ReplyTo.Target,
		)
		return
	}

	slog.Debug("sent reply",
		"topic", kafkaReq.ReplyTo.Topic,
		"target", kafkaReq.ReplyTo.Target,
		"partition", m.TopicPartition.Partition,
		"offset", m.TopicPartition.Offset,
	)
}
