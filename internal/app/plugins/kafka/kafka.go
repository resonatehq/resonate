package kafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"kafka request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"0"`
	Brokers     []string      `flag:"brokers" desc:"kafka broker addresses" default:"localhost:9092"`
	Compression string        `flag:"compression" desc:"compression type (none, gzip, snappy, lz4, zstd)" default:"none"`
}

type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type Kafka struct {
	*base.Plugin
}

type Addr struct {
	Topic   string            `json:"topic"`
	Key     *string           `json:"key,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type processor struct {
	producer Producer
}

func (p *processor) Process(data []byte, head map[string]string, body []byte) (bool, error) {
	var addr Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if addr.Topic == "" {
		return false, fmt.Errorf("topic required")
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &addr.Topic, Partition: kafka.PartitionAny},
		Value:          body,
	}

	if addr.Key != nil {
		msg.Key = []byte(*addr.Key)
	}

	headers := make(map[string]string)
	for k, v := range addr.Headers { // nosemgrep: range-over-map
		headers[k] = v
	}
	for k, v := range head { // nosemgrep: range-over-map
		headers[k] = v
	}

	if len(headers) > 0 {
		msg.Headers = make([]kafka.Header, 0, len(headers))
		for k, v := range headers { // nosemgrep: range-over-map
			msg.Headers = append(msg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	err := p.producer.Produce(msg, deliveryChan)
	if err != nil {
		return false, err
	}

	e := <-deliveryChan
	m, ok := e.(*kafka.Message)
	if !ok {
		return false, fmt.Errorf("expected kafka.Message delivery event, got %T", e)
	}

	if m.TopicPartition.Error != nil {
		return false, m.TopicPartition.Error
	}

	return true, nil
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Kafka, error) {
	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers":   strings.Join(config.Brokers, ", "),
		"delivery.timeout.ms": strconv.FormatInt(config.Timeout.Milliseconds(), 10),
		"compression.type":    config.Compression,
		"retries":             3,
		"acks":                "all",
	}
	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return NewWithProducer(a, metrics, config, producer)
}

func NewWithProducer(a aio.AIO, metrics *metrics.Metrics, config *Config, producer Producer) (*Kafka, error) {
	proc := &processor{
		producer: producer,
	}

	baseConfig := &base.BaseConfig{
		Size:        config.Size,
		Workers:     config.Workers,
		TimeToRetry: config.TimeToRetry,
		TimeToClaim: config.TimeToClaim,
	}

	cleanup := func() error {
		if producer != nil {
			producer.Close()
		}
		return nil
	}

	plugin := base.NewPlugin(a, "kafka", baseConfig, metrics, proc, cleanup)

	return &Kafka{
		Plugin: plugin,
	}, nil
}
