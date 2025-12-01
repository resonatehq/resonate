package kafka

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/plugins"
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

type Worker struct {
	i        int
	sq       <-chan *plugins.Message
	timeout  time.Duration
	aio      aio.AIO
	metrics  *metrics.Metrics
	config   *Config
	producer Producer
}

type Kafka struct {
	sq      chan *plugins.Message
	workers []*Worker
}

type Addr struct {
	Topic   string            `json:"topic"`
	Key     *string           `json:"key,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (a *Addr) validate() error {
	if a.Topic == "" {
		return fmt.Errorf("topic required")
	}
	return nil
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
	sq := make(chan *plugins.Message, config.Size)
	workers := make([]*Worker, config.Workers)

	for i := range workers {
		workers[i] = &Worker{
			i:        i,
			sq:       sq,
			timeout:  config.Timeout,
			aio:      a,
			metrics:  metrics,
			config:   config,
			producer: producer,
		}
	}

	return &Kafka{sq: sq, workers: workers}, nil
}

func (k *Kafka) String() string {
	return fmt.Sprintf("%s:kafka", t_aio.Sender.String())
}

func (k *Kafka) Type() string {
	return "kafka"
}

func (k *Kafka) Start(chan<- error) error {
	for _, worker := range k.workers {
		go worker.Start()
	}

	return nil
}

func (k *Kafka) Stop() error {
	close(k.sq)
	if len(k.workers) > 0 && k.workers[0].producer != nil {
		k.workers[0].producer.Close()
	}
	return nil
}

func (k *Kafka) Enqueue(msg *plugins.Message) bool {
	select {
	case k.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) String() string {
	return fmt.Sprintf("%s:kafka", t_aio.Sender.String())
}

func (w *Worker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for msg := range w.sq {
		counter.Inc()
		success, err := w.Process(msg.Addr, msg.Body)
		if err != nil {
			slog.Warn("failed to send task", "err", err)
		}
		msg.Done(&t_aio.SenderCompletion{
			Success:     success,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
		counter.Dec()
	}
}

func (w *Worker) Process(data []byte, body []byte) (bool, error) {
	var addr Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if err := addr.validate(); err != nil {
		return false, err
	}

	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &addr.Topic, Partition: kafka.PartitionAny},
		Value:          body,
	}

	if addr.Key != nil {
		msg.Key = []byte(*addr.Key)
	}

	if len(addr.Headers) > 0 {
		msg.Headers = make([]kafka.Header, 0, len(addr.Headers))
		for k, v := range addr.Headers { // nosemgrep: range-over-map
			msg.Headers = append(msg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	err := w.producer.Produce(msg, deliveryChan)
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
