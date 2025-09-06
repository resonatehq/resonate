package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size    int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout time.Duration `flag:"timeout" desc:"aws request timeout" default:"30s"`
}

type SQSWorker struct {
	i       int
	sq      <-chan *aio.Message
	timeout time.Duration
	aio     aio.AIO
	metrics *metrics.Metrics
}

type SQS struct {
	sq      chan *aio.Message
	workers []*SQSWorker
}

type Addr struct {
	Region string `json:"region"`
	Queue  string `json:"queue"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*SQS, error) {
	sq := make(chan *aio.Message, config.Size)
	workers := make([]*SQSWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &SQSWorker{
			i:       i,
			sq:      sq,
			timeout: config.Timeout,
			aio:     a,
			metrics: metrics,
		}
	}

	return &SQS{
		sq:      sq,
		workers: workers,
	}, nil
}

func (s *SQS) String() string {
	return fmt.Sprintf("%s:sqs", t_aio.Sender.String())
}

func (s *SQS) Type() string {
	return "sqs"
}

func (s *SQS) Start(chan<- error) error {
	for _, worker := range s.workers {
		go worker.Start()
	}

	return nil
}

func (s *SQS) Stop() error {
	close(s.sq)
	return nil
}

func (s *SQS) Enqueue(msg *aio.Message) bool {
	select {
	case s.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *SQSWorker) String() string {
	return fmt.Sprintf("%s:sqs", t_aio.Sender.String())
}

func (w *SQSWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		msg, ok := <-w.sq
		if !ok {
			return
		}

		counter.Inc()
		msg.Done(w.Process(msg.Addr, msg.Body))
		counter.Dec()
	}
}

func (w *SQSWorker) Process(data []byte, body []byte) (bool, error) {
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if addr.Region == "" {
		return false, errors.New("missing region in address")
	}

	if addr.Queue == "" {
		return false, errors.New("missing queue in address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(addr.Region))
	if err != nil {
		return false, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := sqs.NewFromConfig(cfg)

	qURL, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: &addr.Queue,
	})
	if err != nil {
		return false, fmt.Errorf("failed to get queue URL: %w", err)
	}

	messageBody := string(body)
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    qURL.QueueUrl,
		MessageBody: &messageBody,
	})
	if err != nil {
		return false, fmt.Errorf("failed to send message: %w", err)
	}

	return true, nil
}
