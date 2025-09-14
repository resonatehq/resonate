package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
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

type SQSClient interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, opt ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type SQSWorker struct {
	i       int
	sq      <-chan *aio.Message
	timeout time.Duration
	aio     aio.AIO
	metrics *metrics.Metrics
	client  SQSClient
}

type SQS struct {
	sq      chan *aio.Message
	workers []*SQSWorker
}

type Addr struct {
	QueueURL string `json:"queue_url"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*SQS, error) {
	return NewWithClient(a, metrics, config, nil)
}

func NewWithClient(a aio.AIO, metrics *metrics.Metrics, config *Config, client SQSClient) (*SQS, error) {
	if client == nil {
		ctx := context.Background()
		awsConfig, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}
		client = sqs.NewFromConfig(awsConfig)
	}

	sq := make(chan *aio.Message, config.Size)
	workers := make([]*SQSWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &SQSWorker{
			i:       i,
			sq:      sq,
			timeout: config.Timeout,
			aio:     a,
			metrics: metrics,
			client:  client,
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

	if addr.QueueURL == "" {
		return false, errors.New("missing queue_url in address")
	}

	region, err := parse(addr.QueueURL)
	if err != nil {
		return false, fmt.Errorf("failed to parse SQS URL: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	message_body := string(body)
	_, err = w.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &addr.QueueURL,
		MessageBody: &message_body,
	}, func(o *sqs.Options) {
		o.Region = region
	})
	if err != nil {
		return false, fmt.Errorf("failed to send message: %w", err)
	}

	return true, nil
}

// Parse the SQS URL to extract region.
// Expected format: https://sqs.region.amazonaws.com/account/queue-name
func parse(queueURL string) (string, error) {
	url := strings.TrimSpace(queueURL)

	url = strings.TrimPrefix(url, "https://")
	url = strings.TrimPrefix(url, "http://")

	if !strings.HasPrefix(url, "sqs.") {
		return "", errors.New("invalid SQS URL format: must start with sqs")
	}

	url = strings.TrimPrefix(url, "sqs.")

	parts := strings.Split(url, ".")
	if len(parts) < 2 {
		return "", errors.New("invalid SQS URL format: missing region")
	}

	region := parts[0]
	if region == "" {
		return "", errors.New("invalid SQS URL format: empty region")
	}

	return region, nil
}
