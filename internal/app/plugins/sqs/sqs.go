package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
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
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"aws request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`

	// By default do not attempt to resend if the task is not claimed in
	// a certain amount of time. Once a message is sent to SQS, it is
	// durable and will not be acknowledged until the task is claimed.
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"0"`
}

type Client interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, opt ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type Worker struct {
	i       int
	sq      <-chan *aio.Message
	timeout time.Duration
	aio     aio.AIO
	metrics *metrics.Metrics
	config  *Config
	client  Client
}

type SQS struct {
	sq      chan *aio.Message
	workers []*Worker
}

type Addr struct {
	Url    string  `json:"url"`
	Region *string `json:"region,omitempty"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*SQS, error) {
	aws_config, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := sqs.NewFromConfig(aws_config)
	return NewWithClient(a, metrics, config, client)
}

func NewWithClient(a aio.AIO, metrics *metrics.Metrics, config *Config, client Client) (*SQS, error) {
	sq := make(chan *aio.Message, config.Size)
	workers := make([]*Worker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &Worker{
			i:       i,
			sq:      sq,
			timeout: config.Timeout,
			aio:     a,
			metrics: metrics,
			config:  config,
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

func (w *Worker) String() string {
	return fmt.Sprintf("%s:sqs", t_aio.Sender.String())
}

func (w *Worker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		msg, ok := <-w.sq
		if !ok {
			return
		}

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
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	msg := string(body)
	_, err := w.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &addr.Url,
		MessageBody: &msg,
	}, func(o *sqs.Options) {
		if addr.Region != nil {
			o.Region = *addr.Region
		} else if region, ok := parse_sqs_region(addr.Url); ok {
			o.Region = region
		}
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

func parse_sqs_region(sqs_url string) (string, bool) {
	u, err := url.Parse(sqs_url)
	if err != nil {
		return "", false
	}

	host_parts := strings.Split(u.Host, ".")
	if host_parts[0] != "sqs" || len(host_parts) < 2 {
		return "", false
	}

	return host_parts[1], true
}
