package nats

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"nats request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`

	// By default do not attempt to resend if the task is not claimed in
	// a certain amount of time. Once a message is sent to NATS, it is
	// durable and will not be acknowledged until the task is claimed.
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"0"`
}

type NATSClient interface {
	Publish(subject string, data []byte) error
}

type NATSWorker struct {
	i       int
	sq      <-chan *aio.Message
	timeout time.Duration
	aio     aio.AIO
	metrics *metrics.Metrics
	config  *Config
	client  NATSClient
}

type NATS struct {
	sq      chan *aio.Message
	workers []*NATSWorker
}

type Addr struct {
	Subject string `json:"subject"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*NATS, error) {
	// Connect to NATS server
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return newWithClient(a, metrics, config, nc)
}

func newWithClient(a aio.AIO, metrics *metrics.Metrics, config *Config, client NATSClient) (*NATS, error) {
	sq := make(chan *aio.Message, config.Size)
	workers := make([]*NATSWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &NATSWorker{
			i:       i,
			sq:      sq,
			timeout: config.Timeout,
			aio:     a,
			metrics: metrics,
			config:  config,
			client:  client,
		}
	}

	return &NATS{
		sq:      sq,
		workers: workers,
	}, nil
}

func (n *NATS) String() string {
	return fmt.Sprintf("%s:nats", t_aio.Sender.String())
}

func (n *NATS) Type() string {
	return "nats"
}

func (n *NATS) Start(chan<- error) error {
	for _, worker := range n.workers {
		go worker.Start()
	}

	return nil
}

func (n *NATS) Stop() error {
	close(n.sq)
	return nil
}

func (n *NATS) Enqueue(msg *aio.Message) bool {
	select {
	case n.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *NATSWorker) String() string {
	return fmt.Sprintf("%s:nats", t_aio.Sender.String())
}

func (w *NATSWorker) Start() {
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

func (w *NATSWorker) Process(data []byte, body []byte) (bool, error) {
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if addr.Subject == "" {
		return false, fmt.Errorf("subject is required")
	}

	err := w.client.Publish(addr.Subject, body)
	if err != nil {
		return false, err
	}

	return true, nil
}
