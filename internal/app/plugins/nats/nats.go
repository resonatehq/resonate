package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"1000"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"4"`
	Timeout     time.Duration `flag:"timeout" desc:"nats request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
	URL         string        `flag:"url" desc:"nats server URL" default:"nats://localhost:4222"`
}

type Client interface {
	Publish(subject string, data []byte) error
	Close()
}

type Worker struct {
	id      int
	sq      <-chan *aio.Message
	timeout time.Duration
	aio     aio.AIO
	metrics *metrics.Metrics
	config  *Config
	client  Client
}

type NATS struct {
	sq      chan *aio.Message
	workers []*Worker
}

type Addr struct {
	Subject string `json:"subject"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*NATS, error) {
	if config.URL == "" {
		return nil, fmt.Errorf("NATS URL is required")
	}

	sq := make(chan *aio.Message, config.Size)
	workers := make([]*Worker, config.Workers)

	// Create a separate NATS connection for each worker
	for i := 0; i < config.Workers; i++ {
		opts := []natsgo.Option{
			natsgo.Timeout(config.Timeout),
			natsgo.RetryOnFailedConnect(true),
			natsgo.PingInterval(20 * time.Second),
			natsgo.MaxPingsOutstanding(2),
		}

		nc, err := natsgo.Connect(config.URL, opts...)
		if err != nil {
			// Close any previously created connections
			for j := 0; j < i; j++ {
				if workers[j].client != nil {
					workers[j].client.Close()
				}
			}
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}

		workers[i] = &Worker{
			id:      i,
			sq:      sq,
			timeout: config.Timeout,
			aio:     a,
			metrics: metrics,
			config:  config,
			client:  nc,
		}
	}

	return &NATS{
		sq:      sq,
		workers: workers,
	}, nil
}

func NewWithClient(a aio.AIO, metrics *metrics.Metrics, config *Config, client Client) (*NATS, error) {
	sq := make(chan *aio.Message, config.Size)
	workers := make([]*Worker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &Worker{
			id:      i,
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

func (p *NATS) String() string {
	return fmt.Sprintf("%s:nats", t_aio.Sender.String())
}

func (p *NATS) Type() string {
	return "nats"
}

func (p *NATS) Start(chan<- error) error {
	for _, worker := range p.workers {
		go worker.Start()
	}

	return nil
}

func (p *NATS) Stop() error {
	if p.sq != nil {
		close(p.sq)
	}
	// Close all worker NATS connections
	for _, worker := range p.workers {
		if worker.client != nil {
			worker.client.Close()
		}
	}
	return nil
}

func (p *NATS) Enqueue(msg *aio.Message) bool {
	if p.sq == nil || msg == nil {
		return false
	}

	select {
	case p.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) String() string {
	return fmt.Sprintf("%s:nats", t_aio.Sender.String())
}

func (w *Worker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.id))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		msg, ok := <-w.sq
		if !ok {
			return
		}

		counter.Inc()
		success, err := w.Process(msg.Body, msg.Addr)
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

func (w *Worker) Process(body []byte, data []byte) (bool, error) {
	var addr Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if addr.Subject == "" {
		return false, fmt.Errorf("missing subject")
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- w.client.Publish(addr.Subject, body)
	}()

	select {
	case err := <-done:
		if err != nil {
			return false, err
		}
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
