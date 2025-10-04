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
}

type Client interface {
	Publish(subject string, data []byte) error
	PublishRequest(subject, reply string, data []byte) error
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
	URL     string `json:"url"`
	Subject string `json:"subject"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*NATS, error) {
	return NewWithClient(a, metrics, config, nil)
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

	// TODO - need to find the best approach to handle this
	if addr.URL == "" {
		return false, fmt.Errorf("missing URL")
	}
	if addr.Subject == "" {
		return false, fmt.Errorf("missing subject")
	}

	client := w.client
	if client == nil {
		opts := []natsgo.Option{
			natsgo.Timeout(w.timeout),
			natsgo.RetryOnFailedConnect(false), // Disable retry for timeout testing
			natsgo.PingInterval(20 * time.Second),
			natsgo.MaxPingsOutstanding(2),
		}

		nc, err := natsgo.Connect(addr.URL, opts...)
		if err != nil {
			return false, err
		}
		client = nc
		defer nc.Close()
	}

	// If we have a client (including mock clients), use it directly
	if w.client != nil {
		err := client.Publish(addr.Subject, body)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	// Only use timeout logic for real NATS connections
	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- client.Publish(addr.Subject, body)
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
