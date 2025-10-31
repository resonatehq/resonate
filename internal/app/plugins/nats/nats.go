package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
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

type NATS struct {
	*base.Plugin
}

type Addr struct {
	Subject string `json:"subject"`
}

type processor struct {
	client  Client
	timeout time.Duration
}

func (p *processor) Process(body []byte, data []byte) (bool, error) {
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if addr.Subject == "" {
		return false, fmt.Errorf("missing subject")
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- p.client.Publish(addr.Subject, body)
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

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*NATS, error) {
	if config.URL == "" {
		return nil, fmt.Errorf("NATS URL is required")
	}

	opts := []natsgo.Option{
		natsgo.Timeout(config.Timeout),
		natsgo.RetryOnFailedConnect(true),
		natsgo.PingInterval(20 * time.Second),
		natsgo.MaxPingsOutstanding(2),
	}

	nc, err := natsgo.Connect(config.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return NewWithClient(a, metrics, config, nc)
}

func NewWithClient(a aio.AIO, metrics *metrics.Metrics, config *Config, client Client) (*NATS, error) {
	proc := &processor{
		client:  client,
		timeout: config.Timeout,
	}

	baseConfig := &base.BaseConfig{
		Size:        config.Size,
		Workers:     config.Workers,
		TimeToRetry: config.TimeToRetry,
		TimeToClaim: config.TimeToClaim,
	}

	cleanup := func() error {
		if client != nil {
			client.Close()
		}
		return nil
	}

	plugin := base.NewPlugin(a, "nats", baseConfig, metrics, proc, cleanup)

	return &NATS{
		Plugin: plugin,
	}, nil
}
