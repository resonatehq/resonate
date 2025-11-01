package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"1000"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"4"`
	Timeout     time.Duration `flag:"timeout" desc:"request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
	ProjectID   string        `flag:"project-id" desc:"GCP project ID" default:""`
}

type PubSub struct {
	*base.Plugin
	client *pubsub.Client
}

type processor struct {
	client  *pubsub.Client
	timeout time.Duration
}

type Addr struct {
	Topic string `json:"topic"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*PubSub, error) {
	if config.ProjectID == "" {
		return nil, fmt.Errorf("GCP project ID is required")
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

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

	plugin := base.NewPlugin(
		a,
		"pubsub",
		baseConfig,
		metrics,
		proc,
		func() error {
			if client != nil {
				return client.Close()
			}
			return nil
		},
	)

	return &PubSub{
		Plugin: plugin,
		client: client,
	}, nil
}

func (p *processor) Process(data []byte, head map[string]string, body []byte) (bool, error) {
	var addr Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if addr.Topic == "" {
		return false, fmt.Errorf("missing topic")
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	publisher := p.client.Publisher(addr.Topic)
	result := publisher.Publish(ctx, &pubsub.Message{Data: body})

	_, err := result.Get(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}
