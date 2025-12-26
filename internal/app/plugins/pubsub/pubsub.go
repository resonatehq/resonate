package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/plugins"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"1000"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"4"`
	Timeout     time.Duration `flag:"timeout" desc:"request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
	ProjectID   string        `flag:"project-id" desc:"GCP project ID" default:""`
}

func (c *Config) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string, prefix string, keyPrefix string) {
	cmdUtil.Bind(c, cmd, flg, vip, name, prefix, keyPrefix)
}

func (c *Config) Decode(value any, decodeHook mapstructure.DecodeHookFunc) error {
	decoderConfig := &mapstructure.DecoderConfig{
		Result:     c,
		DecodeHook: decodeHook,
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return err
	}

	if err := decoder.Decode(value); err != nil {
		return err
	}

	return nil
}

func (c *Config) New(metrics *metrics.Metrics) (plugins.Plugin, error) {
	return New(metrics, c)
}

type PubSub struct {
	*base.Plugin
}

type Client interface {
	Publish(ctx context.Context, topic string, data []byte) (string, error)
	Close() error
}

type ClientWrapper struct {
	*pubsub.Client
}

func (w *ClientWrapper) Publish(ctx context.Context, topic string, data []byte) (string, error) {
	publisher := w.Publisher(topic)
	result := publisher.Publish(ctx, &pubsub.Message{Data: data})
	return result.Get(ctx)
}

type processor struct {
	client  Client
	timeout time.Duration
}

type Addr struct {
	Topic string `json:"topic"`
}

func New(metrics *metrics.Metrics, config *Config) (*PubSub, error) {
	if config.ProjectID == "" {
		return nil, fmt.Errorf("GCP project ID is required")
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	wrapper := &ClientWrapper{client}
	return NewWithClient(metrics, config, wrapper)
}

func NewWithClient(metrics *metrics.Metrics, config *Config, client Client) (*PubSub, error) {
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

	plugin := base.NewPlugin("pubsub", baseConfig, metrics, proc, func() error {
		if client != nil {
			return client.Close()
		}
		return nil
	})

	return &PubSub{
		Plugin: plugin,
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

	_, err := p.client.Publish(ctx, addr.Topic, body)
	if err != nil {
		return false, err
	}

	return true, nil
}
