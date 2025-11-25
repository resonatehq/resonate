package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"0"`
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

func (c *Config) New(aio aio.AIO, metrics *metrics.Metrics) (aio.Plugin, error) {
	return New(aio, metrics, c)
}

type Client interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, opt ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type SQS struct {
	*base.Plugin
}

type Addr struct {
	Url    string  `json:"url"`
	Region *string `json:"region,omitempty"`
}

type processor struct {
	client  Client
	timeout time.Duration
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*SQS, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := sqs.NewFromConfig(awsConfig)
	return NewWithClient(a, metrics, config, client)
}

func NewWithClient(a aio.AIO, metrics *metrics.Metrics, config *Config, client Client) (*SQS, error) {
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

	plugin := base.NewPlugin(a, "sqs", baseConfig, metrics, proc, nil)

	return &SQS{
		Plugin: plugin,
	}, nil
}

func (p *processor) Process(data []byte, head map[string]string, body []byte) (bool, error) {
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	msgBody := string(body)
	msgAttrs := map[string]awstypes.MessageAttributeValue{}

	for k, v := range head { // nosemgrep: range-over-map
		msgAttrs[k] = awstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}

	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:          &addr.Url,
		MessageBody:       &msgBody,
		MessageAttributes: msgAttrs,
	}, func(o *sqs.Options) {
		if addr.Region != nil {
			o.Region = *addr.Region
		} else if region, ok := parseSQSRegion(addr.Url); ok {
			o.Region = region
		}
	})
	if err != nil {
		return false, err
	}

	return true, nil
}

func parseSQSRegion(sqsUrl string) (string, bool) {
	u, err := url.Parse(sqsUrl)
	if err != nil {
		return "", false
	}

	hostParts := strings.Split(u.Host, ".")
	if hostParts[0] != "sqs" || len(hostParts) < 2 {
		return "", false
	}

	return hostParts[1], true
}
