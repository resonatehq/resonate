package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	base.BaseConfig
	Timeout time.Duration `flag:"timeout" desc:"aws request timeout" default:"30s"`
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

func (p *processor) Process(body []byte, data []byte) (bool, error) {
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()

	msg := string(body)
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &addr.Url,
		MessageBody: &msg,
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

	plugin := base.NewPlugin(a, "sqs", &config.BaseConfig, metrics, proc, nil)

	return &SQS{
		Plugin: plugin,
	}, nil
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
