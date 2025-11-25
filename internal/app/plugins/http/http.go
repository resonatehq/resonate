package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"github.com/go-viper/mapstructure/v2"
	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/util"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"3"`
	Timeout     time.Duration `flag:"timeout" desc:"http request timeout" default:"3m"`
	ConnTimeout time.Duration `flag:"conn-timeout" desc:"http connection timeout" default:"10s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
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

type Http struct {
	*base.Plugin
}

type Addr struct {
	Headers map[string]string `json:"headers,omitempty"`
	Url     string            `json:"url"`
}

type processor struct {
	client *http.Client
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Http, error) {
	proc := &processor{
		client: &http.Client{
			Timeout: config.Timeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: config.ConnTimeout,
				}).DialContext,
			},
		},
	}

	baseConfig := &base.BaseConfig{
		Size:        config.Size,
		Workers:     config.Workers,
		TimeToRetry: config.TimeToRetry,
		TimeToClaim: config.TimeToClaim,
	}

	return &Http{
		Plugin: base.NewPlugin(a, "http", baseConfig, metrics, proc, nil),
	}, nil
}

func (p *processor) Process(data []byte, head map[string]string, body []byte) (bool, error) {
	var addr *Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	req, err := http.NewRequest("POST", addr.Url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}

	if addr.Headers == nil {
		addr.Headers = map[string]string{}
	}

	for k, v := range head { // nosemgrep: range-over-map
		req.Header.Set(k, v)
	}

	for k, v := range addr.Headers { // nosemgrep: range-over-map
		req.Header.Set(k, v)
	}

	// set non-overridable headers
	req.Header.Set("Content-Type", "application/json")

	succeeded := false
	connected := make(chan struct{})

	trace := &httptrace.ClientTrace{
		ConnectDone: func(network, addr string, err error) {
			// when a connection is established, close the channel and return
			// true
			if err == nil {
				util.Assert(!succeeded, "connect done cannot be called multiple times successfully")

				succeeded = true
				close(connected)
			}
		},
	}

	// perform request asynchronously
	go func() {
		res, err := p.client.Do(req.WithContext(httptrace.WithClientTrace(context.Background(), trace)))

		if res != nil {
			_ = res.Body.Close()
		}

		// when request fails and the connection was not established, close
		// the channel and return false
		if err != nil && !succeeded {
			close(connected)
		}
	}()

	<-connected
	return succeeded, nil
}
