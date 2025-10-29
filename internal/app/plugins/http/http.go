package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/plugins/base"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"http request timeout" default:"1s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
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

	res, err := p.client.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == http.StatusOK, nil
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Http, error) {
	proc := &processor{
		client: &http.Client{Timeout: config.Timeout},
	}

	baseConfig := &base.BaseConfig{
		Size:        config.Size,
		Workers:     config.Workers,
		TimeToRetry: config.TimeToRetry,
		TimeToClaim: config.TimeToClaim,
	}

	plugin := base.NewPlugin(a, "http", baseConfig, metrics, proc, nil)

	return &Http{
		Plugin: plugin,
	}, nil
}
