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
	base.BaseConfig
	Timeout time.Duration `flag:"timeout" desc:"http request timeout" default:"1s"`
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

func (p *processor) Process(body []byte, data []byte) (bool, error) {
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

	for k, v := range addr.Headers {
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

	plugin := base.NewPlugin(a, "http", &config.BaseConfig, metrics, proc, nil)

	return &Http{
		Plugin: plugin,
	}, nil
}
