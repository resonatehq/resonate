package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

type Config struct {
	Timeout time.Duration `flag:"timeout" desc:"http request timeout" default:"1s"`
}

type Http struct {
	client *http.Client
}

type Data struct {
	Headers map[string]string `json:"headers,omitempty"`
	Url     string            `json:"url"`
}

func New(config *Config) (*Http, error) {
	return &Http{
		client: &http.Client{Timeout: config.Timeout},
	}, nil
}

func (h *Http) Type() string {
	return "http"
}

func (h *Http) Enqueue(data []byte, body []byte) (bool, error) {
	var httpData *Data
	if err := json.Unmarshal(data, &httpData); err != nil {
		return false, err
	}

	req, err := http.NewRequest("POST", httpData.Url, bytes.NewReader(body))
	if err != nil {
		return false, err
	}

	if httpData.Headers == nil {
		httpData.Headers = map[string]string{}
	}

	// set content type, can be overriden
	httpData.Headers["Content-Type"] = "application/json"

	for k, v := range httpData.Headers {
		req.Header.Set(k, v)
	}

	res, err := h.client.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == http.StatusOK, nil
}
