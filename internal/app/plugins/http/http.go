package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
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
	sq      chan *aio.Message
	workers []*HttpWorker
}

type Addr struct {
	Headers map[string]string `json:"headers,omitempty"`
	Url     string            `json:"url"`
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Http, error) {
	sq := make(chan *aio.Message, config.Size)
	workers := make([]*HttpWorker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &HttpWorker{
			i:       i,
			sq:      sq,
			config:  config,
			client:  &http.Client{Timeout: config.Timeout},
			aio:     a,
			metrics: metrics,
		}
	}

	return &Http{
		sq:      sq,
		workers: workers,
	}, nil
}

func (h *Http) String() string {
	return fmt.Sprintf("%s:http", t_aio.Sender.String())
}

func (h *Http) Type() string {
	return "http"
}

func (h *Http) Start(chan<- error) error {
	for _, worker := range h.workers {
		go worker.Start()
	}

	return nil
}

func (h *Http) Stop() error {
	close(h.sq)
	return nil
}

func (h *Http) Enqueue(msg *aio.Message) bool {
	select {
	case h.sq <- msg:
		return true
	default:
		return false
	}
}

// Worker

type HttpWorker struct {
	i       int
	sq      <-chan *aio.Message
	config  *Config
	client  *http.Client
	aio     aio.AIO
	metrics *metrics.Metrics
}

func (w *HttpWorker) String() string {
	return fmt.Sprintf("%s:http", t_aio.Sender.String())
}

func (w *HttpWorker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		msg, ok := <-w.sq
		if !ok {
			return
		}

		counter.Inc()
		success, err := w.Process(msg.Addr, msg.Body)
		msg.Done(&t_aio.SenderCompletion{
			Success:     success,
			Error:       err,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
		counter.Dec()
	}
}

func (w *HttpWorker) Process(data []byte, body []byte) (bool, error) {
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

	for k, v := range addr.Headers { // nosemgrep: range-over-map
		req.Header.Set(k, v)
	}

	// set non-overridable headers
	req.Header.Set("Content-Type", "application/json")

	res, err := w.client.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == http.StatusOK, nil
}
