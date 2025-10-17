package redpanda

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"http request timeout" default:"5s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
	Endpoints   []string      `flag:"endpoints" desc:"default Redpanda proxy endpoints" default:"http://localhost:8082"`
	Topic       string        `flag:"topic" desc:"default topic" default:""`
}

type Addr struct {
	Endpoint string            `json:"endpoint,omitempty"`
	Topic    string            `json:"topic,omitempty"`
	Key      *string           `json:"key,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
}

type clientFactory func(timeout time.Duration) *http.Client

type Redpanda struct {
	sq      chan *aio.Message
	workers []*Worker
}

type Worker struct {
	i        int
	sq       <-chan *aio.Message
	config   *Config
	metrics  *metrics.Metrics
	client   *http.Client
	endpoint string
	timeout  time.Duration
}

func New(a aio.AIO, metrics *metrics.Metrics, config *Config) (*Redpanda, error) {
	return newWithFactory(a, metrics, config, func(timeout time.Duration) *http.Client {
		return &http.Client{Timeout: timeout}
	})
}

func newWithFactory(a aio.AIO, metrics *metrics.Metrics, config *Config, factory clientFactory) (*Redpanda, error) {
	if len(config.Endpoints) == 0 {
		return nil, fmt.Errorf("redpanda: at least one endpoint must be configured")
	}

	sq := make(chan *aio.Message, config.Size)
	workers := make([]*Worker, config.Workers)
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	for i := 0; i < config.Workers; i++ {
		workers[i] = &Worker{
			i:        i,
			sq:       sq,
			config:   config,
			metrics:  metrics,
			client:   factory(timeout),
			endpoint: config.Endpoints[i%len(config.Endpoints)],
			timeout:  timeout,
		}
	}

	return &Redpanda{
		sq:      sq,
		workers: workers,
	}, nil
}

func (r *Redpanda) String() string {
	return fmt.Sprintf("%s:redpanda", t_aio.Sender.String())
}

func (r *Redpanda) Type() string {
	return "redpanda"
}

func (r *Redpanda) Start(chan<- error) error {
	for _, worker := range r.workers {
		go worker.Start()
	}

	return nil
}

func (r *Redpanda) Stop() error {
	close(r.sq)
	return nil
}

func (r *Redpanda) Enqueue(msg *aio.Message) bool {
	select {
	case r.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) String() string {
	return fmt.Sprintf("%s:redpanda", t_aio.Sender.String())
}

func (w *Worker) Start() {
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
		if err != nil {
			slog.Warn("failed to send task", "err", err)
		}

		msg.Done(&t_aio.SenderCompletion{
			Success:     success,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
		counter.Dec()
	}
}

func (w *Worker) Process(data []byte, body []byte) (bool, error) {
	addr := &Addr{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, addr); err != nil {
			return false, err
		}
	}

	endpoint := w.endpoint
	if addr.Endpoint != "" {
		endpoint = addr.Endpoint
	}
	endpoint = strings.TrimRight(endpoint, "/")
	if endpoint == "" {
		return false, fmt.Errorf("redpanda: endpoint must be provided")
	}

	topic := addr.Topic
	if topic == "" {
		topic = w.configTopic()
	}
	if topic == "" {
		return false, fmt.Errorf("redpanda: topic must be provided")
	}

	record := map[string]any{
		"value": base64.StdEncoding.EncodeToString(body),
	}
	if addr.Key != nil {
		record["key"] = base64.StdEncoding.EncodeToString([]byte(*addr.Key))
	}
	if len(addr.Headers) > 0 {
		headers := make([]map[string]string, 0, len(addr.Headers))
		for k, v := range addr.Headers { // nosemgrep: range-over-map
			headers = append(headers, map[string]string{
				"key":   k,
				"value": base64.StdEncoding.EncodeToString([]byte(v)),
			})
		}
		record["headers"] = headers
	}

	payload := map[string]any{"records": []map[string]any{record}}
	buf, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}

	reqURL, err := url.JoinPath(endpoint, "topics", topic)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(buf))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/vnd.kafka.binary.v2+json")

	res, err := w.client.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return false, fmt.Errorf("redpanda: unexpected status %d", res.StatusCode)
	}

	return true, nil
}

func (w *Worker) configTopic() string {
	return w.config.Topic
}
