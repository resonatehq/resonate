package nats

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/internal/plugins"
)

type Config struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"100"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"1"`
	Timeout     time.Duration `flag:"timeout" desc:"nats request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"0"`
	Servers     []string      `flag:"servers" desc:"nats server addresses" default:"localhost:4222"`
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

type Conn interface {
	Publish(subject string, data []byte) error
	Close()
}

type Worker struct {
	i       int
	sq      <-chan *plugins.Message
	timeout time.Duration
	metrics *metrics.Metrics
	config  *Config
	conn    Conn
}

type Nats struct {
	sq      chan *plugins.Message
	workers []*Worker
	conn    *nats.Conn
}

type Addr struct {
	Subject string            `json:"subject"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (a *Addr) validate() error {
	if a.Subject == "" {
		return fmt.Errorf("subject required")
	}
	return nil
}

func New(metrics *metrics.Metrics, config *Config) (*Nats, error) {
	servers := strings.Join(config.Servers, ",")

	conn, err := nats.Connect(servers)
	if err != nil {
		return nil, fmt.Errorf("nats connection: %w", err)
	}

	return NewWithConn(metrics, config, conn)
}

func NewWithConn(metrics *metrics.Metrics, config *Config, conn *nats.Conn) (*Nats, error) {
	sq := make(chan *plugins.Message, config.Size)
	workers := make([]*Worker, config.Workers)

	for i := range workers {
		workers[i] = &Worker{
			i:       i,
			sq:      sq,
			timeout: config.Timeout,
			metrics: metrics,
			config:  config,
			conn:    conn,
		}
	}

	return &Nats{sq: sq, workers: workers, conn: conn}, nil
}

func (n *Nats) String() string {
	return fmt.Sprintf("%s:nats", t_aio.Sender.String())
}

func (n *Nats) Type() string {
	return "nats"
}

func (n *Nats) Addr() string {
	return ""
}

func (n *Nats) Start(chan<- error) error {
	for _, worker := range n.workers {
		go worker.Start()
	}

	return nil
}

func (n *Nats) Stop() error {
	close(n.sq)
	if n.conn != nil {
		n.conn.Close()
	}
	return nil
}

func (n *Nats) Enqueue(msg *plugins.Message) bool {
	select {
	case n.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) String() string {
	return fmt.Sprintf("%s:nats", t_aio.Sender.String())
}

func (w *Worker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.i))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for msg := range w.sq {
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
	var addr Addr
	if err := json.Unmarshal(data, &addr); err != nil {
		return false, err
	}

	if err := addr.validate(); err != nil {
		return false, err
	}

	// Publish message to NATS subject
	if err := w.conn.Publish(addr.Subject, body); err != nil {
		return false, err
	}

	return true, nil
}
