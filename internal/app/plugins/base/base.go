package base

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/metrics"
)

type BaseConfig struct {
	Size        int           `flag:"size" desc:"submission buffered channel size" default:"1000"`
	Workers     int           `flag:"workers" desc:"number of workers" default:"4"`
	Timeout     time.Duration `flag:"timeout" desc:"request timeout" default:"30s"`
	TimeToRetry time.Duration `flag:"ttr" desc:"time to wait before resending" default:"15s"`
	TimeToClaim time.Duration `flag:"ttc" desc:"time to wait for claim before resending" default:"1m"`
}

type Processor interface {
	Process(body []byte, addr []byte) (bool, error)
}

type Worker struct {
	id        int
	sq        <-chan *aio.Message
	processor Processor
	config    *BaseConfig
	metrics   *metrics.Metrics
	name      string
}

type Plugin struct {
	name    string
	sq      chan *aio.Message
	workers []*Worker
	cleanup func() error
}

func NewPlugin(name string, config *BaseConfig, metrics *metrics.Metrics, processor Processor, cleanup func() error) *Plugin {
	sq := make(chan *aio.Message, config.Size)
	workers := make([]*Worker, config.Workers)

	for i := 0; i < config.Workers; i++ {
		workers[i] = &Worker{
			id:        i,
			sq:        sq,
			processor: processor,
			config:    config,
			metrics:   metrics,
			name:      name,
		}
	}

	return &Plugin{
		name:    name,
		sq:      sq,
		workers: workers,
		cleanup: cleanup,
	}
}

func (p *Plugin) String() string {
	return fmt.Sprintf("%s:%s", t_aio.Sender.String(), p.name)
}

func (p *Plugin) Type() string {
	return p.name
}

func (p *Plugin) Start(chan<- error) error {
	for _, worker := range p.workers {
		go worker.Start()
	}
	return nil
}

func (p *Plugin) Stop() error {
	if p.sq != nil {
		close(p.sq)
	}
	if p.cleanup != nil {
		return p.cleanup()
	}
	return nil
}

func (p *Plugin) Enqueue(msg *aio.Message) bool {
	if p.sq == nil || msg == nil {
		return false
	}

	select {
	case p.sq <- msg:
		return true
	default:
		return false
	}
}

func (w *Worker) String() string {
	return fmt.Sprintf("%s:%s", t_aio.Sender.String(), w.name)
}

func (w *Worker) Start() {
	counter := w.metrics.AioWorkerInFlight.WithLabelValues(w.String(), strconv.Itoa(w.id))
	w.metrics.AioWorker.WithLabelValues(w.String()).Inc()
	defer w.metrics.AioWorker.WithLabelValues(w.String()).Dec()

	for {
		msg, ok := <-w.sq
		if !ok {
			return
		}

		counter.Inc()
		success, err := w.processor.Process(msg.Body, msg.Addr)
		if err != nil {
			slog.Warn("failed to process message", "plugin", w.name, "err", err)
		}

		msg.Done(&t_aio.SenderCompletion{
			Success:     success,
			TimeToRetry: w.config.TimeToRetry.Milliseconds(),
			TimeToClaim: w.config.TimeToClaim.Milliseconds(),
		})
		counter.Dec()
	}
}
