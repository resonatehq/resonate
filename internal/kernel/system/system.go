package system

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

type Config struct {
	Url                   string        `flag:"url" desc:"resonate server url"`
	CoroutineMaxSize      int           `flag:"coroutine-max-size" desc:"max concurrent coroutines" default:"1000" dst:"1:1000"`
	SubmissionBatchSize   int           `flag:"submission-batch-size" desc:"max submissions processed per tick" default:"1000" dst:"1:1000"`
	CompletionBatchSize   int           `flag:"completion-batch-size" desc:"max completions processed per tick" default:"1000" dst:"1:1000"`
	PromiseBatchSize      int           `flag:"promise-batch-size" desc:"max promises processed per iteration" default:"100" dst:"1:100"`
	PromiseMaxIterations  int           `flag:"promise-max-iterations" desc:"max promise iterations per coroutine" default:"1000" dst:"1:1000"`
	ScheduleBatchSize     int           `flag:"schedule-batch-size" desc:"max schedules processed per iteration" default:"100" dst:"1:100"`
	ScheduleMaxIterations int           `flag:"schedule-max-iterations" desc:"max schedule iterations per coroutine" default:"1000" dst:"1:1000"`
	TaskBatchSize         int           `flag:"task-batch-size" desc:"max tasks processed per iteration" default:"100" dst:"1:100"`
	TaskMaxIterations     int           `flag:"task-max-iterations" desc:"max task iterations per coroutine" default:"1000" dst:"1:1000"`
	SignalTimeout         time.Duration `flag:"signal-timeout" desc:"time to wait for api/aio signal" default:"1s" dst:"1s:10s"`
}

func (c *Config) String() string {
	return fmt.Sprintf(
		"Config(cms=%d, sbs=%d, cbs=%d, pbs=%d, sbs=%d, tbs=%d)",
		c.CoroutineMaxSize,
		c.SubmissionBatchSize,
		c.CompletionBatchSize,
		c.PromiseBatchSize,
		c.ScheduleBatchSize,
		c.TaskBatchSize,
	)
}

type backgroundCoroutine struct {
	coroutine func(map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]
	name      string
	last      int64
	promise   promise.Promise[any]
}

type System struct {
	api          api.API
	aio          aio.AIO
	config       *Config
	metrics      *metrics.Metrics
	scheduler    gocoro.Scheduler[*t_aio.Submission, *t_aio.Completion]
	onRequest    map[t_api.Kind]func(req *t_api.Request, res func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]
	background   []*backgroundCoroutine
	shutdown     chan any
	shortCircuit chan any
}

func New(api api.API, aio aio.AIO, config *Config, metrics *metrics.Metrics) *System {
	return &System{
		api:          api,
		aio:          aio,
		config:       config,
		metrics:      metrics,
		scheduler:    gocoro.New(aio, config.CoroutineMaxSize),
		onRequest:    map[t_api.Kind]func(req *t_api.Request, res func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]{},
		background:   []*backgroundCoroutine{},
		shutdown:     make(chan any),
		shortCircuit: make(chan any),
	}
}

func (s *System) String() string {
	return fmt.Sprintf(
		"System(api=%s, aio=%s, config=%s)",
		s.api,
		s.aio,
		s.config,
	)
}

func (s *System) Loop() error {
	defer close(s.shutdown)

	for {
		// tick first
		s.Tick(time.Now().UnixMilli())

		// complete shutdown if done
		if s.Done() {
			s.aio.Shutdown()
			s.scheduler.Shutdown()
			return nil
		}

		// create signals
		cancel := make(chan any)
		apiSignal := s.api.Signal(cancel)
		aioSignal := s.aio.Signal(cancel)

		// wait for a signal, short circuit, or timeout; whichever occurs
		// first
		select {
		case <-apiSignal:
		case <-aioSignal:
		case <-s.shortCircuit:
		case <-time.After(s.config.SignalTimeout):
		}

		// close the cancel channel so the api and aio can stop listening
		// and wait for the signal channels to close
		close(cancel)
		<-apiSignal
		<-aioSignal
	}
}

func (s *System) Tick(t int64) {
	util.Assert(s.config.SubmissionBatchSize > 0, "submission batch size must be greater than zero")
	util.Assert(s.config.CompletionBatchSize > 0, "completion batch size must be greater than zero")

	// dequeue sqes and cqes
	sqes := s.api.DequeueSQE(s.config.SubmissionBatchSize)
	cqes := s.aio.DequeueCQE(s.config.CompletionBatchSize)
	util.Assert(len(sqes) <= s.config.SubmissionBatchSize, "sqe length must be no greater than the submission batch size")
	util.Assert(len(cqes) <= s.config.CompletionBatchSize, "cqe length must be no greater than the completion batch size")

	// call completion callbacks
	for _, cqe := range cqes {
		cqe.Callback(cqe.Completion, cqe.Error)
	}

	// add background coroutines
	for _, bg := range s.background {
		if len(sqes) > 0 {
			bg.last = 0
		}

		// system is shutting down
		if s.api.Done() {
			break
		}

		// background coroutines are mutually exclusive
		if bg.promise != nil && bg.promise.Pending() {
			continue
		}

		// wait min amount of time between scheduling
		if t-bg.last < s.config.SignalTimeout.Milliseconds() {
			continue
		}

		bg.last = t

		tags := map[string]string{
			"id":   fmt.Sprintf("%s:%d", bg.name, t),
			"name": bg.name,
		}

		if p, ok := gocoro.Add(s.scheduler, bg.coroutine(tags)); ok {
			bg.promise = p
			s.coroutineMetrics(p, tags)
		} else {
			slog.Warn("scheduler queue full", "size", s.config.CoroutineMaxSize)
		}
	}

	// add request coroutines
	for _, sqe := range sqes {
		coroutine, ok := s.onRequest[sqe.Submission.Kind()]
		util.Assert(ok, fmt.Sprintf("no registered coroutine for request kind %s", sqe.Submission.Kind()))

		if p, ok := gocoro.Add(s.scheduler, coroutine(sqe.Submission, sqe.Callback)); ok {
			s.coroutineMetrics(p, sqe.Submission.Metadata)
		} else {
			slog.Warn("scheduler queue full", "size", s.config.CoroutineMaxSize)
			sqe.Callback(nil, t_api.NewError(t_api.StatusSchedulerQueueFull, nil))
		}
	}

	// tick scheduler
	s.scheduler.RunUntilBlocked(t)

	// flush aio
	s.aio.Flush(t)
}

func (s *System) Shutdown() <-chan any {
	// start by shutting down the api
	s.api.Shutdown()

	// short circuit the system loop
	close(s.shortCircuit)

	// return the shutdown channel so the caller can wait on system
	// shutdown
	return s.shutdown
}

func (s *System) Done() bool {
	return s.api.Done() && s.scheduler.Size() == 0
}

func (s *System) AddOnRequest(kind t_api.Kind, constructor func(gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], *t_api.Request) (*t_api.Response, error)) {
	s.onRequest[kind] = func(req *t_api.Request, callback func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
		return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
			util.Assert(req.Metadata != nil, "request tags must be non nil")
			util.Assert(req.Metadata["id"] != "", "id tag must be set")

			// set config
			c.Set("config", s.config)

			// set metrics
			c.Set("metrics", s.metrics)

			timer := prometheus.NewTimer(s.metrics.CoroutinesDuration.WithLabelValues(kind.String()))
			defer timer.ObserveDuration()

			// run coroutine
			res, err := constructor(c, req)

			s.api.EnqueueCQE(&bus.CQE[t_api.Request, t_api.Response]{
				Id:         req.Metadata["id"],
				Completion: res,
				Callback:   callback,
				Error:      err,
			})

			return nil, nil
		}
	}
}

func (s *System) AddBackground(name string, constructor func(gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any], map[string]string) (any, error)) {
	s.background = append(s.background, &backgroundCoroutine{
		name: name,
		coroutine: func(m map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
			return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
				// set config
				c.Set("config", s.config)

				// set metrics
				c.Set("metrics", s.metrics)

				timer := prometheus.NewTimer(s.metrics.CoroutinesDuration.WithLabelValues(name))
				defer timer.ObserveDuration()

				// run coroutine
				return constructor(c, m)
			}
		},
	})
}

// TODO: move this to gocoro
func (s *System) coroutineMetrics(p promise.Promise[any], tags map[string]string) {
	util.Assert(tags != nil, "tags must be set")

	slog.Debug("scheduler:add", "id", tags["id"], "coroutine", tags["name"])
	s.metrics.CoroutinesTotal.WithLabelValues(tags["name"]).Inc()
	s.metrics.CoroutinesInFlight.WithLabelValues(tags["name"]).Inc()

	go func() {
		_, _ = p.Await()
		slog.Debug("scheduler:rmv", "id", tags["id"], "coroutine", tags["name"])
		s.metrics.CoroutinesInFlight.WithLabelValues(tags["name"]).Dec()
	}()
}
