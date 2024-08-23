package system

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/gocoro/pkg/promise"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

type Config struct {
	Url                 string        `flag:"url" desc:"resonate server url"`
	CoroutineMaxSize    int           `flag:"coroutine-max-size" desc:"max concurrent coroutines" default:"1000" dst:"1:1000"`
	SubmissionBatchSize int           `flag:"submission-batch-size" desc:"max submissions processed per tick" default:"1000" dst:"1:1000"`
	CompletionBatchSize int           `flag:"completion-batch-size" desc:"max completions processed per tick" default:"1000" dst:"1:1000"`
	PromiseBatchSize    int           `flag:"promise-batch-size" desc:"max promises processed per iteration" default:"100" dst:"1:100"`
	ScheduleBatchSize   int           `flag:"schedule-batch-size" desc:"max schedules processed per iteration" default:"100" dst:"1:100"`
	TaskBatchSize       int           `flag:"task-batch-size" desc:"max tasks processed per iteration" default:"100" dst:"1:100"`
	TaskEnqueueDelay    time.Duration `flag:"task-enqueue-delay" desc:"time delay before attempting to reenqueue tasks" default:"10s" dst:"1s:10s"`
	SignalTimeout       time.Duration `flag:"signal-timeout" desc:"time to wait for api/aio signal" default:"1s" dst:"1s:10s"`
}

func (c *Config) String() string {
	return fmt.Sprintf(
		"Config(cms=%d, sbs=%d, cbs=%d, pbs=%d, sbs=%d, tbs=%d, ted=%s)",
		c.CoroutineMaxSize,
		c.SubmissionBatchSize,
		c.CompletionBatchSize,
		c.PromiseBatchSize,
		c.ScheduleBatchSize,
		c.TaskBatchSize,
		c.TaskEnqueueDelay.String(),
	)
}

type coroutineTick struct {
	coroutine func(*Config, map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]
	name      string
	last      int64
	every     time.Duration
}

type backgroundCoroutine struct {
	coroutine func(*Config, map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]
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
	onTick       []*coroutineTick
	background   []*backgroundCoroutine
	shutdown     chan interface{}
	shortCircuit chan interface{}
}

func New(api api.API, aio aio.AIO, config *Config, metrics *metrics.Metrics) *System {
	return &System{
		api:          api,
		aio:          aio,
		config:       config,
		metrics:      metrics,
		scheduler:    gocoro.New(aio, config.CoroutineMaxSize),
		onRequest:    map[t_api.Kind]func(req *t_api.Request, res func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]{},
		onTick:       []*coroutineTick{},
		shutdown:     make(chan interface{}),
		shortCircuit: make(chan interface{}),
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
		cancel := make(chan interface{})
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

	// dequeue sqes
	sqes := s.api.Dequeue(s.config.SubmissionBatchSize)
	util.Assert(len(sqes) <= s.config.SubmissionBatchSize, "sqes must be no greater than the submission batch size")

	// dequeue cqes
	for _, cqe := range s.aio.Dequeue(s.config.CompletionBatchSize) {
		cqe.Callback(cqe.Completion, cqe.Error)
	}

	// cqes := s.aio.Dequeue(s.config.CompletionBatchSize)
	// util.Assert(len(cqes) <= s.config.CompletionBatchSize, "cqes must be no greater than the completion batch size")

	// add tick coroutines
	for _, tick := range s.onTick {
		// add the coroutine if the interval has elapsed and the api sq is
		// not done (which means we are shutting down)
		// if every is set to 0 never add the coroutine
		if tick.every != 0 && (t-tick.last) > int64(tick.every.Milliseconds()) && !s.api.Done() {
			tick.last = t

			tags := map[string]string{
				"request_id": fmt.Sprintf("%s:%d", tick.name, t),
				"name":       tick.name,
			}

			if p, ok := gocoro.Add(s.scheduler, tick.coroutine(s.config, tags)); ok {
				s.coroutineMetrics(p, tags)
			} else {
				slog.Warn("scheduler queue full", "size", s.config.CoroutineMaxSize)
			}
		}
	}

	// add background coroutines
	for _, bg := range s.background {
		if !s.api.Done() && (t-bg.last) >= int64(s.config.SignalTimeout.Milliseconds()) && (bg.promise == nil || bg.promise.Completed()) {
			bg.last = t

			tags := map[string]string{
				"request_id": fmt.Sprintf("%s:%d", bg.name, t),
				"name":       bg.name,
			}

			if p, ok := gocoro.Add(s.scheduler, bg.coroutine(s.config, tags)); ok {
				bg.promise = p
				s.coroutineMetrics(p, tags)
			} else {
				slog.Warn("scheduler queue full", "size", s.config.CoroutineMaxSize)
			}
		}
	}

	// add request coroutines
	for _, sqe := range sqes {
		coroutine, ok := s.onRequest[sqe.Submission.Kind]
		util.Assert(ok, fmt.Sprintf("no registered coroutine for request kind %s", sqe.Submission.Kind))

		if p, ok := gocoro.Add(s.scheduler, coroutine(sqe.Submission, sqe.Callback)); ok {
			s.coroutineMetrics(p, sqe.Submission.Tags)
		} else {
			slog.Warn("scheduler queue full", "size", s.config.CoroutineMaxSize)
			sqe.Callback(nil, t_api.NewResonateError(t_api.ErrSchedulerQueueFull, "scheduler queue full", nil))
		}
	}

	// tick scheduler
	s.scheduler.RunUntilBlocked(t)

	// flush aio
	s.aio.Flush(t)
}

func (s *System) Shutdown() <-chan interface{} {
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
	s.onRequest[kind] = func(req *t_api.Request, res func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any] {
		return func(c gocoro.Coroutine[*t_aio.Submission, *t_aio.Completion, any]) (any, error) {
			res(constructor(c, req))
			return nil, nil
		}
	}
}

func (s *System) AddOnTick(every time.Duration, name string, constructor func(*Config, map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]) {
	util.Assert(every >= 0, "frequency must be non negative")
	s.onTick = append(s.onTick, &coroutineTick{
		coroutine: constructor,
		name:      name,
		every:     every,
	})
}

func (s *System) AddBackground(name string, constructor func(*Config, map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]) {
	s.background = append(s.background, &backgroundCoroutine{
		coroutine: constructor,
		name:      name,
	})
}

// TODO: move this to gocoro
func (s *System) coroutineMetrics(p promise.Promise[any], tags map[string]string) {
	util.Assert(tags != nil, "tags must be set")

	id := tags["request_id"]
	name := tags["name"]

	slog.Debug("scheduler:add", "id", id, "coroutine", name)
	s.metrics.CoroutinesTotal.WithLabelValues(name).Inc()
	s.metrics.CoroutinesInFlight.WithLabelValues(name).Inc()

	go func() {
		_, _ = p.Await()
		slog.Debug("scheduler:rmv", "id", id, "coroutine", name)
		s.metrics.CoroutinesInFlight.WithLabelValues(name).Dec()
	}()
}
