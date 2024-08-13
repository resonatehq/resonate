package system

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/resonatehq/gocoro"
	"github.com/resonatehq/gocoro/pkg/io"
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
	Url                 string
	CoroutineMaxSize    int
	SubmissionBatchSize int
	CompletionBatchSize int
	PromiseBatchSize    int
	ScheduleBatchSize   int
	TaskBatchSize       int
	TaskEnqueueDelay    time.Duration
}

func (c *Config) String() string {
	return fmt.Sprintf(
		"Config(cms=%d, sbs=%d, cbs=%d, pbs=%d, sbs=%d, tbs=%d, ted=%d)",
		c.CoroutineMaxSize,
		c.SubmissionBatchSize,
		c.CompletionBatchSize,
		c.PromiseBatchSize,
		c.ScheduleBatchSize,
		c.TaskBatchSize,
		c.TaskEnqueueDelay.Milliseconds(),
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
	promise   promise.Promise[any]
}

type System struct {
	api        api.API
	aio        aio.AIO
	config     *Config
	metrics    *metrics.Metrics
	scheduler  gocoro.Scheduler[*t_aio.Submission, *t_aio.Completion]
	onRequest  map[t_api.Kind]func(req *t_api.Request, res func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]
	onTick     []*coroutineTick
	background []*backgroundCoroutine
	shutdown   chan interface{}
}

func New(api api.API, aio aio.AIO, config *Config, metrics *metrics.Metrics) *System {
	return &System{
		api:       api,
		aio:       aio,
		config:    config,
		metrics:   metrics,
		scheduler: gocoro.New(aio, config.CoroutineMaxSize),
		onRequest: map[t_api.Kind]func(req *t_api.Request, res func(*t_api.Response, error)) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]{},
		onTick:    []*coroutineTick{},
		shutdown:  make(chan interface{}),
	}
}

func (s *System) Loop() error {
	defer close(s.shutdown)

	for {
		select {
		case sqe := <-s.api.SQ():
			slog.Debug("api:dequeue", "id", sqe.Submission.Id(), "sqe", sqe)
			s.Tick(time.Now().UnixMilli(), sqe, nil)
		case cqe := <-s.aio.CQ():
			slog.Debug("aio:dequeue", "id", cqe.Completion.Id(), "cqe", cqe)
			s.Tick(time.Now().UnixMilli(), nil, cqe)
		case <-time.After(1 * time.Second):
			s.Tick(time.Now().UnixMilli(), nil, nil)
		}

		if s.Done() {
			// now we can shut down the aio and scheduler
			s.aio.Shutdown()
			s.scheduler.Shutdown()
			return nil
		}
	}
}

func (s *System) Tick(t int64, sqe *bus.SQE[t_api.Request, t_api.Response], cqe *bus.CQE[t_aio.Submission, t_aio.Completion]) {
	util.Assert(sqe == nil || cqe == nil, "one or both of sqe and cqe must be nil")
	util.Assert(s.config.SubmissionBatchSize > 0, "submission batch size must be greater than zero")
	util.Assert(s.config.CompletionBatchSize > 0, "completion batch size must be greater than zero")

	var sqes []*bus.SQE[t_api.Request, t_api.Response]
	var cqes []io.QE

	if sqe != nil {
		sqes = append(sqes, sqe)
	}

	if cqe != nil {
		cqes = append(cqes, cqe)
	}

	// dequeue sqes
	sqes = append(sqes, s.api.Dequeue(s.config.SubmissionBatchSize)...)

	// dequeue cqes
	cqes = append(cqes, s.aio.Dequeue(s.config.CompletionBatchSize)...)

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
		if !s.api.Done() && (bg.promise == nil || bg.promise.Completed()) {
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
	s.scheduler.RunUntilBlocked(t, cqes)

	// the system is now responsible for flushing the aio
	s.aio.Flush(t)
}

func (s *System) Shutdown() <-chan interface{} {
	// start by shutting down the api
	s.api.Shutdown()

	// return the channel so the caller can wait for the system to shutdown
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

func (s *System) AddForever(constructor func() gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]) {
	gocoro.Add(s.scheduler, constructor())
}

func (s *System) AddBackground(name string, constructor func(*Config, map[string]string) gocoro.CoroutineFunc[*t_aio.Submission, *t_aio.Completion, any]) {
	s.background = append(s.background, &backgroundCoroutine{
		coroutine: constructor,
		name:      name,
	})
}

func (s *System) String() string {
	return fmt.Sprintf(
		"System(api=%s, aio=%s, config=%s)",
		s.api,
		s.aio,
		s.config,
	)
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
