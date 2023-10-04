package system

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/metrics"

	"github.com/resonatehq/resonate/internal/kernel/scheduler"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/util"
)

type Config struct {
	TimeoutCacheSize      int
	NotificationCacheSize int
	SubmissionBatchSize   int
	CompletionBatchSize   int
}

func (c *Config) String() string {
	return fmt.Sprintf(
		"Config(tcs=%d, ncs=%d, sbs=%d, cbs=%d)",
		c.TimeoutCacheSize,
		c.NotificationCacheSize,
		c.SubmissionBatchSize,
		c.CompletionBatchSize,
	)
}

type System struct {
	api       api.API
	aio       aio.AIO
	config    *Config
	metrics   *metrics.Metrics
	scheduler *scheduler.Scheduler
	onRequest map[t_api.Kind]func(*t_api.Request, func(*t_api.Response, error)) *scheduler.Coroutine
	onTick    map[int][]func(*Config) *scheduler.Coroutine
	ticks     int64
}

func New(api api.API, aio aio.AIO, config *Config, metrics *metrics.Metrics) *System {
	return &System{
		api:       api,
		aio:       aio,
		config:    config,
		metrics:   metrics,
		scheduler: scheduler.NewScheduler(aio, metrics),
		onRequest: map[t_api.Kind]func(*t_api.Request, func(*t_api.Response, error)) *scheduler.Coroutine{},
		onTick:    map[int][]func(*Config) *scheduler.Coroutine{},
	}
}

func (s *System) Loop() error {
	for {
		t := time.Now().UnixMilli()
		s.Tick(t, time.After(10*time.Millisecond))

		if s.api.Done() && s.scheduler.Done() {
			return nil
		}
	}
}

func (s *System) Tick(t int64, timeoutCh <-chan time.Time) {
	defer s.housekeeping(t)

	if !s.api.Done() {
		// add request coroutines
		for _, sqe := range s.api.Dequeue(s.config.SubmissionBatchSize, timeoutCh) {
			if coroutine, ok := s.onRequest[sqe.Submission.Kind]; ok {
				slog.Debug("api:dequeue", "sqe", sqe.Submission)
				s.scheduler.Add(coroutine(sqe.Submission, sqe.Callback))
			} else {
				panic("invalid api request")
			}
		}

		// add tick coroutines
		for _, coroutines := range util.OrderedRangeKV(s.onTick) {
			if s.ticks%int64(coroutines.Key) == 0 {
				for _, coroutine := range coroutines.Value {
					s.scheduler.Add(coroutine(s.config))
				}
			}
		}
	}

	// tick scheduler
	s.scheduler.Tick(t, s.config.CompletionBatchSize)
}

func (s *System) AddOnRequest(kind t_api.Kind, constructor func(*t_api.Request, func(*t_api.Response, error)) *scheduler.Coroutine) {
	s.onRequest[kind] = constructor
}

func (s *System) AddOnTick(n int, constructor func(*Config) *scheduler.Coroutine) {
	util.Assert(n > 0, "n must be greater than 0")
	s.onTick[n] = append(s.onTick[n], constructor)
}

func (s *System) housekeeping(int64) {
	s.ticks++
}

func (s *System) String() string {
	return fmt.Sprintf(
		"System(api=%s, aio=%s, config=%s)",
		s.api,
		s.aio,
		s.config,
	)
}
