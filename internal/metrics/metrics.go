package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	AioTotal           *prometheus.CounterVec
	AioInFlight        *prometheus.GaugeVec
	AioDuration        *prometheus.HistogramVec
	AioWorker          *prometheus.GaugeVec
	AioWorkerInFlight  *prometheus.GaugeVec
	AioConnection      *prometheus.GaugeVec
	ApiTotal           *prometheus.CounterVec
	ApiInFlight        *prometheus.GaugeVec
	ApiDuration        *prometheus.HistogramVec
	CoroutinesTotal    *prometheus.CounterVec
	CoroutinesInFlight *prometheus.GaugeVec
	CoroutinesDuration *prometheus.HistogramVec
	PromisesTotal      *prometheus.CounterVec
	SchedulesTotal     *prometheus.CounterVec
	TasksTotal         *prometheus.CounterVec
}

func New(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		AioTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aio_submissions_total",
			Help: "total number of aio submissions",
		}, []string{"type", "status"}),
		AioInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_submissions_in_flight",
			Help: "number of in flight aio submissions",
		}, []string{"type"}),
		AioDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "aio_duration_seconds",
			Help:    "duration of aio submissions in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		AioWorker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_worker_count",
			Help: "number of aio subsystem workers",
		}, []string{"type"}),
		AioWorkerInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_worker_submissions_in_flight",
			Help: "number of in flight aio submissions",
		}, []string{"type", "worker"}),
		AioConnection: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_connection",
			Help: "number of aio subsystem connections",
		}, []string{"type"}),
		ApiTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "api_requests_total",
			Help: "total number of api requests",
		}, []string{"type", "protocol", "status"}),
		ApiInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "api_requests_in_flight",
			Help: "number of in flight api requests",
		}, []string{"type", "protocol"}),
		ApiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "api_duration_seconds",
			Help:    "duration of api requests in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type", "protocol"}),
		CoroutinesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "coroutines_total",
			Help: "total number of coroutines",
		}, []string{"type"}),
		CoroutinesInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "coroutines_in_flight",
			Help: "number of in flight coroutines",
		}, []string{"type"}),
		CoroutinesDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "coroutines_seconds",
			Help:    "duration of coroutines in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"type"}),
		PromisesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "promises_total",
			Help: "count of promises",
		}, []string{"state"}),
		SchedulesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "schedules_total",
			Help: "count of schedules",
		}, []string{"state"}),
		TasksTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_total",
			Help: "count of tasks",
		}, []string{"state"}),
	}

	metrics.Enable(reg)
	return metrics
}

func (m *Metrics) Enable(reg prometheus.Registerer) {
	reg.MustRegister(m.AioTotal)
	reg.MustRegister(m.AioInFlight)
	reg.MustRegister(m.AioDuration)
	reg.MustRegister(m.AioWorker)
	reg.MustRegister(m.AioWorkerInFlight)
	reg.MustRegister(m.AioConnection)
	reg.MustRegister(m.ApiTotal)
	reg.MustRegister(m.ApiInFlight)
	reg.MustRegister(m.ApiDuration)
	reg.MustRegister(m.CoroutinesTotal)
	reg.MustRegister(m.CoroutinesInFlight)
	reg.MustRegister(m.CoroutinesDuration)
	reg.MustRegister(m.PromisesTotal)
	reg.MustRegister(m.SchedulesTotal)
	reg.MustRegister(m.TasksTotal)
}

func (m *Metrics) Disable(reg prometheus.Registerer) {
	reg.Unregister(m.AioTotal)
	reg.Unregister(m.AioInFlight)
	reg.Unregister(m.AioDuration)
	reg.Unregister(m.AioWorker)
	reg.Unregister(m.AioWorkerInFlight)
	reg.Unregister(m.AioConnection)
	reg.Unregister(m.ApiTotal)
	reg.Unregister(m.ApiInFlight)
	reg.Unregister(m.ApiDuration)
	reg.Unregister(m.CoroutinesTotal)
	reg.Unregister(m.CoroutinesInFlight)
	reg.Unregister(m.CoroutinesDuration)
	reg.Unregister(m.PromisesTotal)
	reg.Unregister(m.SchedulesTotal)
	reg.Unregister(m.TasksTotal)
}
