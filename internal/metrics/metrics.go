package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	AioTotal           *prometheus.CounterVec
	AioInFlight        *prometheus.GaugeVec
	AioWorker          *prometheus.GaugeVec
	AioWorkerInFlight  *prometheus.GaugeVec
	AioConnection      *prometheus.GaugeVec
	ApiTotal           *prometheus.CounterVec
	ApiInFlight        *prometheus.GaugeVec
	CoroutinesTotal    *prometheus.CounterVec
	CoroutinesInFlight *prometheus.GaugeVec
	Promises           *prometheus.CounterVec
	Schedules          *prometheus.CounterVec
	Tasks              *prometheus.CounterVec
}

func New(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		AioTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aio_total_submissions",
			Help: "total number of aio submissions",
		}, []string{"type", "status"}),
		AioInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_in_flight_submissions",
			Help: "number of in flight aio submissions",
		}, []string{"type"}),
		AioWorker: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_worker_count",
			Help: "number of aio subsystem workers",
		}, []string{"type"}),
		AioWorkerInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_worker_in_flight_submissions",
			Help: "number of in flight aio submissions",
		}, []string{"type", "worker"}),
		AioConnection: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_connection",
			Help: "number of aio subsystem connections",
		}, []string{"type"}),
		ApiTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "api_total_requests",
			Help: "total number of api requests",
		}, []string{"type", "protocol", "status"}),
		ApiInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "api_in_flight_requests",
			Help: "number of in flight api requests",
		}, []string{"type", "protocol"}),
		CoroutinesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "coroutines_total",
			Help: "total number of coroutines",
		}, []string{"type"}),
		CoroutinesInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "coroutines_in_flight",
			Help: "number of in flight coroutines",
		}, []string{"type"}),
		Promises: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "promises",
			Help: "count of promises",
		}, []string{"state"}),
		Schedules: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "schedules",
			Help: "number of schedules",
		}, []string{"state"}),
		Tasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks",
			Help: "number of tasks",
		}, []string{"state"}),
	}

	metrics.Enable(reg)
	return metrics
}

func (m *Metrics) Enable(reg prometheus.Registerer) {
	reg.MustRegister(m.AioTotal)
	reg.MustRegister(m.AioInFlight)
	reg.MustRegister(m.AioWorker)
	reg.MustRegister(m.AioWorkerInFlight)
	reg.MustRegister(m.AioConnection)
	reg.MustRegister(m.ApiTotal)
	reg.MustRegister(m.ApiInFlight)
	reg.MustRegister(m.CoroutinesTotal)
	reg.MustRegister(m.CoroutinesInFlight)
	reg.MustRegister(m.Promises)
	reg.MustRegister(m.Schedules)
	reg.MustRegister(m.Tasks)
}

func (m *Metrics) Disable(reg prometheus.Registerer) {
	reg.Unregister(m.AioTotal)
	reg.Unregister(m.AioInFlight)
	reg.Unregister(m.AioWorker)
	reg.Unregister(m.AioWorkerInFlight)
	reg.Unregister(m.AioConnection)
	reg.Unregister(m.ApiTotal)
	reg.Unregister(m.ApiInFlight)
	reg.Unregister(m.CoroutinesTotal)
	reg.Unregister(m.CoroutinesInFlight)
	reg.Unregister(m.Promises)
	reg.Unregister(m.Schedules)
	reg.Unregister(m.Tasks)
}
