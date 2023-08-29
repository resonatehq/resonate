package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	AioTotal           *prometheus.CounterVec
	AioInFlight        *prometheus.GaugeVec
	ApiTotal           *prometheus.CounterVec
	ApiInFlight        *prometheus.GaugeVec
	CoroutinesTotal    *prometheus.CounterVec
	CoroutinesInFlight *prometheus.GaugeVec
}

func New(reg prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		AioTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aio_total_submissions",
			Help: "Total number of aio submissions",
		}, []string{"type", "status"}),
		AioInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "aio_in_flight_submissions",
			Help: "Number of in flight aio submissions",
		}, []string{"type"}),
		ApiTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "api_total_requests",
			Help: "Total number of api requests",
		}, []string{"type", "status"}),
		ApiInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "api_in_flight_requests",
			Help: "Number of in flight api requests",
		}, []string{"type"}),
		CoroutinesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "coroutines_total",
			Help: "Total number of coroutines",
		}, []string{"type"}),
		CoroutinesInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "coroutines_in_flight",
			Help: "Number of in flight coroutines",
		}, []string{"type"}),
	}

	metrics.Enable(reg)
	return metrics
}

func (m *Metrics) Enable(reg prometheus.Registerer) {
	reg.MustRegister(m.AioTotal)
	reg.MustRegister(m.AioInFlight)
	reg.MustRegister(m.ApiTotal)
	reg.MustRegister(m.ApiInFlight)
	reg.MustRegister(m.CoroutinesTotal)
	reg.MustRegister(m.CoroutinesInFlight)
}

func (m *Metrics) Disable(reg prometheus.Registerer) {
	reg.Unregister(m.AioTotal)
	reg.Unregister(m.AioInFlight)
	reg.Unregister(m.ApiTotal)
	reg.Unregister(m.ApiInFlight)
	reg.Unregister(m.CoroutinesTotal)
	reg.Unregister(m.CoroutinesInFlight)
}
