package http

import (
	"context"
	"net/http"
	"time"

	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/types"
)

type Http struct {
	addr    string
	timeout time.Duration
	server  *http.Server
}

func New(api api.API, addr string, timeout time.Duration, reg *prometheus.Registry) api.Subsystem {
	r := gin.Default()
	s := &server{api: api}

	// Promise API
	r.GET("/promises", s.searchPromises)
	r.GET("/promises/:id", s.readPromise)
	r.GET("/promises/:id/subscriptions", s.readSubscriptions)
	r.POST("/promises/:id/create", s.createPromise)
	r.POST("/promises/:id/cancel", s.cancelPromise)
	r.POST("/promises/:id/resolve", s.resolvePromise)
	r.POST("/promises/:id/reject", s.rejectPromise)

	// Subscription API
	r.POST("/subscriptions", s.createSubscription)
	r.DELETE("/subscriptions/:id", s.deleteSubscription)

	// Metrics
	r.GET("/metrics", gin.WrapH(promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})))

	return &Http{
		addr:    addr,
		timeout: timeout,
		server: &http.Server{
			Addr:    addr,
			Handler: r,
		},
	}
}

func (h *Http) Start(errors chan<- error) {
	slog.Info("starting http server", "addr", h.addr)
	if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errors <- err
	}
}

func (h *Http) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	return h.server.Shutdown(ctx)
}

type server struct {
	api api.API
}

func (s *server) sendOrPanic(cq chan *bus.CQE[types.Request, types.Response]) func(completion *types.Response, err error) {
	return func(completion *types.Response, err error) {
		cqe := &bus.CQE[types.Request, types.Response]{
			Completion: completion,
			Error:      err,
		}

		select {
		case cq <- cqe:
		default:
			panic("response channel must not block")
		}
	}
}
