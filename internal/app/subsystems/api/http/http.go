package http

import (
	"context"
	"net/http"
	"time"

	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/kernel/bus"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
)

type Config struct {
	Addr    string
	Timeout time.Duration
}

type Http struct {
	config *Config
	server *http.Server
}

func New(api api.API, config *Config) api.Subsystem {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	s := &server{api: api}

	// Middleware
	r.Use(s.log)

	// Promise API
	r.GET("/promises", s.searchPromises)
	r.GET("/promises/:id", s.readPromise)
	r.POST("/promises/:id/create", s.createPromise)
	r.POST("/promises/:id/cancel", s.cancelPromise)
	r.POST("/promises/:id/resolve", s.resolvePromise)
	r.POST("/promises/:id/reject", s.rejectPromise)

	return &Http{
		config: config,
		server: &http.Server{
			Addr:    config.Addr,
			Handler: r,
		},
	}
}

func (h *Http) Start(errors chan<- error) {
	slog.Info("starting http server", "addr", h.config.Addr)
	if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errors <- err
	}
}

func (h *Http) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
	defer cancel()

	return h.server.Shutdown(ctx)
}

func (h *Http) String() string {
	return "http"
}

type server struct {
	api api.API
}

func (s *server) log(c *gin.Context) {
	c.Next()
	slog.Debug("http", "method", c.Request.Method, "url", c.Request.RequestURI, "status", c.Writer.Status())
}

func (s *server) sendOrPanic(cq chan *bus.CQE[t_api.Request, t_api.Response]) func(*t_api.Response, error) {
	return func(completion *t_api.Response, err error) {
		cqe := &bus.CQE[t_api.Request, t_api.Response]{
			Tags:       "http",
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
