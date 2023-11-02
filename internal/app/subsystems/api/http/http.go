package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"

	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/resonatehq/resonate/internal/api"
)

type Config struct {
	Port    int
	Timeout time.Duration
}

type Http struct {
	config *Config
	server *http.Server
}

func New(api api.API, config *Config) api.Subsystem {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	s := &server{service: service.New(api, "http")}

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
			Addr:    fmt.Sprintf("0.0.0.0:%d", config.Port),
			Handler: r,
		},
	}
}

func (h *Http) Start(errors chan<- error) {
	slog.Info("starting http server", "addr", h.server.Addr)
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
	service *service.Service
}

func (s *server) log(c *gin.Context) {
	c.Next()
	slog.Debug("http", "method", c.Request.Method, "url", c.Request.RequestURI, "status", c.Writer.Status())
}
