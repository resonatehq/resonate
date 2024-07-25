package http

import (
	"context"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/resonatehq/resonate/internal/api"
)

type Config struct {
	Addr    string
	Auth    map[string]string
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

	// Register custom validators
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		_ = v.RegisterValidation("oneofcaseinsensitive", service.OneOfCaseInsensitive)
	}

	// Middleware
	r.Use(s.log)

	// Authentication
	authorized := r.Group("/")
	if len(config.Auth) > 0 {
		authorized.Use(gin.BasicAuth(config.Auth))
	}

	// Promises API
	authorized.POST("/promises", s.createPromise)
	authorized.GET("/promises", s.searchPromises)
	authorized.GET("/promises/*id", s.readPromise)
	authorized.PATCH("/promises/*id", s.completePromise)

	// Schedules API
	authorized.POST("/schedules", s.createSchedule)
	authorized.GET("/schedules", s.searchSchedules)
	authorized.GET("/schedules/*id", s.readSchedule)
	authorized.DELETE("/schedules/*id", s.deleteSchedule)

	// Locks API
	authorized.POST("/locks/acquire", s.acquireLock)
	authorized.POST("/locks/release", s.releaseLock)
	authorized.POST("/locks/heartbeat", s.heartbeatLocks)

	// Task API
	authorized.POST("/tasks/claim", s.claimTask)
	authorized.POST("/tasks/complete", s.completeTask)

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
	service *service.Service
}

func (s *server) log(c *gin.Context) {
	slog.Debug("http", "method", c.Request.Method, "url", c.Request.RequestURI, "status", c.Writer.Status())
	c.Next()
}
