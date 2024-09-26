package http

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"log/slog"

	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	i_api "github.com/resonatehq/resonate/internal/api"
)

type Config struct {
	Host    string            `flag:"host" desc:"http server host" default:"0.0.0.0"`
	Port    int               `flag:"port" desc:"http server port" default:"8001"`
	Timeout time.Duration     `flag:"timeout" desc:"http server graceful shutdown timeout" default:"10s"`
	Auth    map[string]string `flag:"auth" desc:"http basic auth username password pairs"`
}

type Http struct {
	addr   string
	config *Config
	server *http.Server
}

func New(a i_api.API, config *Config) i_api.Subsystem {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	s := &server{api: api.New(a, "http")}

	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)

	// Register custom validators
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		_ = v.RegisterValidation("oneofcaseinsensitive", oneOfCaseInsensitive)
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

	// Callbacks API
	authorized.POST("/callbacks", s.createCallback)

	// Schedules API
	authorized.POST("/schedules", s.createSchedule)
	authorized.GET("/schedules", s.searchSchedules)
	authorized.GET("/schedules/*id", s.readSchedule)
	authorized.DELETE("/schedules/*id", s.deleteSchedule)

	// Locks API
	authorized.POST("/locks/acquire", s.acquireLock)
	authorized.POST("/locks/release", s.releaseLock)
	authorized.POST("/locks/heartbeat", s.heartbeatLocks)

	// Tasks API
	authorized.POST("/tasks/claim", s.claimTask)
	authorized.POST("/tasks/complete", s.completeTask)
	authorized.POST("/tasks/heartbeat", s.heartbeatTasks)

	return &Http{
		addr:   addr,
		config: config,
		server: &http.Server{
			Addr:    addr,
			Handler: r,
		},
	}
}

func (h *Http) String() string {
	return "http"
}

func (h *Http) Start(errors chan<- error) {
	slog.Info("starting http server", "addr", h.addr)
	if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errors <- err
	}
}

func (h *Http) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
	defer cancel()

	return h.server.Shutdown(ctx)
}

type server struct {
	api *api.API
}

func (s *server) code(status t_api.StatusCode) int {
	return int(status) / 10
}

func (s *server) log(c *gin.Context) {
	slog.Debug("http", "method", c.Request.Method, "url", c.Request.RequestURI, "status", c.Writer.Status())
	c.Next()
}

// Helper functions

func oneOfCaseInsensitive(f validator.FieldLevel) bool {
	fieldValue := f.Field().String()
	allowedValues := strings.Split(f.Param(), " ")

	for _, allowedValue := range allowedValues {
		if strings.EqualFold(fieldValue, allowedValue) {
			return true
		}
	}

	return false
}
