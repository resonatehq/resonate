package http

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	"log/slog"

	"github.com/gin-contrib/cors"
	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	i_api "github.com/resonatehq/resonate/internal/api"
)

type Config struct {
	Addr          string            `flag:"addr" desc:"http server address" default:":8001"`
	Auth          map[string]string `flag:"auth" desc:"http basic auth username password pairs"`
	Cors          Cors              `flag:"cors" desc:"http cors settings"`
	Timeout       time.Duration     `flag:"timeout" desc:"http server graceful shutdown timeout" default:"10s"`
	TaskFrequency time.Duration     `flag:"task-frequency" desc:"default task frequency" default:"1m"`
}

type Cors struct {
	AllowOrigins []string `flag:"allow-origin" desc:"allowed origins, if not provided cors is not enabled"`
}

type Http struct {
	config *Config
	listen net.Listener
	server *http.Server
}

func New(a i_api.API, config *Config) (i_api.Subsystem, error) {
	gin.SetMode(gin.ReleaseMode)

	handler := gin.New()
	server := &server{api: api.New(a, "http"), config: config}

	// Create a listener on specified address
	listen, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return nil, err
	}

	// Register custom validators
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		_ = v.RegisterValidation("oneofcaseinsensitive", oneOfCaseInsensitive)
	}

	// Middleware
	handler.Use(server.log)

	// CORS
	if len(config.Cors.AllowOrigins) > 0 {
		handler.Use(cors.New(cors.Config{
			AllowOrigins: config.Cors.AllowOrigins,
		}))
	}

	// Authentication
	authorized := handler.Group("/")
	if len(config.Auth) > 0 {
		authorized.Use(gin.BasicAuth(config.Auth))
	}

	// Promises API
	authorized.POST("/promises", server.createPromise)
	authorized.POST("/promises/task", server.createPromiseAndTask)
	authorized.GET("/promises", server.searchPromises)
	authorized.GET("/promises/*id", server.readPromise)
	authorized.PATCH("/promises/*id", server.completePromise)

	// Callbacks API
	authorized.POST("/callbacks", server.createCallback)

	// Subscriptions API
	authorized.POST("/subscriptions", server.createSubscription)

	// Schedules API
	authorized.POST("/schedules", server.createSchedule)
	authorized.GET("/schedules", server.searchSchedules)
	authorized.GET("/schedules/*id", server.readSchedule)
	authorized.DELETE("/schedules/*id", server.deleteSchedule)

	// Locks API
	authorized.POST("/locks/acquire", server.acquireLock)
	authorized.POST("/locks/release", server.releaseLock)
	authorized.POST("/locks/heartbeat", server.heartbeatLocks)

	// Tasks API
	authorized.POST("/tasks/claim", server.claimTask)
	authorized.GET("/tasks/claim/:id/:counter", server.claimTask)
	authorized.POST("/tasks/complete", server.completeTask)
	authorized.GET("/tasks/complete/:id/:counter", server.completeTask)
	authorized.POST("/tasks/heartbeat", server.heartbeatTasks)
	authorized.GET("/tasks/heartbeat/:id/:counter", server.heartbeatTasks)

	return &Http{
		config: config,
		listen: listen,
		server: &http.Server{Handler: handler},
	}, nil
}

func (h *Http) String() string {
	return "http"
}

func (h *Http) Kind() string {
	return "http"
}

func (h *Http) Addr() string {
	return h.listen.Addr().String()
}

func (h *Http) Start(errors chan<- error) {
	// Start the http server
	slog.Info("starting http server", "addr", h.config.Addr)
	if err := h.server.Serve(h.listen); err != nil && err != http.ErrServerClosed {
		errors <- err
	}
}

func (h *Http) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
	defer cancel()

	return h.server.Shutdown(ctx)
}

type server struct {
	api    *api.API
	config *Config
}

func (s *server) code(status t_api.StatusCode) int {
	return int(status) / 100
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
