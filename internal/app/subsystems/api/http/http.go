package http

import (
	"context"
	"net/http"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/service"
	"github.com/resonatehq/resonate/internal/util"

	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/resonatehq/resonate/internal/api"
)

type Credential struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type CredentialsList struct {
	Users []Credential `yaml:"users"`
}

type Config struct {
	Addr    string
	Timeout time.Duration
	Auth    CredentialsList
}

type Http struct {
	config *Config
	server *http.Server
}

func BasicAuthMiddleware(config *Config) gin.HandlerFunc {
	credentials := util.GetProcessedCreds(config.Auth)
	if credentials != nil {
		return gin.BasicAuth(credentials)
	}
	return nil
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
	authorized := r.Group("/")
	authorized.Use(BasicAuthMiddleware(config))

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

	// Distributed Locks API
	authorized.POST("/locks/acquire", s.acquireLock)
	authorized.POST("/locks/heartbeat", s.heartbeatLocks)
	authorized.POST("/locks/release", s.releaseLock)

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
	c.Next()
	slog.Debug("http", "method", c.Request.Method, "url", c.Request.RequestURI, "status", c.Writer.Status())
}
