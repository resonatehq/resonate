package http

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"log/slog"

	"github.com/gin-contrib/cors"
	"github.com/go-playground/validator/v10"
	"github.com/resonatehq/resonate/internal"
	"github.com/resonatehq/resonate/internal/app/auth"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	i_api "github.com/resonatehq/resonate/internal/api"
)

type Config struct {
	Addr          string        `flag:"addr" desc:"http server address" default:":8001"`
	Auth          auth.Config   `flag:"auth" desc:"http auth settings"`
	Cors          Cors          `flag:"cors" desc:"http cors settings"`
	Timeout       time.Duration `flag:"timeout" desc:"http server graceful shutdown timeout" default:"10s"`
	TaskFrequency time.Duration `flag:"task-frequency" desc:"default task frequency" default:"1m"`
}

type Cors struct {
	AllowOrigins []string `flag:"allow-origin" desc:"allowed origins, if not provided cors is not enabled"`
}

type Http struct {
	config   *Config
	listen   net.Listener
	server   *http.Server
	shutdown chan struct{}
}

func New(a i_api.API, config *Config, pollAddr string) (i_api.Subsystem, error) {
	gin.SetMode(gin.ReleaseMode)

	handler := gin.New()
	server := &server{api: api.New(a, "http"), config: config}

	authenticator, err := auth.New(&config.Auth)
	if err != nil {
		return nil, err
	}

	// create a shutdown channel
	shutdown := make(chan struct{})

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
			AllowMethods: []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowHeaders: []string{"Origin", "Content-Length", "Content-Type", "Idempotency-Key", "Strict"},
		}))
	}

	// Authentication
	authorized := handler.Group("/")
	if middleware := auth.GinMiddleware(authenticator); middleware != nil {
		authorized.Use(middleware)
	}

	// Resonate header middleware
	authorized.Use(func(c *gin.Context) {
		c.Header("Resonate-Version", internal.Version())
		c.Next()
	})

	// Promises API
	authorized.POST("/promises", server.createPromise)
	authorized.POST("/promises/task", server.createPromiseAndTask)
	authorized.GET("/promises", server.searchPromises)
	authorized.GET("/promises/*id", server.readPromise)
	authorized.PATCH("/promises/*id", server.completePromise)
	authorized.POST("/promises/callback/*id", server.createCallback)
	authorized.POST("/promises/subscribe/*id", server.createSubscription)

	// Callbacks API
	// Deprecated, use /promises/callback instead
	authorized.POST("/callbacks", server.createCallback)

	// Subscriptions API
	// Deprecated, use /promises/subscribe instead
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
	authorized.POST("/tasks/drop", server.dropTask)
	authorized.GET("/tasks/drop/:id/:counter", server.dropTask)
	authorized.POST("/tasks/heartbeat", server.heartbeatTasks)
	authorized.GET("/tasks/heartbeat/:id/:counter", server.heartbeatTasks)

	// Poller proxy
	if pollAddr != "" {
		target, err := url.Parse(fmt.Sprintf("http://%s", pollAddr))
		if err != nil {
			return nil, err
		}

		proxy := httputil.NewSingleHostReverseProxy(target)

		authorized.GET("/poll/*rest", func(c *gin.Context) {
			ctx, cancel := context.WithCancel(c.Request.Context())
			defer cancel()

			go func() {
				select {
				case <-shutdown:
					cancel()
				case <-ctx.Done():
				}
			}()

			r := c.Request.WithContext(ctx)
			r.URL.Path = c.Param("rest")

			proxy.ServeHTTP(c.Writer, r)
		})
	}

	return &Http{
		config:   config,
		listen:   listen,
		server:   &http.Server{Handler: handler},
		shutdown: shutdown,
	}, nil
}

func (h *Http) String() string {
	return "http"
}

func (h *Http) Kind() string {
	return "http"
}

func (h *Http) Addr() string {
	return listenAddr(h.listen.Addr())
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

	// close the shutdown channel to immediately close all long polling
	// connections all other connections remain open until complete or
	// the timeout occurs
	close(h.shutdown)

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

func listenAddr(addr net.Addr) string {
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}

	host = strings.Trim(host, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	return net.JoinHostPort(host, port)
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
