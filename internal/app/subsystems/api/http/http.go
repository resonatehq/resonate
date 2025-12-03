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
	"github.com/go-viper/mapstructure/v2"
	"github.com/prometheus/client_golang/prometheus"
	cmdUtil "github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal"
	"github.com/resonatehq/resonate/internal/app/subsystems/api"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

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

func (c *Config) Bind(cmd *cobra.Command, flg *pflag.FlagSet, vip *viper.Viper, name string, prefix string, keyPrefix string) {
	cmdUtil.Bind(c, cmd, flg, vip, name, prefix, keyPrefix)
}

func (c *Config) Decode(value any, decodeHook mapstructure.DecodeHookFunc) error {
	decoderConfig := &mapstructure.DecoderConfig{
		Result:     c,
		DecodeHook: decodeHook,
	}

	decoder, err := mapstructure.NewDecoder(decoderConfig)
	if err != nil {
		return err
	}

	if err := decoder.Decode(value); err != nil {
		return err
	}

	return nil
}

func (c *Config) New(a i_api.API, metrics *metrics.Metrics) (i_api.Subsystem, error) {
	return New(a, metrics, c)
}

type Http struct {
	config   *Config
	listen   net.Listener
	server   *http.Server
	shutdown chan struct{}
}

func New(a i_api.API, metrics *metrics.Metrics, config *Config) (i_api.Subsystem, error) {
	gin.SetMode(gin.ReleaseMode)

	handler := gin.New()
	server := &server{api: api.New(a, "http"), config: config}

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

	// Middleware for logging and metrics
	handler.Use(func(c *gin.Context) {
		path := c.FullPath()
		slog.Debug("http", "method", c.Request.Method, "url", c.Request.RequestURI)

		if !strings.HasPrefix(path, "/poll/") {
			timer := prometheus.NewTimer(metrics.HttpRequestsDuration.WithLabelValues(c.Request.Method, path))
			defer timer.ObserveDuration()
		}

		c.Next()
		metrics.HttpRequestsTotal.WithLabelValues(c.Request.Method, path, fmt.Sprintf("%d", c.Writer.Status())).Inc()
	})

	// CORS
	if len(config.Cors.AllowOrigins) > 0 {
		handler.Use(cors.New(cors.Config{
			AllowOrigins:  config.Cors.AllowOrigins,
			AllowMethods:  []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
			AllowHeaders:  []string{"Origin", "Content-Length", "Content-Type", "Idempotency-Key", "Strict"},
			ExposeHeaders: []string{"Resonate-Version"},
		}))
	}

	// Authentication
	authorized := handler.Group("/")
	if len(config.Auth) > 0 {
		authorized.Use(gin.BasicAuth(config.Auth))
	}

	// Resonate header middleware
	authorized.Use(func(c *gin.Context) {
		c.Header("Resonate-Version", internal.Version())
		c.Next()
	})

	// Extract and normalize Authorization header
	authorized.Use(func(c *gin.Context) {
		if authHeader := c.GetHeader("Authorization"); authHeader != "" {
			if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
				authHeader = authHeader[7:] // Skip "Bearer "
			}
			c.Set("authorization", authHeader)
		}
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
	var pollAddr string
	for _, plugin := range a.Plugins() {
		if plugin.Type() == "poll" {
			pollAddr = plugin.Addr()
			break
		}
	}

	if pollAddr != "" {
		target, err := url.Parse(fmt.Sprintf("http://%s", pollAddr))
		if err != nil {
			return nil, err
		}

		proxy := httputil.NewSingleHostReverseProxy(target)

		authorized.GET("/poll/*rest", func(c *gin.Context) {
			ctx, cancel := context.WithCancel(c.Request.Context())
			defer cancel()

			// Run this request through the api middleware
			metadata := map[string]string{}
			if auth := c.GetString("authorization"); auth != "" {
				metadata["authorization"] = auth
			}
			_, err := server.api.Process(c.GetHeader("RequestId"), &t_api.Request{
				Metadata: metadata,
				Payload:  &t_api.NoopRequest{},
			})

			if err != nil {
				c.JSON(server.code(err.Code), gin.H{"error": err})
				return
			}

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
