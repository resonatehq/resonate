package serve

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewCmd(cfg *config.Config, vip *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start Resonate server",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if file, _ := cmd.Flags().GetString("config"); file != "" {
				vip.SetConfigFile(file)
			} else {
				vip.SetConfigName("resonate")
				vip.AddConfigPath(".")
				vip.AddConfigPath("$HOME")
			}

			vip.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
			vip.AutomaticEnv()

			if err := vip.ReadInConfig(); err != nil {
				if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
					return err
				}
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			hooks := mapstructure.ComposeDecodeHookFunc(
				util.MapToBytes(),
				mapstructure.StringToTimeDurationHookFunc(),
				mapstructure.StringToSliceHookFunc(","),
			)

			// decode config
			if err := vip.Unmarshal(&cfg, viper.DecodeHook(hooks)); err != nil {
				return err
			}

			// decode plugins
			for _, plugin := range cfg.Plugins() {
				if err := plugin.Decode(vip, hooks); err != nil {
					return err
				}
			}

			return Serve(cfg)
		},
	}

	// bind config file flag
	cmd.Flags().StringP("config", "c", "", "config file (default resonate.yaml)")

	// bind config
	util.Bind(cfg, cmd, cmd.Flags(), vip)

	// bind plugins
	for _, plugin := range cfg.Plugins() {
		plugin.Bind(cmd, cmd.Flags(), vip, cmd.Name())
	}

	// bind other flags
	cmd.Flags().Bool("ignore-asserts", false, "ignore-asserts mode")
	_ = viper.BindPFlag("ignore-asserts", cmd.Flags().Lookup("ignore-asserts"))

	return cmd
}

func Serve(cfg *config.Config) error {
	// logger
	logLevel, err := log.ParseLevel(cfg.LogLevel)
	if err != nil {
		slog.Error("failed to parse log level", "error", err)
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	// metrics
	reg := prometheus.NewRegistry()
	metrics := metrics.New(reg)

	// api/aio
	api := api.New(cfg.API.Size, metrics)
	aio := aio.New(cfg.AIO.Size, metrics)

	// api middleware
	apiMiddleware, err := cfg.API.Middleware()
	if err != nil {
		return err
	}
	for _, middleware := range apiMiddleware {
		api.AddMiddleware(middleware)
	}

	// plugins
	for _, plugin := range cfg.AIO.Plugins.All() {
		if plugin.Enabled() {
			p, err := plugin.New(metrics)
			if err != nil {
				return err
			}
			api.AddPlugin(p)
			aio.AddPlugin(p)
		}
	}

	// api subsystems
	for _, subsystem := range cfg.API.Subsystems.All() {
		if subsystem.Enabled() {
			s, err := subsystem.New(api, metrics)
			if err != nil {
				return err
			}
			api.AddSubsystem(s)
		}
	}

	// aio subsystems
	var storeCreated bool
	for _, subsystem := range cfg.AIO.Subsystems.All() {
		if subsystem.Enabled() {
			// ensure only one store is enabled at a time,
			// this is a hack
			if strings.HasPrefix(subsystem.Name(), "store-") {
				if storeCreated {
					continue
				}
				storeCreated = true
			}
			s, err := subsystem.New(aio, metrics)
			if err != nil {
				return err
			}
			aio.AddSubsystem(s)
		}
	}

	// start api/aio
	if err := aio.Start(); err != nil {
		if migrationErr, ok := err.(*migrations.MigrationError); ok {
			slog.Error("failed to start aio", "error", fmt.Sprintf("Migration %03d_%s failed: %v", migrationErr.Version, migrationErr.Name, migrationErr.Err))
		} else {
			slog.Error("failed to start aio", "error", err)
		}
		return err
	}
	if err := api.Start(); err != nil {
		slog.Error("failed to start api", "error", err)
		return err
	}

	// set default url
	if cfg.System.Url == "" {
		cfg.System.Url = fmt.Sprintf("http://%s", api.Addr())
	}

	// instantiate system
	system := system.New(api, aio, &cfg.System, metrics)

	// request coroutines
	system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
	system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
	system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
	system.AddOnRequest(t_api.CreatePromiseAndTask, coroutines.CreatePromiseAndTask)
	system.AddOnRequest(t_api.CreateCallback, coroutines.CreateCallback)
	system.AddOnRequest(t_api.CompletePromise, coroutines.CompletePromise)
	system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
	system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
	system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
	system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
	system.AddOnRequest(t_api.ClaimTask, coroutines.ClaimTask)
	system.AddOnRequest(t_api.CompleteTask, coroutines.CompleteTask)
	system.AddOnRequest(t_api.DropTask, coroutines.DropTask)
	system.AddOnRequest(t_api.HeartbeatTasks, coroutines.HeartbeatTasks)
	system.AddOnRequest(t_api.Noop, coroutines.Noop)

	// background coroutines
	system.AddBackground("TimeoutPromises", coroutines.TimeoutPromises)
	system.AddBackground("SchedulePromises", coroutines.SchedulePromises)
	system.AddBackground("EnqueueTasks", coroutines.EnqueueTasks)
	system.AddBackground("TimeoutTasks", coroutines.TimeoutTasks)

	// metrics server
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: mux}

	go func() {
		for {
			slog.Info("starting metrics server", "addr", metricsServer.Addr)
			if err := metricsServer.ListenAndServe(); err != nil && err == http.ErrServerClosed {
				return
			}

			slog.Error("restarting metrics server...", "error", err)
			time.Sleep(5 * time.Second)
		}
	}()

	// listen for shutdown signal
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

		// halt until we get a shutdown signal or an error
		// occurs, whichever happens first
		select {
		case s := <-sig:
			slog.Info("shutdown signal received, shutting down", "signal", s)
		case err := <-api.Errors():
			slog.Error("api error received, shutting down", "error", err)
		case err := <-aio.Errors():
			slog.Error("aio error received, shutting down", "error", err)
		}

		// shutdown system
		<-system.Shutdown()

		// shutdown metrics server
		if err := metricsServer.Close(); err != nil {
			slog.Warn("error stopping metrics server", "error", err)
		}
	}()

	// control loop
	if err := system.Loop(); err != nil {
		slog.Error("control loop failed", "error", err)
		return err
	}

	// stop api/aio
	if err := api.Stop(); err != nil {
		slog.Error("failed to stop api", "error", err)
		return err
	}
	if err := aio.Stop(); err != nil {
		slog.Error("failed to stop aio", "error", err)
		return err
	}

	return nil
}
