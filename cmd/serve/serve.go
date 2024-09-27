package serve

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func ServeCmd() *cobra.Command {
	var (
		config = &config.Config{}
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the durable promise server",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := config.Parse(); err != nil {
				return err
			}

			// logger
			logLevel, err := log.ParseLevel(config.LogLevel)
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
			api := api.New(config.API.Size, metrics)
			aio := aio.New(config.AIO.Size, metrics)

			// api subsystems
			for _, subsystem := range config.API.Subsystems.Instantiate(api) {
				api.AddSubsystem(subsystem)
			}

			// aio subsystems
			subsystems, err := config.AIO.Subsystems.Instantiate(aio, metrics)
			if err != nil {
				return err
			}
			for _, subsystem := range subsystems {
				aio.AddSubsystem(subsystem)
			}

			// start api/aio
			if err := api.Start(); err != nil {
				slog.Error("failed to start api", "error", err)
				return err
			}
			if err := aio.Start(); err != nil {
				slog.Error("failed to start aio", "error", err)
				return err
			}

			// instantiate system
			system := system.New(api, aio, &config.System, metrics)

			// request coroutines
			system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
			system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
			system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
			system.AddOnRequest(t_api.CreatePromiseAndTask, coroutines.CreatePromiseAndTask)
			system.AddOnRequest(t_api.CreatePromiseAndCallback, coroutines.CreatePromiseAndCallback)
			system.AddOnRequest(t_api.CreateCallback, coroutines.CreateCallback)
			system.AddOnRequest(t_api.CompletePromise, coroutines.CompletePromise)
			system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
			system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
			system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
			system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
			system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
			system.AddOnRequest(t_api.HeartbeatLocks, coroutines.HeartbeatLocks)
			system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
			system.AddOnRequest(t_api.ClaimTask, coroutines.ClaimTask)
			system.AddOnRequest(t_api.CompleteTask, coroutines.CompleteTask)
			system.AddOnRequest(t_api.HeartbeatTasks, coroutines.HeartbeatTasks)

			// background coroutines
			system.AddBackground("TimeoutPromises", coroutines.TimeoutPromises)
			system.AddBackground("SchedulePromises", coroutines.SchedulePromises)
			system.AddBackground("TimeoutLocks", coroutines.TimeoutLocks)
			system.AddBackground("EnqueueTasks", coroutines.EnqueueTasks)
			system.AddBackground("TimeoutTasks", coroutines.TimeoutTasks)

			// metrics server
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

			metricsServer := &http.Server{
				Addr:    fmt.Sprintf(":%d", config.MetricsPort),
				Handler: mux,
			}

			go func() {
				for {
					slog.Info("starting metrics server", "addr", metricsServer.Addr)

					if err := metricsServer.ListenAndServe(); err != nil {
						if err == http.ErrServerClosed {
							return
						}

						slog.Error("error starting metrics server", "error", err)
					}

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
		},
	}

	// bind config
	_ = config.Bind(cmd)

	// bind other flags
	cmd.Flags().Bool("ignore-asserts", false, "ignore-asserts mode")
	_ = viper.BindPFlag("ignore-asserts", cmd.Flags().Lookup("ignore-asserts"))

	// maintain defined order of flags
	cmd.Flags().SortFlags = false

	return cmd
}
