package serve

import (
	"fmt"
	"log/slog"
	netHttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queue"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func ServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the durable promise server",
		RunE: func(cmd *cobra.Command, args []string) error {
			// config
			config, err := util.NewConfig()
			if err != nil {
				return err
			}

			// defaults
			if config.System.Url == "" {
				host := config.API.Subsystems.Http.Host
				if host == "0.0.0.0" {
					host = "127.0.0.1"
				}
				config.System.Url = fmt.Sprintf("http://%s:%d", host, config.API.Subsystems.Http.Port)
			}

			// logger
			logLevel, err := log.ParseLevel(config.Log.Level)
			if err != nil {
				slog.Error("failed to parse log level", "error", err)
				return err
			}
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
			slog.SetDefault(logger)

			// instantiate metrics
			reg := prometheus.NewRegistry()
			metrics := metrics.New(reg)

			// instantiate api/aio
			api := api.New(config.API.Size, metrics)
			aio := aio.New(config.AIO.Size, metrics)

			// instantiate api subsystems
			http := http.New(api, config.API.Subsystems.Http)
			grpc := grpc.New(api, config.API.Subsystems.Grpc)

			// instantiate aio subsystems
			store, err := util.NewStore(config.AIO.Subsystems.Store)
			if err != nil {
				return err
			}

			// add api subsystems
			api.AddSubsystem(http)
			api.AddSubsystem(grpc)

			// add aio subsystems
			aio.AddSubsystem(t_aio.Store, store, config.AIO.Subsystems.Store.Subsystem)
			aio.AddSubsystem(t_aio.Queue, queue.New(), config.AIO.Subsystems.Queue.Subsystem)

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
			system := system.New(api, aio, config.System, metrics)
			system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
			system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
			system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
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

			system.AddBackground("TimeoutPromises", coroutines.TimeoutPromises)
			system.AddBackground("EnqueueTasks", coroutines.EnqueueTasks)
			system.AddBackground("TimeoutTasks", coroutines.TimeoutTasks)

			// TODO: migrate to system coroutines
			system.AddOnTick(10*time.Second, "SchedulePromises", coroutines.SchedulePromises)
			system.AddOnTick(10*time.Second, "TimeoutLocks", coroutines.TimeoutLocks)

			// metrics server
			mux := netHttp.NewServeMux()
			mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

			metricsServer := &netHttp.Server{
				Addr:    fmt.Sprintf(":%d", config.Metrics.Port),
				Handler: mux,
			}

			go func() {
				for {
					slog.Info("starting metrics server", "addr", metricsServer.Addr)

					if err := metricsServer.ListenAndServe(); err != nil {
						if err == netHttp.ErrServerClosed {
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

	// api
	cmd.Flags().Int("api-size", 100, "size of the submission queue buffered channel")
	cmd.Flags().String("api-http-host", "0.0.0.0", "http server host")
	cmd.Flags().Int("api-http-port", 8001, "http server port")
	cmd.Flags().Duration("api-http-timeout", 10*time.Second, "http server graceful shutdown timeout")
	cmd.Flags().StringToString("api-http-auth", map[string]string{}, "basic auth username/password pairs")
	cmd.Flags().String("api-grpc-host", "0.0.0.0", "grpc server host")
	cmd.Flags().Int("api-grpc-port", 50051, "grpc server port")

	_ = viper.BindPFlag("api.size", cmd.Flags().Lookup("api-size"))
	_ = viper.BindPFlag("api.subsystems.http.host", cmd.Flags().Lookup("api-http-host"))
	_ = viper.BindPFlag("api.subsystems.http.port", cmd.Flags().Lookup("api-http-port"))
	_ = viper.BindPFlag("api.subsystems.http.timeout", cmd.Flags().Lookup("api-http-timeout"))
	_ = viper.BindPFlag("api.subsystems.http.auth", cmd.Flags().Lookup("api-http-auth"))
	_ = viper.BindPFlag("api.subsystems.grpc.host", cmd.Flags().Lookup("api-grpc-host"))
	_ = viper.BindPFlag("api.subsystems.grpc.port", cmd.Flags().Lookup("api-grpc-port"))

	// aio
	cmd.Flags().Int("aio-size", 100, "size of the completion queue buffered channel")
	cmd.Flags().Int("aio-queue-size", 100, "size of queue submission queue buffered channel")
	cmd.Flags().Int("aio-queue-workers", 1, "number of concurrent connections to the queue")
	cmd.Flags().Int("aio-queue-batch-size", 100, "max submissions processed each tick by a queue worker")
	cmd.Flags().String("aio-store", "sqlite", "promise store type")
	cmd.Flags().Int("aio-store-size", 100, "size of store submission queue buffered channel")
	cmd.Flags().Int("aio-store-workers", 1, "number of concurrent connections to the store")
	cmd.Flags().Int("aio-store-batch-size", 100, "max submissions processed each tick by a store worker")
	cmd.Flags().String("aio-store-sqlite-path", "resonate.db", "sqlite database path")
	cmd.Flags().Duration("aio-store-sqlite-tx-timeout", 10*time.Second, "sqlite transaction timeout")
	cmd.Flags().Bool("aio-store-sqlite-reset", false, "sqlite database clean on shutdown")
	cmd.Flags().String("aio-store-postgres-host", "localhost", "postgres host")
	cmd.Flags().String("aio-store-postgres-port", "5432", "postgres port")
	cmd.Flags().String("aio-store-postgres-username", "", "postgres username")
	cmd.Flags().String("aio-store-postgres-password", "", "postgres password")
	cmd.Flags().String("aio-store-postgres-database", "resonate", "postgres database name")
	cmd.Flags().StringToString("aio-store-postgres-query", make(map[string]string, 0), "postgres query options")
	cmd.Flags().Duration("aio-store-postgres-tx-timeout", 10*time.Second, "postgres transaction timeout")
	cmd.Flags().Bool("aio-store-postgres-reset", false, "postgres database clean on shutdown")

	_ = viper.BindPFlag("aio.size", cmd.Flags().Lookup("aio-size"))
	_ = viper.BindPFlag("aio.subsystems.queue.subsystem.size", cmd.Flags().Lookup("aio-queue-size"))
	_ = viper.BindPFlag("aio.subsystems.queue.subsystem.workers", cmd.Flags().Lookup("aio-queue-workers"))
	_ = viper.BindPFlag("aio.subsystems.queue.subsystem.batchSize", cmd.Flags().Lookup("aio-queue-batch-size"))
	_ = viper.BindPFlag("aio.subsystems.store.config.kind", cmd.Flags().Lookup("aio-store"))
	_ = viper.BindPFlag("aio.subsystems.store.subsystem.size", cmd.Flags().Lookup("aio-store-size"))
	_ = viper.BindPFlag("aio.subsystems.store.subsystem.workers", cmd.Flags().Lookup("aio-store-workers"))
	_ = viper.BindPFlag("aio.subsystems.store.subsystem.batchSize", cmd.Flags().Lookup("aio-store-batch-size"))
	_ = viper.BindPFlag("aio.subsystems.store.config.sqlite.path", cmd.Flags().Lookup("aio-store-sqlite-path"))
	_ = viper.BindPFlag("aio.subsystems.store.config.sqlite.txTimeout", cmd.Flags().Lookup("aio-store-sqlite-tx-timeout"))
	_ = viper.BindPFlag("aio.subsystems.store.config.sqlite.reset", cmd.Flags().Lookup("aio-store-sqlite-reset"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.host", cmd.Flags().Lookup("aio-store-postgres-host"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.port", cmd.Flags().Lookup("aio-store-postgres-port"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.username", cmd.Flags().Lookup("aio-store-postgres-username"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.password", cmd.Flags().Lookup("aio-store-postgres-password"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.database", cmd.Flags().Lookup("aio-store-postgres-database"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.database", cmd.Flags().Lookup("aio-store-postgres-database"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.query", cmd.Flags().Lookup("aio-store-postgres-query"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.txTimeout", cmd.Flags().Lookup("aio-store-postgres-tx-timeout"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.reset", cmd.Flags().Lookup("aio-store-postgres-reset"))

	// system
	cmd.Flags().String("system-url", "", "resonate server url")
	cmd.Flags().Int("system-coroutine-max-size", 1000, "max number of coroutines to run concurrently")
	cmd.Flags().Int("system-submission-batch-size", 100, "max number of submissions to process on each tick")
	cmd.Flags().Int("system-completion-batch-size", 100, "max number of completions to process on each tick")
	cmd.Flags().Int("system-promise-batch-size", 1000, "max number of promises to process on each iteration")
	cmd.Flags().Int("system-schedule-batch-size", 1000, "max number of schedules to process on each iteration")
	cmd.Flags().Int("system-task-batch-size", 1000, "max number of tasks to process on each iteration")
	cmd.Flags().Duration("system-task-enqueue-delay", 10*time.Second, "time to wait before attempting to reenqueue a task")

	_ = viper.BindPFlag("system.url", cmd.Flags().Lookup("system-url"))
	_ = viper.BindPFlag("system.coroutineMaxSize", cmd.Flags().Lookup("system-coroutine-max-size"))
	_ = viper.BindPFlag("system.submissionBatchSize", cmd.Flags().Lookup("system-submission-batch-size"))
	_ = viper.BindPFlag("system.completionBatchSize", cmd.Flags().Lookup("system-completion-batch-size"))
	_ = viper.BindPFlag("system.promiseBatchSize", cmd.Flags().Lookup("system-promise-batch-size"))
	_ = viper.BindPFlag("system.scheduleBatchSize", cmd.Flags().Lookup("system-schedule-batch-size"))
	_ = viper.BindPFlag("system.taskBatchSize", cmd.Flags().Lookup("system-task-batch-size"))
	_ = viper.BindPFlag("system.taskEnqueueDelay", cmd.Flags().Lookup("system-task-enqueue-delay"))

	// metrics
	cmd.Flags().Int("metrics-port", 9090, "prometheus metrics server port")
	_ = viper.BindPFlag("metrics.port", cmd.Flags().Lookup("metrics-port"))

	// assert
	cmd.Flags().Bool("ignore-asserts", false, "ignore-asserts mode")
	_ = viper.BindPFlag("ignore-asserts", cmd.Flags().Lookup("ignore-asserts"))

	cmd.Flags().SortFlags = false

	return cmd
}
