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
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/queuing"
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
			network := network.New(config.AIO.Subsystems.Network.Config)
			store, err := util.NewStore(config.AIO.Subsystems.Store)
			if err != nil {
				return err
			}
			queuing, err := queuing.NewSubsytemOrDie(config.AIO.Subsystems.Queuing.Config)
			if err != nil {
				return err
			}

			// add api subsystems
			api.AddSubsystem(http)
			api.AddSubsystem(grpc)

			// add aio subsystems
			aio.AddSubsystem(t_aio.Network, network, config.AIO.Subsystems.Network.Subsystem)
			aio.AddSubsystem(t_aio.Store, store, config.AIO.Subsystems.Store.Subsystem)
			aio.AddSubsystem(t_aio.Queuing, queuing, config.AIO.Subsystems.Queuing.Subsystem)

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
			system.AddOnRequest(t_api.CompletePromise, coroutines.CompletePromise)
			system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
			system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
			system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
			system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
			system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
			system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
			system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)
			system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
			system.AddOnRequest(t_api.HeartbeatLocks, coroutines.HeartbeatLocks)
			system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
			system.AddOnRequest(t_api.ClaimTask, coroutines.ClaimTask)
			system.AddOnRequest(t_api.CompleteTask, coroutines.CompleteTask)
			system.AddOnTick(2, coroutines.EnqueueTasks)
			system.AddOnTick(2, coroutines.TimeoutLocks)
			system.AddOnTick(2, coroutines.SchedulePromises)
			system.AddOnTick(2, coroutines.TimeoutPromises)
			system.AddOnTick(1, coroutines.NotifySubscriptions)

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
				system.Shutdown()

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
	cmd.Flags().String("api-http-addr", "0.0.0.0:8001", "http server address")
	cmd.Flags().Duration("api-http-timeout", 10*time.Second, "http server graceful shutdown timeout")
	cmd.Flags().String("api-grpc-addr", "0.0.0.0:50051", "grpc server address")

	_ = viper.BindPFlag("api.size", cmd.Flags().Lookup("api-size"))
	_ = viper.BindPFlag("api.subsystems.http.addr", cmd.Flags().Lookup("api-http-addr"))
	_ = viper.BindPFlag("api.subsystems.http.timeout", cmd.Flags().Lookup("api-http-timeout"))
	_ = viper.BindPFlag("api.subsystems.grpc.addr", cmd.Flags().Lookup("api-grpc-addr"))

	// aio
	// Store
	cmd.Flags().Int("aio-size", 100, "size of the completion queue buffered channel")
	cmd.Flags().String("aio-store", "sqlite", "promise store type")
	cmd.Flags().Int("aio-store-size", 100, "size of store submission queue buffered channel")
	cmd.Flags().Int("aio-store-workers", 1, "number of concurrent connections to the store")
	cmd.Flags().Int("aio-store-batch-size", 100, "max submissions processed each tick by a store worker")
	cmd.Flags().String("aio-store-sqlite-path", "resonate.db", "sqlite database path")
	cmd.Flags().Duration("aio-store-sqlite-tx-timeout", 250*time.Millisecond, "sqlite transaction timeout")
	cmd.Flags().Bool("aio-store-sqlite-reset", false, "sqlite database clean on shutdown")
	cmd.Flags().String("aio-store-postgres-host", "localhost", "postgres host")
	cmd.Flags().String("aio-store-postgres-port", "5432", "postgres port")
	cmd.Flags().String("aio-store-postgres-username", "", "postgres username")
	cmd.Flags().String("aio-store-postgres-password", "", "postgres password")
	cmd.Flags().String("aio-store-postgres-database", "resonate", "postgres database name")
	cmd.Flags().Duration("aio-store-postgres-tx-timeout", 250*time.Millisecond, "postgres transaction timeout")
	cmd.Flags().Bool("aio-store-postgres-reset", false, "postgres database clean on shutdown")
	// Network
	cmd.Flags().Int("aio-network-size", 100, "size of network submission queue buffered channel")
	cmd.Flags().Int("aio-network-workers", 3, "number of concurrent http requests")
	cmd.Flags().Int("aio-network-batch-size", 100, "max submissions processed each tick by a network worker")
	cmd.Flags().Duration("aio-network-timeout", 10*time.Second, "network request timeout")
	// Queuing
	cmd.Flags().Int("aio-queuing-size", 100, "size of queuing submission queue buffered channel")
	cmd.Flags().Int("aio-queuing-workers", 1, "number of queuing workers") // must be 1.
	cmd.Flags().Int("aio-queuing-batch-size", 100, "max submissions processed each tick by a queuing worker")
	cmd.Flags().Var(&ConnectionSlice{}, "aio-queuing-connections", "queuing subsystem connections")

	// Store
	_ = viper.BindPFlag("aio.size", cmd.Flags().Lookup("aio-size"))
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
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.txTimeout", cmd.Flags().Lookup("aio-store-postgres-tx-timeout"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.reset", cmd.Flags().Lookup("aio-store-postgres-reset"))
	// Network
	_ = viper.BindPFlag("aio.subsystems.network.subsystem.size", cmd.Flags().Lookup("aio-network-size"))
	_ = viper.BindPFlag("aio.subsystems.network.subsystem.workers", cmd.Flags().Lookup("aio-network-workers"))
	_ = viper.BindPFlag("aio.subsystems.network.subsystem.batchSize", cmd.Flags().Lookup("aio-network-batch-size"))
	_ = viper.BindPFlag("aio.subsystems.network.config.timeout", cmd.Flags().Lookup("aio-network-timeout"))
	// Queuing
	_ = viper.BindPFlag("aio.subsystems.queuing.subsystem.size", cmd.Flags().Lookup("aio-queuing-size"))
	_ = viper.BindPFlag("aio.subsystems.queuing.subsystem.workers", cmd.Flags().Lookup("aio-queuing-workers"))
	_ = viper.BindPFlag("aio.subsystems.queuing.subsystem.batchSize", cmd.Flags().Lookup("aio-queuing-batch-size"))
	_ = viper.BindPFlag("aio.subsystems.queuing.config.connections", cmd.Flags().Lookup("aio-queuing-connections"))

	// system
	cmd.Flags().Int("system-notification-cache-size", 100, "max number of notifications to keep in cache")
	cmd.Flags().Int("system-submission-batch-size", 100, "max number of submissions to process on each tick")
	cmd.Flags().Int("system-completion-batch-size", 100, "max number of completions to process on each tick")

	_ = viper.BindPFlag("system.notificationCacheSize", cmd.Flags().Lookup("system-notification-cache-size"))
	_ = viper.BindPFlag("system.submissionBatchSize", cmd.Flags().Lookup("system-submission-batch-size"))
	_ = viper.BindPFlag("system.completionBatchSize", cmd.Flags().Lookup("system-completion-batch-size"))

	// metrics
	cmd.Flags().Int("metrics-port", 9090, "prometheus metrics server port")
	_ = viper.BindPFlag("metrics.port", cmd.Flags().Lookup("metrics-port"))

	cmd.Flags().SortFlags = false

	return cmd
}
