package cmd

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
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the durable promises server",
	RunE: func(cmd *cobra.Command, args []string) error {
		// config
		config, err := NewConfig()
		if err != nil {
			return err
		}

		// logger
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: config.Log.Level}))
		slog.SetDefault(logger)

		// instantiate metrics
		reg := prometheus.NewRegistry()
		metrics := metrics.New(reg)

		// instatiate api/aio
		api := api.New(config.API.Size, metrics)
		aio := aio.New(config.AIO.Size, metrics)

		// instatiate api subsystems
		http := http.New(api, config.API.Subsystems.Http)
		grpc := grpc.New(api, config.API.Subsystems.Grpc)

		// instatiate aio subsystems
		network := network.New(config.AIO.Subsystems.Network.Config)
		store, err := NewStore(config.AIO.Subsystems.Store)
		if err != nil {
			return err
		}

		// add api subsystems
		api.AddSubsystem(http)
		api.AddSubsystem(grpc)

		// add api subsystems
		aio.AddSubsystem(t_aio.Network, network, config.AIO.Subsystems.Network.Size, config.AIO.Subsystems.Network.BatchSize, config.AIO.Subsystems.Network.Workers)
		aio.AddSubsystem(t_aio.Store, store, config.AIO.Subsystems.Store.Size, config.AIO.Subsystems.Store.BatchSize, config.AIO.Subsystems.Store.Workers)

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
		system.AddOnRequest(t_api.ResolvePromise, coroutines.ResolvePromise)
		system.AddOnRequest(t_api.RejectPromise, coroutines.RejectPromise)
		system.AddOnRequest(t_api.CancelPromise, coroutines.CancelPromise)
		system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
		system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
		system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)
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
				slog.Info("shutdown signal recieved, shutting down", "signal", s)
			case err := <-api.Errors():
				slog.Error("api error recieved, shutting down", "error", err)
			case err := <-aio.Errors():
				slog.Error("aio error recieved, shutting down", "error", err)
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

func init() {
	// api
	serveCmd.Flags().Int("api-size", 100, "size of the submission queue buffered channel")
	serveCmd.Flags().String("api-http-addr", "0.0.0.0:8001", "http server address")
	serveCmd.Flags().Duration("api-http-timeout", 10*time.Second, "http server graceful shutdown timeout")
	serveCmd.Flags().String("api-grpc-addr", "0.0.0.0:50051", "grpc server address")

	_ = viper.BindPFlag("api.size", serveCmd.Flags().Lookup("api-size"))
	_ = viper.BindPFlag("api.subsystems.http.addr", serveCmd.Flags().Lookup("api-http-addr"))
	_ = viper.BindPFlag("api.subsystems.http.timeout", serveCmd.Flags().Lookup("api-http-timeout"))
	_ = viper.BindPFlag("api.subsystems.grpc.addr", serveCmd.Flags().Lookup("api-grpc-addr"))

	// aio
	serveCmd.Flags().Int("aio-size", 100, "size of the completion queue buffered channel")
	serveCmd.Flags().String("aio-store", "sqlite", "promise store type")
	serveCmd.Flags().Int("aio-store-size", 100, "size of store submission queue buffered channel")
	serveCmd.Flags().Int("aio-store-workers", 1, "number of concurrent connections to the store")
	serveCmd.Flags().Int("aio-store-batch-size", 100, "max submissions processed each tick by a store worker")
	serveCmd.Flags().String("aio-store-sqlite-path", "resonate.db", "sqlite database path")
	serveCmd.Flags().Duration("aio-store-sqlite-tx-timeout", 250*time.Millisecond, "sqlite transaction timeout")
	serveCmd.Flags().String("aio-store-postgres-host", "localhost", "postgres host")
	serveCmd.Flags().String("aio-store-postgres-port", "5432", "postgres port")
	serveCmd.Flags().String("aio-store-postgres-username", "", "postgres username")
	serveCmd.Flags().String("aio-store-postgres-password", "", "postgres password")
	serveCmd.Flags().String("aio-store-postgres-database", "resonate", "postgres database name")
	serveCmd.Flags().Duration("aio-store-postgres-tx-timeout", 250*time.Millisecond, "postgres transaction timeout")
	serveCmd.Flags().Int("aio-network-size", 100, "size of network submission queue buffered channel")
	serveCmd.Flags().Int("aio-network-workers", 3, "number of concurrent http requests")
	serveCmd.Flags().Int("aio-network-batch-size", 100, "max submissions processed each tick by a network worker")
	serveCmd.Flags().Duration("aio-network-timeout", 10*time.Second, "network request timeout")

	_ = viper.BindPFlag("aio.size", serveCmd.Flags().Lookup("aio-size"))
	_ = viper.BindPFlag("aio.subsystems.store.config.kind", serveCmd.Flags().Lookup("aio-store"))
	_ = viper.BindPFlag("aio.subsystems.store.size", serveCmd.Flags().Lookup("aio-store-size"))
	_ = viper.BindPFlag("aio.subsystems.store.workers", serveCmd.Flags().Lookup("aio-store-workers"))
	_ = viper.BindPFlag("aio.subsystems.store.batchSize", serveCmd.Flags().Lookup("aio-store-batch-size"))
	_ = viper.BindPFlag("aio.subsystems.store.config.sqlite.path", serveCmd.Flags().Lookup("aio-store-sqlite-path"))
	_ = viper.BindPFlag("aio.subsystems.store.config.sqlite.txTimeout", serveCmd.Flags().Lookup("aio-store-sqlite-tx-timeout"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.host", serveCmd.Flags().Lookup("aio-store-postgres-host"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.port", serveCmd.Flags().Lookup("aio-store-postgres-port"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.username", serveCmd.Flags().Lookup("aio-store-postgres-username"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.password", serveCmd.Flags().Lookup("aio-store-postgres-password"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.database", serveCmd.Flags().Lookup("aio-store-postgres-database"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.database", serveCmd.Flags().Lookup("aio-store-postgres-database"))
	_ = viper.BindPFlag("aio.subsystems.store.config.postgres.txTimeout", serveCmd.Flags().Lookup("aio-store-postgres-tx-timeout"))
	_ = viper.BindPFlag("aio.subsystems.network.size", serveCmd.Flags().Lookup("aio-network-size"))
	_ = viper.BindPFlag("aio.subsystems.network.workers", serveCmd.Flags().Lookup("aio-network-workers"))
	_ = viper.BindPFlag("aio.subsystems.network.batchSize", serveCmd.Flags().Lookup("aio-network-batch-size"))
	_ = viper.BindPFlag("aio.subsystems.network.config.timeout", serveCmd.Flags().Lookup("aio-network-timeout"))

	// system
	serveCmd.Flags().Int("system-notification-cache-size", 100, "max number of notifications to keep in cache")
	serveCmd.Flags().Int("system-submission-batch-size", 100, "max number of submissions to process on each tick")
	serveCmd.Flags().Int("system-completion-batch-size", 100, "max number of completions to process on each tick")

	_ = viper.BindPFlag("system.notificationCacheSize", serveCmd.Flags().Lookup("system-notification-cache-size"))
	_ = viper.BindPFlag("system.submissionBatchSize", serveCmd.Flags().Lookup("system-submission-batch-size"))
	_ = viper.BindPFlag("system.completionBatchSize", serveCmd.Flags().Lookup("system-completion-batch-size"))

	// metrics
	serveCmd.Flags().Int("metrics-port", 9090, "prometheus metrics server port")
	_ = viper.BindPFlag("metrics.port", serveCmd.Flags().Lookup("metrics-port"))

	serveCmd.Flags().SortFlags = false
	rootCmd.AddCommand(serveCmd)
}
