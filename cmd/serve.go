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
	"github.com/resonatehq/resonate/pkg/log"
	"github.com/spf13/cobra"
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
		aio.AddSubsystem(t_aio.Network, network, config.AIO.Subsystems.Network.Subsystem)
		aio.AddSubsystem(t_aio.Store, store, config.AIO.Subsystems.Store.Subsystem)

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

var serveFlags = FlagGroups{
	{
		Message: "api",
		Commands: []*ResonateFlag{
			{
				flag:  "api-size",
				value: 100,
				usage: "size of the submission queue buffered channel",
			},
			{
				flag:  "api-subsystems-http-addr",
				value: "0.0.0.0:8001",
				usage: "http server address",
			},
			{
				flag:  "api-subsystems-http-timeout",
				value: 10 * time.Second,
				usage: "http server graceful shutdown timeout",
			},
			{
				flag:  "api-subsystems-grpc-addr",
				value: "0.0.0.0:50051",
				usage: "grpc server address",
			},
		},
	},
	{
		Message: "aio",
		Commands: []*ResonateFlag{
			{
				flag:  "aio-size",
				value: 100,
				usage: "size of the completion queue buffered channel",
			},
			{
				flag:  "aio-subsystems-store-subsystem-size",
				value: 100,
				usage: "size of store submission queue buffered channel",
			},
			{
				flag:  "aio-subsystems-store-subsystem-workers",
				value: 1,
				usage: "number of concurrent connections to the store",
			},
			{
				flag:  "aio-subsystems-store-subsystem-batchSize",
				value: 100,
				usage: "max submissions processed each tick by a store worker",
			},
			{
				flag:  "aio-subsystems-store-config-kind",
				value: "sqlite",
				usage: "promise store type",
			},
			{
				flag:  "aio-subsystems-store-config-sqlite-path",
				value: "resonate.db",
				usage: "sqlite database path",
			},
			{
				flag:  "aio-subsystems-store-config-sqlite-txTimeout",
				value: 250 * time.Millisecond,
				usage: "sqlite transaction timeout",
			},
			{
				flag:  "aio-subsystems-store-config-postgres-host",
				value: "localhost",
				usage: "postgres host",
			},
			{
				flag:  "aio-subsystems-store-config-postgres-port",
				value: "5432",
				usage: "postgres port",
			},
			{
				flag:  "aio-subsystems-store-config-postgres-username",
				value: "",
				usage: "postgres username",
			},
			{
				flag:  "aio-subsystems-store-config-postgres-password",
				value: "",
				usage: "postgres password",
			},
			{
				flag:  "aio-subsystems-store-config-postgres-database",
				value: "resonate",
				usage: "postgres database name",
			},
			{
				flag:  "aio-subsystems-store-config-postgres-txTimeout",
				value: 250 * time.Millisecond,
				usage: "postgres transaction timeout",
			},
			{
				flag:  "aio-subsystems-network-subsystem-size",
				value: 100,
				usage: "size of network submission queue buffered channel",
			},
			{
				flag:  "aio-subsystems-network-subsystem-workers",
				value: 3,
				usage: "number of concurrent http requests",
			},
			{
				flag:  "aio-subsystems-network-subsystem-batchSize",
				value: 100,
				usage: "max submissions processed each tick by a network worker",
			},
			{
				flag:  "aio-subsystems-network-config-timeout",
				value: 10 * time.Second,
				usage: "network request timeout",
			},
		},
	},
	{
		Message: "system",
		Commands: []*ResonateFlag{
			{
				flag:  "system-notificationCacheSize",
				value: 100,
				usage: "max number of notifications to keep in cache",
			},
			{
				flag:  "system-submissionBatchSize",
				value: 100,
				usage: "max number of submissions to process on each tick",
			},
			{
				flag:  "system-completionBatchSize",
				value: 100,
				usage: "max number of completions to process on each tick",
			},
			// TODO: add kernel tick
		},
	},
	{
		Message: "metrics",
		Commands: []*ResonateFlag{
			{
				flag:  "metrics-port",
				value: 9090,
				usage: "prometheus metrics server port",
			},
		},
	},
}

func init() {
	serveFlags.AddFlagsToCommand(serveCmd)
	serveCmd.Flags().SortFlags = false
	rootCmd.AddCommand(serveCmd)
}
