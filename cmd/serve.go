package cmd

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/types"
	"github.com/spf13/cobra"
)

var (
	httpAddr string
	grpcAddr string
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the durable promises server",
	RunE: func(cmd *cobra.Command, args []string) error {
		// config
		cfg := &system.Config{
			PromiseCacheSize:      100,
			TimeoutCacheSize:      100,
			NotificationCacheSize: 100,
			SubmissionBatchSize:   100,
			CompletionBatchSize:   100,
		}

		// instatiate api/aio
		api := api.New(100)
		aio := aio.New(100)

		// instatiate api subsystems
		http := http.New(api, httpAddr, 10*time.Second)
		grpc := grpc.New(api, grpcAddr)

		// instatiate aio subsystems
		network := network.New(10 * time.Second)
		store, err := sqlite.New(sqlite.Config{Path: "resonate.db"})
		if err != nil {
			return err
		}

		// add subsystems
		api.AddSubsystem(http)
		api.AddSubsystem(grpc)
		aio.AddSubsystem(types.Network, network, 100, 1, 3)
		aio.AddSubsystem(types.Store, store, 100, 10, 1)

		// start api/aio
		if err := api.Start(); err != nil {
			return err
		}
		if err := aio.Start(); err != nil {
			return err
		}

		// instantiate system
		system := system.New(cfg, api, aio)
		system.AddOnRequest(types.ReadPromise, coroutines.ReadPromise)
		system.AddOnRequest(types.SearchPromises, coroutines.SearchPromises)
		system.AddOnRequest(types.CreatePromise, coroutines.CreatePromise)
		system.AddOnRequest(types.ResolvePromise, coroutines.ResolvePromise)
		system.AddOnRequest(types.RejectPromise, coroutines.RejectPromise)
		system.AddOnRequest(types.CancelPromise, coroutines.CancelPromise)
		system.AddOnRequest(types.ReadSubscriptions, coroutines.ReadSubscriptions)
		system.AddOnRequest(types.CreateSubscription, coroutines.CreateSubscription)
		system.AddOnRequest(types.DeleteSubscription, coroutines.DeleteSubscription)
		system.AddOnTick(2, coroutines.TimeoutPromises)
		system.AddOnTick(1, coroutines.NotifySubscriptions)

		go func() {
			sig := make(chan os.Signal, 1)
			signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

			// halt until we get a shutdown signal or an error
			// occurs, whicever happens first
			select {
			case <-sig:
			case <-api.Errors():
			case <-aio.Errors():
			}

			// shutdown api/aio
			api.Shutdown()
			aio.Shutdown()
		}()

		// control loop
		if err := system.Loop(); err != nil {
			return err
		}

		// stop api/aio
		if err := api.Stop(); err != nil {
			return err
		}
		if err := aio.Stop(); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	serveCmd.Flags().StringVarP(&httpAddr, "http", "", "0.0.0.0:8001", "http server address")
	serveCmd.Flags().StringVarP(&grpcAddr, "grpc", "", "0.0.0.0:50051", "grpc server address")
	rootCmd.AddCommand(serveCmd)
}
