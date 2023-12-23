package dst

import (
	"log/slog"
	"math/rand" // nosemgrep
	netHttp "net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/network"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_aio"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/log"
	"github.com/resonatehq/resonate/test/dst"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func RunDSTCmd() *cobra.Command {
	var (
		seed            int64
		ticks           int64
		reqsPerTick     = util.RangeIntFlag{Min: 1, Max: 1000}
		ids             = util.RangeIntFlag{Min: 1, Max: 1000}
		idempotencyKeys = util.RangeIntFlag{Min: 1, Max: 1000}
		headers         = util.RangeIntFlag{Min: 1, Max: 1000}
		data            = util.RangeIntFlag{Min: 1, Max: 1000}
		tags            = util.RangeIntFlag{Min: 1, Max: 1000}
		urls            = util.RangeIntFlag{Min: 1, Max: 1000}
		retries         = util.RangeIntFlag{Min: 1, Max: 1000}
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run dst",
		RunE: func(cmd *cobra.Command, args []string) error {
			r := rand.New(rand.NewSource(seed))

			// config
			config, err := util.NewConfigDST(r)
			if err != nil {
				return err
			}

			// logger
			logLevel, err := log.ParseLevel(config.Log.Level)
			if err != nil {
				slog.Error("failed to parse log level", "error", err)
				return err
			}
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: logLevel,
				ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
					// suppress time attr
					if attr.Key == "time" {
						return slog.Attr{}
					}

					return attr
				},
			}))
			slog.SetDefault(logger)

			// instantiate metrics
			reg := prometheus.NewRegistry()
			metrics := metrics.New(reg)

			mux := netHttp.NewServeMux()
			mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
			metricsServer := &netHttp.Server{
				Addr:    ":9090",
				Handler: mux,
			}

			go metricsServer.ListenAndServe() // nolint: errcheck

			// instatiate api/aio
			api := api.New(config.API.Size, metrics)
			aio := aio.NewDST(r, metrics)

			// instatiate aio subsystems
			network := network.NewDST(config.AIO.Subsystems.NetworkDST.Config, rand.New(rand.NewSource(r.Int63())))
			store, err := util.NewStore(config.AIO.Subsystems.Store)
			if err != nil {
				return err
			}

			// add api subsystems
			aio.AddSubsystem(t_aio.Network, network)
			aio.AddSubsystem(t_aio.Store, store)

			// start api/aio
			if err := api.Start(); err != nil {
				return err
			}
			if err := aio.Start(); err != nil {
				return err
			}

			// instantiate system
			system := system.New(api, aio, config.System, metrics)
			system.AddOnRequest(t_api.ReadPromise, coroutines.ReadPromise)
			system.AddOnRequest(t_api.SearchPromises, coroutines.SearchPromises)
			system.AddOnRequest(t_api.CreatePromise, coroutines.CreatePromise)
			system.AddOnRequest(t_api.CancelPromise, coroutines.CancelPromise)
			system.AddOnRequest(t_api.ResolvePromise, coroutines.ResolvePromise)
			system.AddOnRequest(t_api.RejectPromise, coroutines.RejectPromise)
			system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
			system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
			system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
			system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
			system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
			system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
			system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)
			system.AddOnTick(2, coroutines.SchedulePromises)
			system.AddOnTick(2, coroutines.TimeoutPromises)
			system.AddOnTick(10, coroutines.NotifySubscriptions)

			reqs := []t_api.Kind{
				// PROMISE
				t_api.ReadPromise,
				t_api.SearchPromises,
				t_api.CreatePromise,
				t_api.CancelPromise,
				t_api.ResolvePromise,
				t_api.RejectPromise,

				// SCHEDULE
				t_api.ReadSchedule,
				t_api.SearchSchedules,
				t_api.CreateSchedule,
				t_api.DeleteSchedule,

				// SUBSCRIPTION
				t_api.ReadSubscriptions,
				t_api.CreateSubscription,
				t_api.DeleteSubscription,
			}

			dst := dst.New(&dst.Config{
				Ticks: ticks,
				Reqs: func() int {
					return reqsPerTick.Resolve(r)
				},
				Ids:             ids.Resolve(r),
				IdempotencyKeys: idempotencyKeys.Resolve(r),
				Headers:         headers.Resolve(r),
				Data:            data.Resolve(r),
				Tags:            tags.Resolve(r),
				Urls:            urls.Resolve(r),
				Retries:         retries.Resolve(r),
			})

			slog.Info("DST", "seed", seed, "ticks", ticks, "reqs", reqsPerTick.String(), "dst", dst, "system", system)
			if errs := dst.Run(r, api, aio, system, reqs); len(errs) > 0 {
				return errs[0]
			}

			// reset store
			if err := store.Reset(); err != nil {
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

	cmd.Flags().Int64Var(&seed, "seed", 0, "dst seed")
	cmd.Flags().Int64Var(&ticks, "ticks", 1000, "number of ticks")

	// dst related values
	cmd.Flags().Var(&reqsPerTick, "reqs-per-tick", "number of requests per tick")
	cmd.Flags().Var(&ids, "ids", "number promise ids")
	cmd.Flags().Var(&idempotencyKeys, "idempotency-keys", "number promise idempotency keys")
	cmd.Flags().Var(&headers, "headers", "number promise headers")
	cmd.Flags().Var(&data, "data", "number promise data byte arrays")
	cmd.Flags().Var(&tags, "tags", "number promise tags")
	cmd.Flags().Var(&urls, "urls", "number subscription urls")
	cmd.Flags().Var(&retries, "retries", "number subscription retries")

	// api
	cmd.Flags().Var(&util.RangeIntFlag{Min: 1, Max: 1000000}, "api-size", "size of the submission queue buffered channel")
	_ = viper.BindPFlag("dst.api.size", cmd.Flags().Lookup("api-size"))

	// aio
	cmd.Flags().Var(&util.RangeIntFlag{Min: 1, Max: 1000000}, "aio-size", "size of the completion queue buffered channel")
	cmd.Flags().String("aio-store", "sqlite", "promise store type")
	cmd.Flags().Int("aio-store-workers", 1, "number of concurrent connections to the store")
	cmd.Flags().String("aio-store-sqlite-path", ":memory:", "sqlite database path")
	cmd.Flags().Duration("aio-store-sqlite-tx-timeout", 2*time.Second, "sqlite transaction timeout")
	cmd.Flags().String("aio-store-postgres-host", "localhost", "postgres host")
	cmd.Flags().String("aio-store-postgres-port", "5432", "postgres port")
	cmd.Flags().String("aio-store-postgres-username", "", "postgres username")
	cmd.Flags().String("aio-store-postgres-password", "", "postgres password")
	cmd.Flags().String("aio-store-postgres-database", "resonate_dst", "postgres database name")
	cmd.Flags().Duration("aio-store-postgres-tx-timeout", 2*time.Second, "postgres transaction timeout")
	cmd.Flags().Float32("aio-network-success-rate", 0.5, "simulated success rate of http requests")

	_ = viper.BindPFlag("dst.aio.size", cmd.Flags().Lookup("aio-size"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.kind", cmd.Flags().Lookup("aio-store"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.subsystem.workers", cmd.Flags().Lookup("aio-store-workers"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.sqlite.path", cmd.Flags().Lookup("aio-store-sqlite-path"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.sqlite.txTimeout", cmd.Flags().Lookup("aio-store-sqlite-tx-timeout"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.host", cmd.Flags().Lookup("aio-store-postgres-host"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.port", cmd.Flags().Lookup("aio-store-postgres-port"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.username", cmd.Flags().Lookup("aio-store-postgres-username"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.password", cmd.Flags().Lookup("aio-store-postgres-password"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.database", cmd.Flags().Lookup("aio-store-postgres-database"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.txTimeout", cmd.Flags().Lookup("aio-store-postgres-tx-timeout"))
	_ = viper.BindPFlag("dst.aio.subsystems.networkDST.config.p", cmd.Flags().Lookup("aio-network-success-rate"))

	// system
	cmd.Flags().Var(&util.RangeIntFlag{Min: 1, Max: 1000}, "system-notification-cache-size", "max number of notifications to keep in cache")
	cmd.Flags().Var(&util.RangeIntFlag{Min: 1, Max: 1000}, "system-submission-batch-size", "size of the completion queue buffered channel")
	cmd.Flags().Var(&util.RangeIntFlag{Min: 1, Max: 1000}, "system-completion-batch-size", "max number of completions to process on each tick")

	_ = viper.BindPFlag("dst.system.notificationCacheSize", cmd.Flags().Lookup("system-notification-cache-size"))
	_ = viper.BindPFlag("dst.system.submissionBatchSize", cmd.Flags().Lookup("system-submission-batch-size"))
	_ = viper.BindPFlag("dst.system.completionBatchSize", cmd.Flags().Lookup("system-completion-batch-size"))

	cmd.Flags().SortFlags = false

	return cmd
}
