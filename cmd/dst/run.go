package dst

import (
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep
	netHttp "net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/api"
	"github.com/resonatehq/resonate/internal/app/coroutines"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/resonatehq/resonate/internal/kernel/t_api"
	"github.com/resonatehq/resonate/internal/metrics"
	"github.com/resonatehq/resonate/pkg/log"
	"github.com/resonatehq/resonate/test/dst"
	"github.com/spf13/cobra"
)

func RunDSTCmd() *cobra.Command {
	var (
		config = &config.ConfigDST{}

		seed              int64
		ticks             int64
		timeout           time.Duration
		scenario          string
		visualizationPath string

		reqsPerTick     = util.NewRangeIntFlag(1, 25)
		ids             = util.NewRangeIntFlag(1, 25)
		idempotencyKeys = util.NewRangeIntFlag(1, 25)
		headers         = util.NewRangeIntFlag(1, 25)
		data            = util.NewRangeIntFlag(1, 25)
		tags            = util.NewRangeIntFlag(1, 25)
		backchannelSize = util.NewRangeIntFlag(1, 1000)
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run deterministic simulation test",
		RunE: func(cmd *cobra.Command, args []string) error {
			r := rand.New(rand.NewSource(seed))

			// config
			if err := config.Parse(r); err != nil {
				return err
			}

			// logger
			logLevel, err := log.ParseLevel(config.LogLevel)
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
			metricsServer := &netHttp.Server{Addr: ":9090", Handler: mux}

			go metricsServer.ListenAndServe() // nolint: errcheck

			var p float64
			var t int64

			// set up scenarios
			switch scenario {
			case "default":
				p = 0
				t = ticks
			case "fault":
				p = r.Float64()
				t = ticks
			case "lazy":
				p = 0
				t = 10
			default:
				return fmt.Errorf("invalid scenario %s", scenario)
			}

			// instantiate backchannel
			backchannel := make(chan interface{}, backchannelSize.Resolve(r))

			// api/aio
			api := api.New(config.API.Size, metrics)
			aio := aio.NewDST(r, p, metrics)

			// api subsystems
			apiSubsystems, err := config.APISubsystems(api)
			if err != nil {
				return err
			}
			for _, subsystem := range apiSubsystems {
				api.AddSubsystem(subsystem)
			}

			// aio subsystems
			aioSubsystems, err := config.AIOSubsystems(aio, metrics, r, backchannel)
			if err != nil {
				return err
			}
			for _, subsystem := range aioSubsystems {
				aio.AddSubsystem(subsystem)
			}

			// start api/aio
			if err := api.Start(); err != nil {
				return err
			}
			if err := aio.Start(); err != nil {
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
			system.AddOnRequest(t_api.CompletePromise, coroutines.CompletePromise)
			system.AddOnRequest(t_api.CreateCallback, coroutines.CreateCallback)
			system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
			system.AddOnRequest(t_api.SearchSchedules, coroutines.SearchSchedules)
			system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
			system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
			system.AddOnRequest(t_api.AcquireLock, coroutines.AcquireLock)
			system.AddOnRequest(t_api.ReleaseLock, coroutines.ReleaseLock)
			system.AddOnRequest(t_api.HeartbeatLocks, coroutines.HeartbeatLocks)
			system.AddOnRequest(t_api.ClaimTask, coroutines.ClaimTask)
			system.AddOnRequest(t_api.CompleteTask, coroutines.CompleteTask)
			system.AddOnRequest(t_api.HeartbeatTasks, coroutines.HeartbeatTasks)

			// background coroutines
			if scenario != "lazy" {
				system.AddBackground("TimeoutPromises", coroutines.TimeoutPromises)
				system.AddBackground("SchedulePromises", coroutines.SchedulePromises)
				system.AddBackground("TimeoutLocks", coroutines.TimeoutLocks)
				system.AddBackground("EnqueueTasks", coroutines.EnqueueTasks)
				system.AddBackground("TimeoutTasks", coroutines.TimeoutTasks)
			}

			dst := dst.New(r, &dst.Config{
				Ticks:              ticks,
				Timeout:            timeout,
				VisualizationPath:  visualizationPath,
				TimeElapsedPerTick: 1000, // ms
				TimeoutTicks:       t,
				ReqsPerTick:        func() int { return reqsPerTick.Resolve(r) },
				MaxReqsPerTick:     int64(reqsPerTick.Max()),
				Ids:                ids.Resolve(r),
				IdempotencyKeys:    idempotencyKeys.Resolve(r),
				Headers:            headers.Resolve(r),
				Data:               data.Resolve(r),
				Tags:               tags.Resolve(r),
				FaultInjection:     p != 0,
				Backchannel:        backchannel,
			})

			slog.Info("DST", "seed", seed, "ticks", ticks, "reqsPerTick", reqsPerTick.String(), "dst", dst, "system", system)

			ok := dst.Run(r, api, aio, system)

			// stop api/aio
			if err := api.Stop(); err != nil {
				return err
			}
			if err := aio.Stop(); err != nil {
				return err
			}

			if !ok {
				return fmt.Errorf("DST failed for seed='%d'", seed)
			}

			return nil
		},
	}

	// dst related values
	cmd.Flags().Int64Var(&seed, "seed", 0, "dst seed")
	cmd.Flags().Int64Var(&ticks, "ticks", 1000, "number of ticks")
	cmd.Flags().DurationVar(&timeout, "timeout", 1*time.Hour, "timeout")
	cmd.Flags().StringVar(&scenario, "scenario", "default", "can be one of: default, fault, lazy")
	cmd.Flags().StringVar(&visualizationPath, "visualization-path", "dst.html", "porcupine visualization file path")
	cmd.Flags().Var(reqsPerTick, "reqs-per-tick", "number of requests per tick")
	cmd.Flags().Var(ids, "ids", "promise id set size")
	cmd.Flags().Var(idempotencyKeys, "idempotency-keys", "idempotency key set size")
	cmd.Flags().Var(headers, "headers", "promise header set size")
	cmd.Flags().Var(data, "data", "promise data set size")
	cmd.Flags().Var(tags, "tags", "promise tags set size")
	cmd.Flags().Var(backchannelSize, "backchannel-size", "backchannel size")

	// bind config
	_ = config.BindDST(cmd)

	cmd.Flags().SortFlags = false

	return cmd
}
