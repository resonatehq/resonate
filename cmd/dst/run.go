package dst

import (
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep
	netHttp "net/http"
	"os"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
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
	"github.com/spf13/viper"
)

func RunDSTCmd(cfg *config.Config, vip *viper.Viper) *cobra.Command {
	var (
		seed              int64
		ticks             int64
		timeout           time.Duration
		scenario          string
		visualizationPath string
		verbose           bool
		printOps          bool

		reqsPerTick     = util.NewRangeIntFlag(1, 25)
		ids             = util.NewRangeIntFlag(1, 25)
		headers         = util.NewRangeIntFlag(1, 25)
		data            = util.NewRangeIntFlag(1, 25)
		tags            = util.NewRangeIntFlag(1, 25)
		backchannelSize = util.NewRangeIntFlag(1, 1000)
	)

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run deterministic simulation test",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if file, _ := cmd.Flags().GetString("config"); file != "" {
				vip.SetConfigFile(file)
			} else {
				vip.SetConfigName("resonate-dst")
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
			r := rand.New(rand.NewSource(seed))

			hooks := mapstructure.ComposeDecodeHookFunc(
				util.StringToRange(r),
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

			// logger
			logLevel, err := log.ParseLevel(cfg.LogLevel)
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
			api := api.New(cfg.API.Size, metrics)
			aio := aio.NewDST(r, p, metrics)

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
					subsystem, err := subsystem.NewDST(aio, metrics, r, backchannel)
					if err != nil {
						return err
					}
					aio.AddSubsystem(subsystem)
				}
			}

			// start api/aio
			if err := api.Start(); err != nil {
				return err
			}
			if err := aio.Start(); err != nil {
				return err
			}

			// instantiate system
			system := system.New(api, aio, &cfg.System, metrics)

			// request coroutines
			system.AddOnRequest(t_api.PromiseGet, coroutines.ReadPromise)
			system.AddOnRequest(t_api.PromiseSearch, coroutines.SearchPromises)
			system.AddOnRequest(t_api.PromiseCreate, coroutines.CreatePromise)
			system.AddOnRequest(t_api.TaskCreate, coroutines.CreatePromiseAndTask)
			system.AddOnRequest(t_api.PromiseComplete, coroutines.CompletePromise)
			system.AddOnRequest(t_api.CallbackCreate, coroutines.CreateCallback)
			system.AddOnRequest(t_api.ScheduleRead, coroutines.ReadSchedule)
			system.AddOnRequest(t_api.ScheduleSearch, coroutines.SearchSchedules)
			system.AddOnRequest(t_api.ScheduleCreate, coroutines.CreateSchedule)
			system.AddOnRequest(t_api.ScheduleDelete, coroutines.DeleteSchedule)
			system.AddOnRequest(t_api.TaskAcquire, coroutines.ClaimTask)
			system.AddOnRequest(t_api.TaskComplete, coroutines.CompleteTask)
			system.AddOnRequest(t_api.TaskRelease, coroutines.DropTask)
			system.AddOnRequest(t_api.TaskHeartbeat, coroutines.HeartbeatTasks)

			// background coroutines
			if scenario != "lazy" {
				system.AddBackground("TimeoutPromises", coroutines.TimeoutPromises)
				system.AddBackground("SchedulePromises", coroutines.SchedulePromises)
				system.AddBackground("EnqueueTasks", coroutines.EnqueueTasks)
				system.AddBackground("TimeoutTasks", coroutines.TimeoutTasks)
			}

			dst := dst.New(r, &dst.Config{
				Ticks:              ticks,
				Timeout:            timeout,
				VisualizationPath:  visualizationPath,
				Verbose:            verbose,
				PrintOps:           printOps,
				TimeElapsedPerTick: 1000, // ms
				TimeoutTicks:       t,
				ReqsPerTick:        func() int { return reqsPerTick.Resolve(r) },
				MaxReqsPerTick:     int64(reqsPerTick.Max()),
				Ids:                ids.Resolve(r),
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

	// bind config file flag
	cmd.Flags().StringP("config", "c", "", "config file (default resonate-dst.yaml)")

	// dst related values
	cmd.Flags().Int64Var(&seed, "seed", 0, "dst seed")
	cmd.Flags().Int64Var(&ticks, "ticks", 1000, "number of ticks")
	cmd.Flags().DurationVar(&timeout, "timeout", 1*time.Hour, "timeout")
	cmd.Flags().StringVar(&scenario, "scenario", "default", "can be one of: default, fault, lazy")
	cmd.Flags().StringVar(&visualizationPath, "visualization-path", "dst.html", "porcupine visualization file path")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "log additional information when run is non linearizable")
	cmd.Flags().BoolVar(&printOps, "print-ops", true, "log the request/response pairs of a run.")
	cmd.Flags().Var(reqsPerTick, "reqs-per-tick", "number of requests per tick")
	cmd.Flags().Var(ids, "ids", "promise id set size")
	cmd.Flags().Var(headers, "headers", "promise header set size")
	cmd.Flags().Var(data, "data", "promise data set size")
	cmd.Flags().Var(tags, "tags", "promise tags set size")
	cmd.Flags().Var(backchannelSize, "backchannel-size", "backchannel size")

	// bind config
	util.Bind(cfg, cmd, cmd.Flags(), vip, "dst")

	// bind plugins
	for _, plugin := range cfg.Plugins() {
		plugin.Bind(cmd, cmd.Flags(), vip, "dst")
	}

	cmd.SilenceUsage = true
	return cmd
}
