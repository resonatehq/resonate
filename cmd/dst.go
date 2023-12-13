package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand" // nosemgrep
	netHttp "net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

var (
	// common
	seed int64

	// run command
	ticks           int64
	reqsPerTick     = rangeIntFlag{Min: 1, Max: 1000}
	ids             = rangeIntFlag{Min: 1, Max: 1000}
	idempotencyKeys = rangeIntFlag{Min: 1, Max: 1000}
	headers         = rangeIntFlag{Min: 1, Max: 1000}
	data            = rangeIntFlag{Min: 1, Max: 1000}
	tags            = rangeIntFlag{Min: 1, Max: 1000}
	urls            = rangeIntFlag{Min: 1, Max: 1000}
	retries         = rangeIntFlag{Min: 1, Max: 1000}

	// issue command
	store  string
	reason string
	file   string
	repo   string
	commit string
)

type Issue struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}

const issueFmt = ` # DST Failed
%s

**Seed**
~~~
%d
~~~

**Store**
~~~
%s
~~~

**Commit**
~~~
%s
~~~

**Command**
~~~
go run ./... dst run --seed %d --aio-store %s
~~~

**Logs**
~~~
%s
~~~
`

var dstCmd = &cobra.Command{
	Use:   "dst",
	Short: "Deterministic simulation testing",
}

var dstRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run dst",
	RunE: func(cmd *cobra.Command, args []string) error {
		r := rand.New(rand.NewSource(seed))

		// config
		config, err := NewConfigDST(r)
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
		store, err := NewStore(config.AIO.Subsystems.Store)
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
		system.AddOnRequest(t_api.ReadSubscriptions, coroutines.ReadSubscriptions)
		system.AddOnRequest(t_api.CreateSubscription, coroutines.CreateSubscription)
		system.AddOnRequest(t_api.DeleteSubscription, coroutines.DeleteSubscription)
		system.AddOnRequest(t_api.ReadSchedule, coroutines.ReadSchedule)
		system.AddOnRequest(t_api.CreateSchedule, coroutines.CreateSchedule)
		system.AddOnRequest(t_api.DeleteSchedule, coroutines.DeleteSchedule)
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

			// SUBSCRIPTION
			t_api.ReadSubscriptions,
			t_api.CreateSubscription,
			t_api.DeleteSubscription,

			// SCHEDULE
			t_api.ReadSchedule,
			t_api.CreateSchedule,
			t_api.DeleteSchedule,
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

var dstIssueCmd = &cobra.Command{
	Use:   "issue",
	Short: "Create a GitHub issue for failed dst run",
	RunE: func(cmd *cobra.Command, args []string) error {
		token, ok := os.LookupEnv("GITHUB_TOKEN")
		if !ok {
			return fmt.Errorf("github token not set")
		}

		// read logs file
		logs := "n/a"
		if file != "" {
			var err error
			logs, err = parseLogs(file, 50, 100)
			if err != nil {
				return fmt.Errorf("failed to parse logs")
			}
		}

		// create github issue
		issue := &Issue{
			Title: fmt.Sprintf("DST: %d", seed),
			Body:  fmt.Sprintf(issueFmt, reason, seed, store, commit, seed, store, logs),
		}

		return createGitHubIssue(repo, token, issue)
	},
}

func init() {
	// run command
	dstRunCmd.Flags().Int64Var(&seed, "seed", 0, "dst seed")
	dstRunCmd.Flags().Int64Var(&ticks, "ticks", 1000, "number of ticks")

	// dst related values
	dstRunCmd.Flags().Var(&reqsPerTick, "reqs-per-tick", "number of requests per tick")
	dstRunCmd.Flags().Var(&ids, "ids", "number promise ids")
	dstRunCmd.Flags().Var(&idempotencyKeys, "idempotency-keys", "number promise idempotency keys")
	dstRunCmd.Flags().Var(&headers, "headers", "number promise headers")
	dstRunCmd.Flags().Var(&data, "data", "number promise data byte arrays")
	dstRunCmd.Flags().Var(&tags, "tags", "number promise tags")
	dstRunCmd.Flags().Var(&urls, "urls", "number subscription urls")
	dstRunCmd.Flags().Var(&retries, "retries", "number subscription retries")

	// api
	dstRunCmd.Flags().Var(&rangeIntFlag{Min: 1, Max: 1000000}, "api-size", "size of the submission queue buffered channel")
	_ = viper.BindPFlag("dst.api.size", dstRunCmd.Flags().Lookup("api-size"))

	// aio
	dstRunCmd.Flags().Var(&rangeIntFlag{Min: 1, Max: 1000000}, "aio-size", "size of the completion queue buffered channel")
	dstRunCmd.Flags().String("aio-store", "sqlite", "promise store type")
	dstRunCmd.Flags().Int("aio-store-workers", 1, "number of concurrent connections to the store")
	dstRunCmd.Flags().String("aio-store-sqlite-path", ":memory:", "sqlite database path")
	dstRunCmd.Flags().Duration("aio-store-sqlite-tx-timeout", 2*time.Second, "sqlite transaction timeout")
	dstRunCmd.Flags().String("aio-store-postgres-host", "localhost", "postgres host")
	dstRunCmd.Flags().String("aio-store-postgres-port", "5432", "postgres port")
	dstRunCmd.Flags().String("aio-store-postgres-username", "", "postgres username")
	dstRunCmd.Flags().String("aio-store-postgres-password", "", "postgres password")
	dstRunCmd.Flags().String("aio-store-postgres-database", "resonate_dst", "postgres database name")
	dstRunCmd.Flags().Duration("aio-store-postgres-tx-timeout", 2*time.Second, "postgres transaction timeout")
	dstRunCmd.Flags().Float32("aio-network-success-rate", 0.5, "simulated success rate of http requests")

	_ = viper.BindPFlag("dst.aio.size", dstRunCmd.Flags().Lookup("aio-size"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.kind", dstRunCmd.Flags().Lookup("aio-store"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.subsystem.workers", dstRunCmd.Flags().Lookup("aio-store-workers"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.sqlite.path", dstRunCmd.Flags().Lookup("aio-store-sqlite-path"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.sqlite.txTimeout", dstRunCmd.Flags().Lookup("aio-store-sqlite-tx-timeout"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.host", dstRunCmd.Flags().Lookup("aio-store-postgres-host"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.port", dstRunCmd.Flags().Lookup("aio-store-postgres-port"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.username", dstRunCmd.Flags().Lookup("aio-store-postgres-username"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.password", dstRunCmd.Flags().Lookup("aio-store-postgres-password"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.database", dstRunCmd.Flags().Lookup("aio-store-postgres-database"))
	_ = viper.BindPFlag("dst.aio.subsystems.store.config.postgres.txTimeout", dstRunCmd.Flags().Lookup("aio-store-postgres-tx-timeout"))
	_ = viper.BindPFlag("dst.aio.subsystems.networkDST.config.p", dstRunCmd.Flags().Lookup("aio-network-success-rate"))

	// system
	dstRunCmd.Flags().Var(&rangeIntFlag{Min: 1, Max: 1000}, "system-notification-cache-size", "max number of notifications to keep in cache")
	dstRunCmd.Flags().Var(&rangeIntFlag{Min: 1, Max: 1000}, "system-submission-batch-size", "size of the completion queue buffered channel")
	dstRunCmd.Flags().Var(&rangeIntFlag{Min: 1, Max: 1000}, "system-completion-batch-size", "max number of completions to process on each tick")

	_ = viper.BindPFlag("dst.system.notificationCacheSize", dstRunCmd.Flags().Lookup("system-notification-cache-size"))
	_ = viper.BindPFlag("dst.system.submissionBatchSize", dstRunCmd.Flags().Lookup("system-submission-batch-size"))
	_ = viper.BindPFlag("dst.system.completionBatchSize", dstRunCmd.Flags().Lookup("system-completion-batch-size"))

	dstRunCmd.Flags().SortFlags = false
	dstCmd.AddCommand(dstRunCmd)

	// issue command
	dstIssueCmd.Flags().Int64Var(&seed, "seed", 0, "dst seed")
	dstIssueCmd.Flags().StringVar(&store, "store", "", "dst store")
	dstIssueCmd.Flags().StringVar(&reason, "reason", "", "dst failure reason")
	dstIssueCmd.Flags().StringVar(&file, "file", "", "dst logs file")
	dstIssueCmd.Flags().StringVar(&repo, "repo", "", "github repo")
	dstIssueCmd.Flags().StringVar(&commit, "commit", "", "git commit sha")
	dstCmd.AddCommand(dstIssueCmd)

	rootCmd.AddCommand(dstCmd)
}

func parseLogs(filename string, head int, tail int) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	var result strings.Builder

	scanner := bufio.NewScanner(file)
	lineCount := 0
	lastLines := make([]string, 0, tail)

	for scanner.Scan() {
		line := scanner.Text()
		lineCount++

		if lineCount <= head {
			result.WriteString(line)
			result.WriteString("\n")
		}

		lastLines = append(lastLines, line)
		if len(lastLines) > tail {
			lastLines = lastLines[1:]
		}
	}

	if lineCount > tail {
		result.WriteString("\n...\n\n")
		for _, line := range lastLines {
			result.WriteString(line)
			result.WriteString("\n")
		}
	}

	return result.String(), nil
}

func createGitHubIssue(repo string, token string, issue *Issue) error {
	url := fmt.Sprintf("https://api.github.com/repos/%s/issues", repo)

	// Convert issue to JSON
	payload, err := json.Marshal(issue)
	if err != nil {
		return err
	}

	// Create HTTP request
	req, err := netHttp.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	// Set required headers
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("Content-Type", "application/json")

	// Send the HTTP request
	client := netHttp.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Check the response status
	if res.StatusCode != netHttp.StatusCreated {
		return fmt.Errorf("failed to create GitHub issue, status code: %d", res.StatusCode)
	}

	return nil
}
