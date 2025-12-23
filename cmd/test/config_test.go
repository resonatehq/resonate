package cmd

import (
	"math/rand" // nosemgrep
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/resonatehq/resonate/cmd/config"
	"github.com/resonatehq/resonate/cmd/dev"
	"github.com/resonatehq/resonate/cmd/dst"
	"github.com/resonatehq/resonate/cmd/migrate"
	"github.com/resonatehq/resonate/cmd/serve"
	"github.com/resonatehq/resonate/cmd/util"
	httpPlugin "github.com/resonatehq/resonate/internal/app/plugins/http"
	"github.com/resonatehq/resonate/internal/app/plugins/poll"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/router"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/sender"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/grpc"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/http"
	"github.com/resonatehq/resonate/internal/kernel/system"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type cmd struct {
	cmd        func(*config.Config, *viper.Viper) *cobra.Command
	persistent bool
}

var devCmd = cmd{cmd: dev.NewCmd, persistent: false}
var dstCmd = cmd{cmd: dst.RunDSTCmd, persistent: false}
var migrateCmd = cmd{cmd: migrate.NewCmd, persistent: true}
var serveCmd = cmd{cmd: serve.NewCmd, persistent: false}

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		cmds     []cmd
		file     string
		args     []string
		plugins  func(*config.Config)
		expected *config.Config
	}{
		{
			name: "default serve and dev",
			cmds: []cmd{serveCmd, devCmd},
			expected: &config.Config{
				System: system.Config{
					Url:                   "",
					CoroutineMaxSize:      1000,
					SubmissionBatchSize:   1000,
					CompletionBatchSize:   1000,
					PromiseBatchSize:      100,
					PromiseMaxIterations:  1000,
					ScheduleBatchSize:     100,
					ScheduleMaxIterations: 1000,
					TaskBatchSize:         100,
					TaskMaxIterations:     1000,
					SignalTimeout:         1 * time.Second,
				},
				API: config.API{
					Size: 1000,
					Auth: config.Auth{
						PublicKey: "",
					},
				},
				AIO: config.AIO{
					Size: 1000,
				},
				MetricsAddr: ":9090",
				LogLevel:    "info",
			},
		},
		{
			name:     "default migrate",
			cmds:     []cmd{migrateCmd},
			expected: &config.Config{}, // migrate only uses plugins
		},
		{
			name: "config from file",
			cmds: []cmd{serveCmd, devCmd, dstCmd},
			file: `
system:
  url: https://resonatehq.io:9000
  coroutineMaxSize: 1
  submissionBatchSize: 2
  completionBatchSize: 3
  promiseBatchSize: 4
  promiseMaxIterations: 40
  scheduleBatchSize: 5
  scheduleMaxIterations: 50
  taskBatchSize: 6
  taskMaxIterations: 60
  signalTimeout: 7s
api:
  size: 1
  auth:
    publicKey: "key1.pub"
aio:
  size: 2
metricsAddr: "localhost:8080"
logLevel: "warn"`,
			expected: &config.Config{
				System: system.Config{
					Url:                   "https://resonatehq.io:9000",
					CoroutineMaxSize:      1,
					SubmissionBatchSize:   2,
					CompletionBatchSize:   3,
					PromiseBatchSize:      4,
					PromiseMaxIterations:  40,
					ScheduleBatchSize:     5,
					ScheduleMaxIterations: 50,
					TaskBatchSize:         6,
					TaskMaxIterations:     60,
					SignalTimeout:         7 * time.Second,
				},
				API: config.API{
					Size: 1,
					Auth: config.Auth{
						PublicKey: "key1.pub",
					},
				},
				AIO: config.AIO{
					Size: 2,
				},
				MetricsAddr: "localhost:8080",
				LogLevel:    "warn",
			},
		},
		{
			name: "config from flags",
			cmds: []cmd{serveCmd, devCmd, dstCmd},
			args: []string{
				"--system-url", "https://resonatehq.io:9001",
				"--system-coroutine-max-size", "2",
				"--system-submission-batch-size", "3",
				"--system-completion-batch-size", "4",
				"--system-promise-batch-size", "5",
				"--system-promise-max-iterations", "50",
				"--system-schedule-batch-size", "6",
				"--system-schedule-max-iterations", "60",
				"--system-task-batch-size", "7",
				"--system-task-max-iterations", "70",
				"--system-signal-timeout", "8s",
				"--api-size", "2",
				"--api-auth-public-key", "key2.pub",
				"--aio-size", "3",
				"--metrics-addr", "localhost:8081",
				"--log-level", "error",
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "https://resonatehq.io:9001",
					CoroutineMaxSize:      2,
					SubmissionBatchSize:   3,
					CompletionBatchSize:   4,
					PromiseBatchSize:      5,
					PromiseMaxIterations:  50,
					ScheduleBatchSize:     6,
					ScheduleMaxIterations: 60,
					TaskBatchSize:         7,
					TaskMaxIterations:     70,
					SignalTimeout:         8 * time.Second,
				},
				API: config.API{
					Size: 2,
					Auth: config.Auth{
						PublicKey: "key2.pub",
					},
				},
				AIO: config.AIO{
					Size: 3,
				},
				MetricsAddr: "localhost:8081",
				LogLevel:    "error",
			},
		},
		{
			name: "config flags take precedence",
			cmds: []cmd{serveCmd, devCmd, dstCmd},
			file: `
system:
  url: https://resonatehq.io:9000
  coroutineMaxSize: 1
  submissionBatchSize: 2
  completionBatchSize: 3
  promiseBatchSize: 4
  promiseMaxIterations: 40
  scheduleBatchSize: 5
  scheduleMaxIterations: 50
  taskBatchSize: 6
  taskMaxIterations: 60
  signalTimeout: 7s
api:
  size: 1
  auth:
    publicKey: "key1.pub"
aio:
  size: 2
metricsAddr: "localhost:8080"
logLevel: "warn"`,
			args: []string{
				"--system-url", "https://resonatehq.io:9001",
				"--system-submission-batch-size", "3",
				"--system-promise-batch-size", "5",
				"--system-task-batch-size", "7",
				"--api-size", "2",
				"--aio-size", "3",
				"--log-level", "error",
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "https://resonatehq.io:9001",
					CoroutineMaxSize:      1,
					SubmissionBatchSize:   3,
					CompletionBatchSize:   3,
					PromiseBatchSize:      5,
					PromiseMaxIterations:  40,
					ScheduleBatchSize:     5,
					ScheduleMaxIterations: 50,
					TaskBatchSize:         7,
					TaskMaxIterations:     60,
					SignalTimeout:         7 * time.Second,
				},
				API: config.API{
					Size: 2,
					Auth: config.Auth{
						PublicKey: "key1.pub",
					},
				},
				AIO: config.AIO{
					Size: 3,
				},
				MetricsAddr: "localhost:8080",
				LogLevel:    "error",
			},
		},
		{
			name: "with plugins serve",
			cmds: []cmd{serveCmd},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
				cfg.AIO.Subsystems.Add("router", true, &router.Config{})
				cfg.AIO.Subsystems.Add("sender", true, &sender.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "",
					CoroutineMaxSize:      1000,
					SubmissionBatchSize:   1000,
					CompletionBatchSize:   1000,
					PromiseBatchSize:      100,
					PromiseMaxIterations:  1000,
					ScheduleBatchSize:     100,
					ScheduleMaxIterations: 1000,
					TaskBatchSize:         100,
					TaskMaxIterations:     1000,
					SignalTimeout:         1 * time.Second,
				},
				API: withAPISubsystems(
					config.API{
						Size: 1000,
						Auth: config.Auth{
							PublicKey: "",
						},
					}, []plugin[config.APISubsystem]{
						{
							name:    "http",
							enabled: true,
							config: &http.Config{
								Addr:          ":8001",
								Auth:          map[string]string{},
								Cors:          http.Cors{AllowOrigins: []string{}},
								Timeout:       10 * time.Second,
								TaskFrequency: 1 * time.Minute,
							},
						},
						{
							name:    "grpc",
							enabled: true,
							config: &grpc.Config{
								Addr: ":50051",
							},
						},
					},
				),
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{
						Size: 1000,
					},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "router",
							enabled: true,
							config: &router.Config{
								Size:    100,
								Workers: 1,
							},
						},
						{
							name:    "sender",
							enabled: true,
							config: &sender.Config{
								Size: 100,
							},
						},
						{
							name:    "store-sqlite",
							enabled: true,
							config: &sqlite.Config{
								Size:      1000,
								BatchSize: 1000,
								Path:      "resonate.db",
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
						{
							name:    "store-postgres",
							enabled: false,
							config: &postgres.Config{
								Size:      1000,
								BatchSize: 1000,
								Workers:   1,
								Host:      "localhost",
								Port:      "5432",
								Database:  "resonate",
								Query:     map[string]string{},
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
					},
					[]plugin[config.AIOPlugin]{
						{
							name:    "http",
							enabled: true,
							config: &httpPlugin.Config{
								Size:        100,
								Workers:     3,
								Timeout:     3 * time.Minute,
								ConnTimeout: 10 * time.Second,
								TimeToRetry: 15 * time.Second,
								TimeToClaim: 1 * time.Minute,
							},
						},
						{
							name:    "poll",
							enabled: true,
							config: &poll.Config{
								Size:            100,
								BufferSize:      100,
								MaxConnections:  1000,
								Addr:            ":8002",
								Cors:            poll.Cors{AllowOrigins: []string{}},
								Timeout:         10 * time.Second,
								DisconnectAfter: 0,
								Auth:            map[string]string{},
								TimeToRetry:     15 * time.Second,
								TimeToClaim:     1 * time.Minute,
							},
						},
					},
				),
				MetricsAddr: ":9090",
				LogLevel:    "info",
			},
		},
		{
			name: "with plugins dev",
			cmds: []cmd{devCmd},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
				cfg.AIO.Subsystems.Add("router", true, &router.Config{})
				cfg.AIO.Subsystems.Add("sender", true, &sender.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "",
					CoroutineMaxSize:      1000,
					SubmissionBatchSize:   1000,
					CompletionBatchSize:   1000,
					PromiseBatchSize:      100,
					PromiseMaxIterations:  1000,
					ScheduleBatchSize:     100,
					ScheduleMaxIterations: 1000,
					TaskBatchSize:         100,
					TaskMaxIterations:     1000,
					SignalTimeout:         1 * time.Second,
				},
				API: withAPISubsystems(
					config.API{
						Size: 1000,
						Auth: config.Auth{
							PublicKey: "",
						},
					}, []plugin[config.APISubsystem]{
						{
							name:    "http",
							enabled: true,
							config: &http.Config{
								Addr:          ":8001",
								Auth:          map[string]string{},
								Cors:          http.Cors{AllowOrigins: []string{}},
								Timeout:       10 * time.Second,
								TaskFrequency: 1 * time.Minute,
							},
						},
						{
							name:    "grpc",
							enabled: true,
							config: &grpc.Config{
								Addr: ":50051",
							},
						},
					},
				),
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{
						Size: 1000,
					},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "router",
							enabled: true,
							config: &router.Config{
								Size:    100,
								Workers: 1,
							},
						},
						{
							name:    "sender",
							enabled: true,
							config: &sender.Config{
								Size: 100,
							},
						},
						{
							name:    "store-sqlite",
							enabled: true,
							config: &sqlite.Config{
								Size:      1000,
								BatchSize: 1000,
								Path:      ":memory:",
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
						{
							name:    "store-postgres",
							enabled: false,
							config: &postgres.Config{
								Size:      1000,
								BatchSize: 1000,
								Workers:   1,
								Host:      "localhost",
								Port:      "5432",
								Database:  "resonate",
								Query:     map[string]string{"sslmode": "disable"},
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
					},
					[]plugin[config.AIOPlugin]{
						{
							name:    "http",
							enabled: true,
							config: &httpPlugin.Config{
								Size:        100,
								Workers:     3,
								Timeout:     3 * time.Minute,
								ConnTimeout: 10 * time.Second,
								TimeToRetry: 15 * time.Second,
								TimeToClaim: 1 * time.Minute,
							},
						},
						{
							name:    "poll",
							enabled: true,
							config: &poll.Config{
								Size:            100,
								BufferSize:      100,
								MaxConnections:  1000,
								Addr:            ":8002",
								Cors:            poll.Cors{AllowOrigins: []string{}},
								Timeout:         10 * time.Second,
								DisconnectAfter: 0,
								Auth:            map[string]string{},
								TimeToRetry:     15 * time.Second,
								TimeToClaim:     1 * time.Minute,
							},
						},
					},
				),
				MetricsAddr: ":9090",
				LogLevel:    "info",
			},
		},
		{
			name: "with plugins migrate",
			cmds: []cmd{migrateCmd},
			plugins: func(cfg *config.Config) {
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
			},
			expected: &config.Config{
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "store-sqlite",
							enabled: true,
							config: &sqlite.Config{
								Size:      1000,
								BatchSize: 1000,
								Path:      "resonate.db",
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
						{
							name:    "store-postgres",
							enabled: false,
							config: &postgres.Config{
								Size:      1000,
								BatchSize: 1000,
								Workers:   1,
								Host:      "localhost",
								Port:      "5432",
								Database:  "resonate",
								Query:     map[string]string{},
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
					},
					nil,
				),
			},
		},
		{
			name: "with serve plugins switched",
			cmds: []cmd{serveCmd},
			args: []string{
				"--api-http-enable=false",
				"--api-grpc-enable=true",
				"--aio-store-sqlite-enable=false",
				"--aio-store-postgres-enable=true",
				"--aio-http-enable=false",
				"--aio-poll-enable=true",
			},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", false, &grpc.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", false, &poll.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "",
					CoroutineMaxSize:      1000,
					SubmissionBatchSize:   1000,
					CompletionBatchSize:   1000,
					PromiseBatchSize:      100,
					PromiseMaxIterations:  1000,
					ScheduleBatchSize:     100,
					ScheduleMaxIterations: 1000,
					TaskBatchSize:         100,
					TaskMaxIterations:     1000,
					SignalTimeout:         1 * time.Second,
				},
				API: withAPISubsystems(
					config.API{
						Size: 1000,
						Auth: config.Auth{
							PublicKey: "",
						},
					}, []plugin[config.APISubsystem]{
						{
							name:    "http",
							enabled: false,
							config: &http.Config{
								Addr:          ":8001",
								Auth:          map[string]string{},
								Cors:          http.Cors{AllowOrigins: []string{}},
								Timeout:       10 * time.Second,
								TaskFrequency: 1 * time.Minute,
							},
						},
						{
							name:    "grpc",
							enabled: true,
							config: &grpc.Config{
								Addr: ":50051",
							},
						},
					},
				),
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{
						Size: 1000,
					},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "store-sqlite",
							enabled: false,
							config: &sqlite.Config{
								Size:      1000,
								BatchSize: 1000,
								Path:      "resonate.db",
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
						{
							name:    "store-postgres",
							enabled: true,
							config: &postgres.Config{
								Size:      1000,
								BatchSize: 1000,
								Workers:   1,
								Host:      "localhost",
								Port:      "5432",
								Database:  "resonate",
								Query:     map[string]string{},
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
					},
					[]plugin[config.AIOPlugin]{
						{
							name:    "http",
							enabled: false,
							config: &httpPlugin.Config{
								Size:        100,
								Workers:     3,
								Timeout:     3 * time.Minute,
								ConnTimeout: 10 * time.Second,
								TimeToRetry: 15 * time.Second,
								TimeToClaim: 1 * time.Minute,
							},
						},
						{
							name:    "poll",
							enabled: true,
							config: &poll.Config{
								Size:            100,
								BufferSize:      100,
								MaxConnections:  1000,
								Addr:            ":8002",
								Cors:            poll.Cors{AllowOrigins: []string{}},
								Timeout:         10 * time.Second,
								DisconnectAfter: 0,
								Auth:            map[string]string{},
								TimeToRetry:     15 * time.Second,
								TimeToClaim:     1 * time.Minute,
							},
						},
					},
				),
				MetricsAddr: ":9090",
				LogLevel:    "info",
			},
		},
		{
			name: "with dev plugins switched",
			cmds: []cmd{devCmd},
			args: []string{
				"--api-http-enable=false",
				"--api-grpc-enable=true",
				"--aio-store-sqlite-enable=false",
				"--aio-store-postgres-enable=true",
				"--aio-http-enable=false",
				"--aio-poll-enable=true",
			},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", false, &grpc.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", false, &poll.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "",
					CoroutineMaxSize:      1000,
					SubmissionBatchSize:   1000,
					CompletionBatchSize:   1000,
					PromiseBatchSize:      100,
					PromiseMaxIterations:  1000,
					ScheduleBatchSize:     100,
					ScheduleMaxIterations: 1000,
					TaskBatchSize:         100,
					TaskMaxIterations:     1000,
					SignalTimeout:         1 * time.Second,
				},
				API: withAPISubsystems(
					config.API{
						Size: 1000,
						Auth: config.Auth{
							PublicKey: "",
						},
					}, []plugin[config.APISubsystem]{
						{
							name:    "http",
							enabled: false,
							config: &http.Config{
								Addr:          ":8001",
								Auth:          map[string]string{},
								Cors:          http.Cors{AllowOrigins: []string{}},
								Timeout:       10 * time.Second,
								TaskFrequency: 1 * time.Minute,
							},
						},
						{
							name:    "grpc",
							enabled: true,
							config: &grpc.Config{
								Addr: ":50051",
							},
						},
					},
				),
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{
						Size: 1000,
					},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "store-sqlite",
							enabled: false,
							config: &sqlite.Config{
								Size:      1000,
								BatchSize: 1000,
								Path:      ":memory:",
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
						{
							name:    "store-postgres",
							enabled: true,
							config: &postgres.Config{
								Size:      1000,
								BatchSize: 1000,
								Workers:   1,
								Host:      "localhost",
								Port:      "5432",
								Database:  "resonate",
								Query:     map[string]string{"sslmode": "disable"},
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
					},
					[]plugin[config.AIOPlugin]{
						{
							name:    "http",
							enabled: false,
							config: &httpPlugin.Config{
								Size:        100,
								Workers:     3,
								Timeout:     3 * time.Minute,
								ConnTimeout: 10 * time.Second,
								TimeToRetry: 15 * time.Second,
								TimeToClaim: 1 * time.Minute,
							},
						},
						{
							name:    "poll",
							enabled: true,
							config: &poll.Config{
								Size:            100,
								BufferSize:      100,
								MaxConnections:  1000,
								Addr:            ":8002",
								Cors:            poll.Cors{AllowOrigins: []string{}},
								Timeout:         10 * time.Second,
								DisconnectAfter: 0,
								Auth:            map[string]string{},
								TimeToRetry:     15 * time.Second,
								TimeToClaim:     1 * time.Minute,
							},
						},
					},
				),
				MetricsAddr: ":9090",
				LogLevel:    "info",
			},
		},
		{
			name: "with migrate plugins switched",
			cmds: []cmd{migrateCmd},
			args: []string{
				"--aio-store-sqlite-enable=false",
				"--aio-store-postgres-enable=true",
			},
			plugins: func(cfg *config.Config) {
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
			},
			expected: &config.Config{
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "store-sqlite",
							enabled: false,
							config: &sqlite.Config{
								Size:      1000,
								BatchSize: 1000,
								Path:      "resonate.db",
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
						{
							name:    "store-postgres",
							enabled: true,
							config: &postgres.Config{
								Size:      1000,
								BatchSize: 1000,
								Workers:   1,
								Host:      "localhost",
								Port:      "5432",
								Database:  "resonate",
								Query:     map[string]string{},
								TxTimeout: 10 * time.Second,
								Reset:     false,
							},
						},
					},
					nil,
				),
			},
		},
		{
			name: "config from file with plugins",
			cmds: []cmd{serveCmd, devCmd, dstCmd},
			file: `
system:
  url: https://resonatehq.io:9000
  coroutineMaxSize: 1
  submissionBatchSize: 2
  completionBatchSize: 3
  promiseBatchSize: 4
  promiseMaxIterations: 40
  scheduleBatchSize: 5
  scheduleMaxIterations: 50
  taskBatchSize: 6
  taskMaxIterations: 60
  signalTimeout: 7s
api:
  size: 1
  auth:
    publicKey: "key1.pub"
  subsystems:
    http:
      enabled: true
      config:
        addr: ":9998"
        auth:
          foo: bar
          baz: qux
        cors:
          allowOrigins:
          - "https://example1.com"
          - "https://example2.com"
        timeout: 15s
        taskFrequency: 2m
    grpc:
      enabled: false
      config:
        addr: ":9999"
aio:
  size: 2
  subsystems:
    store:
      sqlite:
        enabled: true
        config:
          size: 3
          batchSize: 4
          path: "alt.db"
          txTimeout: 5s
          reset: true
      postgres:
        enabled: false
        config:
          size: 6
          batchSize: 7
          workers: 8
          host: "postgres.local"
          port: "9999"
          database: "alt"
          query:
            sslmode: "enable"
          txTimeout: 11s
          reset: true
  plugins:
    http:
      enabled: true
      config:
        size: 12
        workers: 13
        timeout: 13s
        connTimeout: 15s
        timeToRetry: 16s
        timeToClaim: 17m
    poll:
        enabled: false
        config:
          size: 18
          bufferSize: 19
          maxConnections: 20
          addr: ":2121"
          cors:
            allowOrigins: []
          timeout: 22s
          disconnectAfter: 23m
          auth: {}
          timeToRetry: 24s
          timeToClaim: 25m
metricsAddr: "localhost:8080"
logLevel: "warn"`,
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "https://resonatehq.io:9000",
					CoroutineMaxSize:      1,
					SubmissionBatchSize:   2,
					CompletionBatchSize:   3,
					PromiseBatchSize:      4,
					PromiseMaxIterations:  40,
					ScheduleBatchSize:     5,
					ScheduleMaxIterations: 50,
					TaskBatchSize:         6,
					TaskMaxIterations:     60,
					SignalTimeout:         7 * time.Second,
				},
				API: withAPISubsystems(
					config.API{
						Size: 1,
						Auth: config.Auth{
							PublicKey: "key1.pub",
						},
					}, []plugin[config.APISubsystem]{
						{
							name:    "http",
							enabled: true,
							config: &http.Config{
								Addr: ":9998",
								Auth: map[string]string{
									"foo": "bar",
									"baz": "qux",
								},
								Cors: http.Cors{
									AllowOrigins: []string{
										"https://example1.com",
										"https://example2.com",
									},
								},
								Timeout:       15 * time.Second,
								TaskFrequency: 2 * time.Minute,
							},
						},
						{
							name:    "grpc",
							enabled: false,
							config: &grpc.Config{
								Addr: ":9999",
							},
						},
					},
				),
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{
						Size: 2,
					},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "store-sqlite",
							enabled: true,
							config: &sqlite.Config{
								Size:      3,
								BatchSize: 4,
								Path:      "alt.db",
								TxTimeout: 5 * time.Second,
								Reset:     true,
							},
						},
						{
							name:    "store-postgres",
							enabled: false,
							config: &postgres.Config{
								Size:      6,
								BatchSize: 7,
								Workers:   8,
								Host:      "postgres.local",
								Port:      "9999",
								Database:  "alt",
								Query:     map[string]string{"sslmode": "enable"},
								TxTimeout: 11 * time.Second,
								Reset:     true,
							},
						},
					},
					[]plugin[config.AIOPlugin]{
						{
							name:    "http",
							enabled: true,
							config: &httpPlugin.Config{
								Size:        12,
								Workers:     13,
								Timeout:     13 * time.Second,
								ConnTimeout: 15 * time.Second,
								TimeToRetry: 16 * time.Second,
								TimeToClaim: 17 * time.Minute,
							},
						},
						{
							name:    "poll",
							enabled: false,
							config: &poll.Config{
								Size:            18,
								BufferSize:      19,
								MaxConnections:  20,
								Addr:            ":2121",
								Cors:            poll.Cors{AllowOrigins: []string{}},
								Timeout:         22 * time.Second,
								DisconnectAfter: 23 * time.Minute,
								Auth:            map[string]string{},
								TimeToRetry:     24 * time.Second,
								TimeToClaim:     25 * time.Minute,
							},
						},
					},
				),
				MetricsAddr: "localhost:8080",
				LogLevel:    "warn",
			},
		},
		{
			name: "config flags take precedence with plugins",
			cmds: []cmd{serveCmd, devCmd, dstCmd},
			file: `
system:
  url: https://resonatehq.io:9000
  coroutineMaxSize: 1
  submissionBatchSize: 2
  completionBatchSize: 3
  promiseBatchSize: 4
  promiseMaxIterations: 40
  scheduleBatchSize: 5
  scheduleMaxIterations: 50
  taskBatchSize: 6
  taskMaxIterations: 60
  signalTimeout: 7s
api:
  size: 1
  auth:
    publicKey: "key1.pub"
  subsystems:
    http:
      enabled: true
      config:
        addr: ":9998"
        auth:
          foo: bar
          baz: qux
        cors:
          allowOrigins:
          - "https://example1.com"
          - "https://example2.com"
        timeout: 15s
        taskFrequency: 2m
    grpc:
      enabled: false
      config:
        addr: ":9999"
aio:
  size: 2
  subsystems:
    store:
      sqlite:
        enabled: true
        config:
          size: 3
          batchSize: 4
          path: "alt.db"
          txTimeout: 5s
          reset: true
      postgres:
        enabled: false
        config:
          size: 6
          batchSize: 7
          workers: 8
          host: "postgres.local"
          port: "9999"
          database: "alt"
          query:
            sslmode: "enable"
          txTimeout: 11s
          reset: true
  plugins:
    http:
      enabled: true
      config:
        size: 12
        workers: 13
        timeout: 13s
        connTimeout: 15s
        timeToRetry: 16s
        timeToClaim: 17m
    poll:
        enabled: false
        config:
          size: 18
          bufferSize: 19
          maxConnections: 20
          addr: ":2121"
          cors:
            allowOrigins: []
          timeout: 22s
          disconnectAfter: 23m
          auth: {}
          timeToRetry: 24s
          timeToClaim: 25m
metricsAddr: "localhost:8080"
logLevel: "warn"`,
			args: []string{
				"--api-http-enable=false",
				"--api-http-auth", "x=y",
				"--api-http-timeout", "1h",
				"--api-grpc-enable=true",
				"--aio-store-sqlite-enable=false",
				"--aio-store-sqlite-batch-size", "10",
				"--aio-store-sqlite-tx-timeout", "20s",
				"--aio-store-postgres-enable=true",
				"--aio-store-postgres-batch-size", "10",
				"--aio-store-postgres-host", "postgres.remote",
				"--aio-store-postgres-database", "postgres",
				"--aio-store-postgres-tx-timeout", "11h",
				"--aio-http-enable=false",
				"--aio-http-size", "100",
				"--aio-http-ttr", "1m",
				"--aio-http-ttc", "2m",
				"--aio-poll-enable=true",
				"--aio-poll-size", "1000",
				"--aio-poll-ttr", "1h",
				"--aio-poll-ttc", "2h",
			},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                   "https://resonatehq.io:9000",
					CoroutineMaxSize:      1,
					SubmissionBatchSize:   2,
					CompletionBatchSize:   3,
					PromiseBatchSize:      4,
					PromiseMaxIterations:  40,
					ScheduleBatchSize:     5,
					ScheduleMaxIterations: 50,
					TaskBatchSize:         6,
					TaskMaxIterations:     60,
					SignalTimeout:         7 * time.Second,
				},
				API: withAPISubsystems(
					config.API{
						Size: 1,
						Auth: config.Auth{
							PublicKey: "key1.pub",
						},
					}, []plugin[config.APISubsystem]{
						{
							name:    "http",
							enabled: false,
							config: &http.Config{
								Addr: ":9998",
								Auth: map[string]string{
									"x": "y",
								},
								Cors: http.Cors{
									AllowOrigins: []string{
										"https://example1.com",
										"https://example2.com",
									},
								},
								Timeout:       1 * time.Hour,
								TaskFrequency: 2 * time.Minute,
							},
						},
						{
							name:    "grpc",
							enabled: true,
							config: &grpc.Config{
								Addr: ":9999",
							},
						},
					},
				),
				AIO: withAIOSubsystemsAndPlugins(
					config.AIO{
						Size: 2,
					},
					[]plugin[config.AIOSubsystem]{
						{
							name:    "store-sqlite",
							enabled: false,
							config: &sqlite.Config{
								Size:      3,
								BatchSize: 10,
								Path:      "alt.db",
								TxTimeout: 20 * time.Second,
								Reset:     true,
							},
						},
						{
							name:    "store-postgres",
							enabled: true,
							config: &postgres.Config{
								Size:      6,
								BatchSize: 10,
								Workers:   8,
								Host:      "postgres.remote",
								Port:      "9999",
								Database:  "postgres",
								Query:     map[string]string{"sslmode": "enable"},
								TxTimeout: 11 * time.Hour,
								Reset:     true,
							},
						},
					},
					[]plugin[config.AIOPlugin]{
						{
							name:    "http",
							enabled: false,
							config: &httpPlugin.Config{
								Size:        100,
								Workers:     13,
								Timeout:     13 * time.Second,
								ConnTimeout: 15 * time.Second,
								TimeToRetry: 1 * time.Minute,
								TimeToClaim: 2 * time.Minute,
							},
						},
						{
							name:    "poll",
							enabled: true,
							config: &poll.Config{
								Size:            1000,
								BufferSize:      19,
								MaxConnections:  20,
								Addr:            ":2121",
								Cors:            poll.Cors{AllowOrigins: []string{}},
								Timeout:         22 * time.Second,
								DisconnectAfter: 23 * time.Minute,
								Auth:            map[string]string{},
								TimeToRetry:     1 * time.Hour,
								TimeToClaim:     2 * time.Hour,
							},
						},
					},
				),
				MetricsAddr: "localhost:8080",
				LogLevel:    "warn",
			},
		},
	}

	hooks := mapstructure.ComposeDecodeHookFunc(
		util.StringToRange(rand.New(rand.NewSource(0))),
		util.MapToBytes(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, c := range tt.cmds {
				cfg := &config.Config{}
				if tt.plugins != nil {
					tt.plugins(cfg)
				}

				vip := viper.New()
				cmd := c.cmd(cfg, vip)

				t.Run(cmd.Name(), func(t *testing.T) {
					flags := cmd.Flags()
					if c.persistent {
						flags = cmd.PersistentFlags()
					}

					prerun := cmd.PreRunE
					if c.persistent {
						prerun = cmd.PersistentPreRunE
					}

					// set up config file
					configFile := filepath.Join(t.TempDir(), "resonate.yaml")
					err := os.WriteFile(configFile, []byte(tt.file), 0644)
					require.NoError(t, err)

					// wire up config file
					err = flags.Set("config", configFile)
					require.NoError(t, err)

					// call command with flags
					err = cmd.ParseFlags(tt.args)
					require.NoError(t, err)

					// run pre-run to load config
					err = prerun(cmd, []string{})
					require.NoError(t, err)

					// decode config
					err = vip.Unmarshal(&cfg, viper.DecodeHook(hooks))
					require.NoError(t, err)

					// decode plugins
					for _, plugin := range cfg.Plugins() {
						err := plugin.Decode(vip, hooks)
						require.NoError(t, err)
					}

					assert.Equal(t, tt.expected, cfg)
				})
			}
		})
	}
}

type plugin[T any] struct {
	name    string
	enabled bool
	config  T
}

func withAPISubsystems(api config.API, subsystems []plugin[config.APISubsystem]) config.API {
	for _, s := range subsystems {
		api.Subsystems.Add(s.name, s.enabled, s.config)
	}
	return api
}

func withAIOSubsystemsAndPlugins(aio config.AIO, subsystems []plugin[config.AIOSubsystem], plugins []plugin[config.AIOPlugin]) config.AIO {
	for _, s := range subsystems {
		aio.Subsystems.Add(s.name, s.enabled, s.config)
	}
	for _, p := range plugins {
		aio.Plugins.Add(p.name, p.enabled, p.config)
	}
	return aio
}
