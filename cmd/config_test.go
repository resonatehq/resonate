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
	"github.com/resonatehq/resonate/internal/app/plugins/sqs"
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

type cmd func(*config.Config, *viper.Viper) *cobra.Command

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		cmds     []cmd
		pers     bool
		file     string
		args     []string
		plugins  func(*config.Config)
		expected *config.Config
	}{
		{
			name: "default serve and dev",
			cmds: []cmd{serve.NewCmd, dev.NewCmd},
			expected: &config.Config{
				System: system.Config{
					Url:                 "",
					CoroutineMaxSize:    1000,
					SubmissionBatchSize: 1000,
					CompletionBatchSize: 1000,
					PromiseBatchSize:    100,
					ScheduleBatchSize:   100,
					TaskBatchSize:       100,
					SignalTimeout:       1 * time.Second,
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
			cmds:     []cmd{migrate.NewCmd},
			pers:     true,
			expected: &config.Config{}, // migrate only uses plugins
		},
		{
			name: "config from file",
			cmds: []cmd{serve.NewCmd, dev.NewCmd, dst.RunDSTCmd},
			file: `
system:
  url: https://resonatehq.io:9000
  coroutineMaxSize: 1
  submissionBatchSize: 2
  completionBatchSize: 3
  promiseBatchSize: 4
  scheduleBatchSize: 5
  taskBatchSize: 6
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
					Url:                 "https://resonatehq.io:9000",
					CoroutineMaxSize:    1,
					SubmissionBatchSize: 2,
					CompletionBatchSize: 3,
					PromiseBatchSize:    4,
					ScheduleBatchSize:   5,
					TaskBatchSize:       6,
					SignalTimeout:       7 * time.Second,
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
			cmds: []cmd{serve.NewCmd, dev.NewCmd, dst.RunDSTCmd},
			args: []string{
				"--system-url", "https://resonatehq.io:9001",
				"--system-coroutine-max-size", "2",
				"--system-submission-batch-size", "3",
				"--system-completion-batch-size", "4",
				"--system-promise-batch-size", "5",
				"--system-schedule-batch-size", "6",
				"--system-task-batch-size", "7",
				"--system-signal-timeout", "8s",
				"--api-size", "2",
				"--api-auth-public-key", "key2.pub",
				"--aio-size", "3",
				"--metrics-addr", "localhost:8081",
				"--log-level", "error",
			},
			expected: &config.Config{
				System: system.Config{
					Url:                 "https://resonatehq.io:9001",
					CoroutineMaxSize:    2,
					SubmissionBatchSize: 3,
					CompletionBatchSize: 4,
					PromiseBatchSize:    5,
					ScheduleBatchSize:   6,
					TaskBatchSize:       7,
					SignalTimeout:       8 * time.Second,
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
			cmds: []cmd{serve.NewCmd, dev.NewCmd, dst.RunDSTCmd},
			file: `
system:
  url: https://resonatehq.io:9000
  coroutineMaxSize: 1
  submissionBatchSize: 2
  completionBatchSize: 3
  promiseBatchSize: 4
  scheduleBatchSize: 5
  taskBatchSize: 6
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
					Url:                 "https://resonatehq.io:9001",
					CoroutineMaxSize:    1,
					SubmissionBatchSize: 3,
					CompletionBatchSize: 3,
					PromiseBatchSize:    5,
					ScheduleBatchSize:   5,
					TaskBatchSize:       7,
					SignalTimeout:       7 * time.Second,
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
			cmds: []cmd{serve.NewCmd},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
				cfg.AIO.Subsystems.Add("router", true, &router.Config{})
				cfg.AIO.Subsystems.Add("sender", true, &sender.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
				cfg.AIO.Plugins.Add("sqs", true, &sqs.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                 "",
					CoroutineMaxSize:    1000,
					SubmissionBatchSize: 1000,
					CompletionBatchSize: 1000,
					PromiseBatchSize:    100,
					ScheduleBatchSize:   100,
					TaskBatchSize:       100,
					SignalTimeout:       1 * time.Second,
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
						{
							name:    "sqs",
							enabled: true,
							config: &sqs.Config{
								Size:        100,
								Workers:     1,
								Timeout:     30 * time.Second,
								TimeToRetry: 15 * time.Second,
								TimeToClaim: 0,
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
			cmds: []cmd{dev.NewCmd},
			plugins: func(cfg *config.Config) {
				cfg.API.Subsystems.Add("http", true, &http.Config{})
				cfg.API.Subsystems.Add("grpc", true, &grpc.Config{})
				cfg.AIO.Subsystems.Add("router", true, &router.Config{})
				cfg.AIO.Subsystems.Add("sender", true, &sender.Config{})
				cfg.AIO.Subsystems.Add("store-sqlite", true, &sqlite.Config{})
				cfg.AIO.Subsystems.Add("store-postgres", false, &postgres.Config{})
				cfg.AIO.Plugins.Add("http", true, &httpPlugin.Config{})
				cfg.AIO.Plugins.Add("poll", true, &poll.Config{})
				cfg.AIO.Plugins.Add("sqs", true, &sqs.Config{})
			},
			expected: &config.Config{
				System: system.Config{
					Url:                 "",
					CoroutineMaxSize:    1000,
					SubmissionBatchSize: 1000,
					CompletionBatchSize: 1000,
					PromiseBatchSize:    100,
					ScheduleBatchSize:   100,
					TaskBatchSize:       100,
					SignalTimeout:       1 * time.Second,
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
						{
							name:    "sqs",
							enabled: true,
							config: &sqs.Config{
								Size:        100,
								Workers:     1,
								Timeout:     30 * time.Second,
								TimeToRetry: 15 * time.Second,
								TimeToClaim: 0,
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
			cmds: []cmd{migrate.NewCmd},
			pers: true,
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
	}

	hooks := mapstructure.ComposeDecodeHookFunc(
		util.StringToRange(rand.New(rand.NewSource(0))),
		util.MapToBytes(),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, cmdFunc := range tt.cmds {
				cfg := &config.Config{}
				if tt.plugins != nil {
					tt.plugins(cfg)
				}

				vip := viper.New()
				cmd := cmdFunc(cfg, vip)

				t.Run(cmd.Name(), func(t *testing.T) {
					flags := cmd.Flags()
					if tt.pers {
						flags = cmd.PersistentFlags()
					}

					prerun := cmd.PreRunE
					if tt.pers {
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
						value, ok := util.Extract(vip.AllSettings(), plugin.Key())
						assert.True(t, ok)
						err := plugin.Config().Decode(value, hooks)
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
