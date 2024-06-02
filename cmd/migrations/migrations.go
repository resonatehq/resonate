package migrations

import (
	"time"

	"github.com/resonatehq/resonate/cmd/util"
	"github.com/resonatehq/resonate/internal/aio"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/postgres"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/sqlite"
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	var (
		aioStore                  string
		aioStoreSqlitePath        string
		aioStoreSqliteTxTimeout   time.Duration
		aioStorePostgresHost      string
		aioStorePostgresPort      string
		aioStorePostgresUsername  string
		aioStorePostgresPassword  string
		aioStorePostgresDatabase  string
		aioStorePostgresQuery     map[string]string
		aioStorePostgresTxTimeout time.Duration
		plan                      bool
	)

	var cmd = &cobra.Command{
		Use:   "migrate",
		Short: "Synchronizes the database state with the current set of models and migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			desiredPlan := migrations.Apply
			if plan {
				desiredPlan = migrations.DryRun
			}

			cfg := &util.AIOSubsystemConfig[util.StoreConfig]{
				Subsystem: &aio.SubsystemConfig{
					Size:      1,
					BatchSize: 1,
					Workers:   1, // Only one connection will be opened to run migrations.
				},
				Config: &util.StoreConfig{
					Kind: util.StoreKind(aioStore),
					Sqlite: &sqlite.Config{
						Path:      aioStoreSqlitePath,
						TxTimeout: aioStoreSqliteTxTimeout,
						Plan:      desiredPlan,
					},
					Postgres: &postgres.Config{
						Host:      aioStorePostgresHost,
						Port:      aioStorePostgresPort,
						Username:  aioStorePostgresUsername,
						Password:  aioStorePostgresPassword,
						Database:  aioStorePostgresDatabase,
						Query:     aioStorePostgresQuery,
						TxTimeout: aioStorePostgresTxTimeout,
						Plan:      desiredPlan,
					},
				},
			}

			store, err := util.NewStore(cfg)
			if err != nil {
				return err
			}

			// Runs migrations.
			if err := store.Start(); err != nil {
				return err
			}

			return store.Stop()
		},
	}

	cmd.Flags().StringVar(&aioStore, "aio-store", "sqlite", "promise store type")
	cmd.Flags().StringVar(&aioStoreSqlitePath, "aio-store-sqlite-path", "resonate.db", "sqlite database path")
	cmd.Flags().DurationVar(&aioStoreSqliteTxTimeout, "aio-store-sqlite-tx-timeout", 10_000*time.Millisecond, "sqlite transaction timeout")
	cmd.Flags().StringVar(&aioStorePostgresHost, "aio-store-postgres-host", "localhost", "postgres host")
	cmd.Flags().StringVar(&aioStorePostgresPort, "aio-store-postgres-port", "5432", "postgres port")
	cmd.Flags().StringVar(&aioStorePostgresUsername, "aio-store-postgres-username", "", "postgres username")
	cmd.Flags().StringVar(&aioStorePostgresPassword, "aio-store-postgres-password", "", "postgres password")
	cmd.Flags().StringVar(&aioStorePostgresDatabase, "aio-store-postgres-database", "resonate", "postgres database name")
	cmd.Flags().StringToStringVar(&aioStorePostgresQuery, "aio-store-postgres-query", make(map[string]string, 0), "postgres query options")
	cmd.Flags().DurationVar(&aioStorePostgresTxTimeout, "aio-store-postgres-tx-timeout", 10_000*time.Millisecond, "postgres transaction timeout")
	cmd.Flags().BoolVarP(&plan, "plan", "p", false, "Dry run to check which migrations will be applied")

	return cmd
}
