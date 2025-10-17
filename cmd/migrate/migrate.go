package migrate

import (
	"database/sql"
	"fmt"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var storeType string

func NewCmd() *cobra.Command {
	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Database migration commands",
		Long:  "Manage database migrations for Resonate",
	}

	// Add --store flag
	migrateCmd.PersistentFlags().StringVar(&storeType, "store", "sqlite", "store type (sqlite or postgres)")

	// Add SQLite flags
	migrateCmd.PersistentFlags().String("aio-store-sqlite-path", "resonate.db", "sqlite database path")
	viper.BindPFlag("aio.store.sqlite.path", migrateCmd.PersistentFlags().Lookup("aio-store-sqlite-path"))

	// Add PostgreSQL flags
	migrateCmd.PersistentFlags().String("aio-store-postgres-host", "localhost", "postgres host")
	migrateCmd.PersistentFlags().String("aio-store-postgres-port", "5432", "postgres port")
	migrateCmd.PersistentFlags().String("aio-store-postgres-username", "", "postgres username")
	migrateCmd.PersistentFlags().String("aio-store-postgres-password", "", "postgres password")
	migrateCmd.PersistentFlags().String("aio-store-postgres-database", "resonate", "postgres database")
	migrateCmd.PersistentFlags().String("aio-store-postgres-sslmode", "disable", "postgres SSL mode (disable, require, verify-ca, verify-full)")
	viper.BindPFlag("aio.store.postgres.host", migrateCmd.PersistentFlags().Lookup("aio-store-postgres-host"))
	viper.BindPFlag("aio.store.postgres.port", migrateCmd.PersistentFlags().Lookup("aio-store-postgres-port"))
	viper.BindPFlag("aio.store.postgres.username", migrateCmd.PersistentFlags().Lookup("aio-store-postgres-username"))
	viper.BindPFlag("aio.store.postgres.password", migrateCmd.PersistentFlags().Lookup("aio-store-postgres-password"))
	viper.BindPFlag("aio.store.postgres.database", migrateCmd.PersistentFlags().Lookup("aio-store-postgres-database"))
	viper.BindPFlag("aio.store.postgres.sslmode", migrateCmd.PersistentFlags().Lookup("aio-store-postgres-sslmode"))

	// Add subcommands
	migrateCmd.AddCommand(newStatusCmd())
	migrateCmd.AddCommand(newDryRunCmd())
	migrateCmd.AddCommand(newUpCmd())

	return migrateCmd
}

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show current migration status",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB()
			if err != nil {
				return err
			}
			defer db.Close()

			store, err := getMigrationStore(db)
			if err != nil {
				return err
			}

			currentVersion, err := store.GetCurrentVersion()
			if err != nil {
				return fmt.Errorf("failed to get current version: %w", err)
			}

			allMigrations, err := migrations.LoadMigrations(store)
			if err != nil {
				return fmt.Errorf("failed to load migrations: %w", err)
			}

			var latestVersion int
			if len(allMigrations) > 0 {
				latestVersion = allMigrations[len(allMigrations)-1].Version
			} else {
				latestVersion = currentVersion
			}

			pending, err := migrations.GetPendingMigrations(currentVersion, store)
			if err != nil {
				return fmt.Errorf("failed to get pending migrations: %w", err)
			}

			fmt.Printf("Store type: %s\n", storeType)
			fmt.Printf("Current migration version: %d\n", currentVersion)
			fmt.Printf("Latest migration version: %d\n", latestVersion)
			fmt.Printf("Pending migrations: %d\n", len(pending))

			if len(pending) > 0 {
				fmt.Println("\nStatus: MIGRATIONS PENDING")
			} else {
				fmt.Println("\nStatus: UP TO DATE")
			}

			return nil
		},
	}
}

func newDryRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "dry-run",
		Short: "Show which migrations would be applied",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB()
			if err != nil {
				return err
			}
			defer db.Close()

			store, err := getMigrationStore(db)
			if err != nil {
				return err
			}

			currentVersion, err := store.GetCurrentVersion()
			if err != nil {
				return fmt.Errorf("failed to get current version: %w", err)
			}

			pending, err := migrations.GetPendingMigrations(currentVersion, store)
			if err != nil {
				return fmt.Errorf("failed to get pending migrations: %w", err)
			}

			if len(pending) == 0 {
				fmt.Println("No pending migrations")
				return nil
			}

			fmt.Printf("Would apply the following %d migration(s):\n\n", len(pending))
			for _, m := range pending {
				fmt.Printf("  %03d_%s.sql\n", m.Version, m.Name)
			}

			return nil
		},
	}
}

func newUpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "up",
		Short: "Apply all pending migrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			db, err := openDB()
			if err != nil {
				return err
			}
			defer db.Close()

			store, err := getMigrationStore(db)
			if err != nil {
				return err
			}

			currentVersion, err := store.GetCurrentVersion()
			if err != nil {
				return fmt.Errorf("failed to get current version: %w", err)
			}

			pending, err := migrations.GetPendingMigrations(currentVersion, store)
			if err != nil {
				return fmt.Errorf("failed to get pending migrations: %w", err)
			}

			if len(pending) == 0 {
				fmt.Println("No pending migrations")
				return nil
			}

			// Validate migration sequence
			if err := migrations.ValidateMigrationSequence(pending, currentVersion); err != nil {
				return err
			}

			fmt.Printf("Applying %d migration(s)...\n\n", len(pending))

			if err := migrations.ApplyMigrations(pending, store); err != nil {
				return err
			}

			latestVersion := pending[len(pending)-1].Version
			fmt.Printf("\n✓ Successfully applied %d migration(s). Database is now at version %d.\n",
				len(pending), latestVersion)

			return nil
		},
	}
}

// getMigrationStore returns the appropriate migration store based on the store type
func getMigrationStore(db *sql.DB) (migrations.MigrationStore, error) {
	switch storeType {
	case "sqlite":
		return migrations.NewSqliteMigrationStore(db), nil
	case "postgres":
		return migrations.NewPostgresMigrationStore(db), nil
	default:
		return nil, fmt.Errorf("unsupported store type: %s", storeType)
	}
}

// openDB opens a database connection based on the store type and config
func openDB() (*sql.DB, error) {
	switch storeType {
	case "sqlite":
		return openSQLiteDB()
	case "postgres":
		return openPostgresDB()
	default:
		return nil, fmt.Errorf("unsupported store type: %s", storeType)
	}
}

func openSQLiteDB() (*sql.DB, error) {
	path := viper.GetString("aio.store.sqlite.path")
	if path == "" {
		path = "resonate.db"
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	return db, nil
}

func openPostgresDB() (*sql.DB, error) {
	host := viper.GetString("aio.store.postgres.host")
	port := viper.GetString("aio.store.postgres.port")
	user := viper.GetString("aio.store.postgres.username")
	password := viper.GetString("aio.store.postgres.password")
	dbname := viper.GetString("aio.store.postgres.database")
	sslmode := viper.GetString("aio.store.postgres.sslmode")

	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5432"
	}
	if dbname == "" {
		dbname = "resonate"
	}
	if sslmode == "" {
		sslmode = "disable"
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL database: %w", err)
	}

	return db, nil
}
