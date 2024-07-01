package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"time"

	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
)

// Version is the Postgres database release version.
const Version = 1

func Run(currVersion int, db *sql.DB, txTimeout time.Duration, migrationsFS embed.FS, plan migrations.Plan) error {
	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	// Acquire a lock to check the database version.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var lockAcquired bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_lock(1)").Scan(&lockAcquired)
	if err != nil {
		return err
	}
	if !lockAcquired {
		err = fmt.Errorf("could not acquire advisory lock")
		return err
	}
	defer func() {
		if _, unlockErr := tx.ExecContext(ctx, "SELECT pg_advisory_unlock(1)"); unlockErr != nil {
			err = fmt.Errorf("%v; %v", err, unlockErr)
		}
	}()

	// Check the database version again while holding the lock
	var dbVersion int
	dbVersion, err = migrations.ReadVersion(tx)
	if err != nil {
		return err
	}

	if currVersion < dbVersion {
		err = fmt.Errorf("current version %d is less than database version %d please updated to latest resonate release", currVersion, dbVersion)
		return err
	}
	if currVersion == dbVersion {
		if err = tx.Commit(); err != nil {
			return err
		}
		return nil
	}

	// If the database version is -1, it means the migrations table does not exist.
	if dbVersion == -1 && plan != migrations.DryRun {
		plan = migrations.Apply
	}

	switch plan {
	case migrations.Default:
		return fmt.Errorf("database version %d does not match current version %d please run `resonate migrate --plan` to see migrations needed", dbVersion, currVersion)
	case migrations.DryRun:
		var plan migrations.MigrationPlan
		plan, err = migrations.GenerateMigrationPlan(migrationsFS, dbVersion)
		if err != nil {
			return err
		}
		fmt.Println("Migrations to apply:")
		fmt.Printf("Migrations to apply: %v", plan)
	case migrations.Apply:
		var plan migrations.MigrationPlan
		plan, err = migrations.GenerateMigrationPlan(migrationsFS, dbVersion)
		if err != nil {
			return err
		}
		if err = migrations.ApplyMigrationPlan(tx, plan); err != nil {
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid plan: %v", plan)
	}

	return nil
}
