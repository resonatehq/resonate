package migrations

import (
	"database/sql"
	"fmt"
)

// CheckPostgresMigrations checks if there are pending migrations for PostgreSQL
func CheckPostgresMigrations(db *sql.DB) error {
	currentVersion, err := GetCurrentVersion(db)
	if err != nil {
		return fmt.Errorf("failed to check migration version: %w", err)
	}

	pending, err := GetPendingMigrations(currentVersion, "postgres")
	if err != nil {
		return fmt.Errorf("failed to get pending migrations: %w", err)
	}

	if len(pending) > 0 {
		return fmt.Errorf(
			"database migrations pending. Current version: %d, Latest version: %d.\n"+
				"Please run 'resonate migrate up --store postgres' before starting",
			currentVersion, pending[len(pending)-1].Version)
	}

	return nil
}
