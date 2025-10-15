package migrations

import "database/sql"

// MigrationStore defines the interface for database-specific migration operations
type MigrationStore interface {
	// GetMigrationFiles returns the list of migration file names for this store type
	GetMigrationFiles() ([]string, error)

	// GetMigrationContent returns the content of a migration file
	GetMigrationContent(path string) (string, error)

	// GetInsertMigrationSQL returns the SQL statement to record a migration
	// The statement should use the appropriate parameter placeholder for the database type
	GetInsertMigrationSQL() string

	// CheckMigrations checks for pending migrations and returns an error if any exist
	CheckMigrations(db *sql.DB) error

	// String returns the name of the store type for logging/debugging
	String() string
}
