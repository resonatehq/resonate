package migrations

import (
	"github.com/resonatehq/resonate/internal/migrationfiles"
)

// GetSQLiteMigrationFiles returns list of SQLite migration files
func GetSQLiteMigrationFiles() ([]string, error) {
	return migrationfiles.GetSQLiteMigrationFiles()
}

// GetPostgresMigrationFiles returns list of Postgres migration files
func GetPostgresMigrationFiles() ([]string, error) {
	return migrationfiles.GetPostgresMigrationFiles()
}

// GetSQLiteMigrationContent reads a SQLite migration file
func GetSQLiteMigrationContent(path string) (string, error) {
	return migrationfiles.GetSQLiteMigrationContent(path)
}

// GetPostgresMigrationContent reads a Postgres migration file
func GetPostgresMigrationContent(path string) (string, error) {
	return migrationfiles.GetPostgresMigrationContent(path)
}
