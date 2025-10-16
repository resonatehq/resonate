package migrations

import "database/sql"

// CheckPostgresMigrations checks if there are pending migrations for PostgreSQL
func CheckPostgresMigrations(db *sql.DB) error {
	store := NewPostgresMigrationStore(db)
	return store.CheckMigrations()
}
