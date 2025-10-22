package migrations

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var ErrPendingMigrations = errors.New("pending migrations exist")

// Migration represents a single database migration
type Migration struct {
	Version int
	Name    string
	SQL     string
}

func ParseMigrationFilename(filename string) (version int, name string, err error) {
	re := regexp.MustCompile(`^(\d{3})_(.+)\.sql$`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) != 3 {
		return 0, "", fmt.Errorf("invalid migration filename format: %s", filename)
	}

	version, err = strconv.Atoi(matches[1])
	if err != nil {
		return 0, "", fmt.Errorf("invalid version number in filename: %s", filename)
	}

	return version, matches[2], nil
}

func LoadMigrations(store MigrationStore) ([]Migration, error) {
	files, err := store.GetMigrationFiles()
	if err != nil {
		return nil, err
	}

	migrations := make([]Migration, 0, len(files))
	for _, file := range files {
		filename := filepath.Base(file)
		version, name, err := ParseMigrationFilename(filename)
		if err != nil {
			return nil, err
		}

		content, err := store.GetMigrationContent(file)
		if err != nil {
			return nil, fmt.Errorf("failed to read migration %s: %w", file, err)
		}

		migrations = append(migrations, Migration{
			Version: version,
			Name:    name,
			SQL:     content,
		})
	}

	// Sort by version
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// GetPendingMigrations returns migrations that need to be applied
func GetPendingMigrations(currentVersion int, store MigrationStore) ([]Migration, error) {
	allMigrations, err := LoadMigrations(store)
	if err != nil {
		return nil, err
	}

	pending := make([]Migration, 0)
	for _, m := range allMigrations {
		if m.Version > currentVersion {
			pending = append(pending, m)
		}
	}

	return pending, nil
}

// ValidateMigrationSequence ensures migrations are sequential with no gaps
func ValidateMigrationSequence(migrations []Migration, startVersion int) error {
	expectedVersion := startVersion + 1
	for _, m := range migrations {
		if m.Version != expectedVersion {
			return fmt.Errorf("migration sequence gap: expected version %d, found %d",
				expectedVersion, m.Version)
		}
		expectedVersion++
	}
	return nil
}

// ApplyMigrations executes migrations in a transaction
func ApplyMigrations(migrations []Migration, store MigrationStore, verbose bool) error {
	if len(migrations) == 0 {
		return nil
	}

	// Get DB from store
	db := store.GetDB()

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	for _, migration := range migrations {
		if verbose {
			fmt.Printf("Applying migration %03d_%s.sql... ", migration.Version, migration.Name)
		}

		// Execute migration SQL
		_, err := tx.Exec(migration.SQL)
		if err != nil {
			if verbose {
				fmt.Println("✗")
			}
			return &MigrationError{
				Version: migration.Version,
				Name:    migration.Name,
				Err:     err,
			}
		}

		// Update migrations table using store-specific SQL
		updateSQL := store.GetInsertMigrationSQL()
		_, err = tx.Exec(updateSQL, migration.Version)
		if err != nil {
			if verbose {
				fmt.Println("✗")
			}
			return fmt.Errorf("failed to update migrations table: %w", err)
		}

		if verbose {
			fmt.Println("✓")
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// MigrationError represents an error during migration execution
type MigrationError struct {
	Version int
	Name    string
	Err     error
}

func (e *MigrationError) Error() string {
	return fmt.Sprintf("Migration %03d_%s failed: %v\n", e.Version, e.Name, e.Err)
}

func (e *MigrationError) Unwrap() error {
	return e.Err
}
