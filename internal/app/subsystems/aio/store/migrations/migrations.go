package migrations

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// Migration represents a single database migration
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// GetCurrentVersion reads the current migration version from the database
func GetCurrentVersion(db *sql.DB) (int, error) {
	var version int
	err := db.QueryRow("SELECT id FROM migrations ORDER BY id DESC LIMIT 1").Scan(&version)
	if err == sql.ErrNoRows {
		// Empty migrations table, return 0
		return 0, nil
	}
	if err != nil {
		// Check if error is because migrations table doesn't exist
		errStr := err.Error()
		if regexp.MustCompile(`(?i)no such table|does not exist|table.*not found`).MatchString(errStr) {
			// Migrations table doesn't exist yet, return 0 (no migrations applied)
			return 0, nil
		}
		return 0, fmt.Errorf("failed to get current migration version: %w", err)
	}
	return version, nil
}

// ParseMigrationFilename extracts version and name from a migration filename
// Expected format: 002_description.sql
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

// LoadMigrations loads all migrations from embedded files for the specified store type
func LoadMigrations(storeType string) ([]Migration, error) {
	var files []string
	var err error

	switch storeType {
	case "sqlite":
		files, err = GetSQLiteMigrationFiles()
	case "postgres":
		files, err = GetPostgresMigrationFiles()
	default:
		return nil, fmt.Errorf("unsupported store type: %s", storeType)
	}

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

		var content string
		switch storeType {
		case "sqlite":
			content, err = GetSQLiteMigrationContent(file)
		case "postgres":
			content, err = GetPostgresMigrationContent(file)
		}

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
func GetPendingMigrations(currentVersion int, storeType string) ([]Migration, error) {
	allMigrations, err := LoadMigrations(storeType)
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
func ApplyMigrations(db *sql.DB, migrations []Migration, storeType string) error {
	if len(migrations) == 0 {
		return nil
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed

	for _, migration := range migrations {
		fmt.Printf("Applying migration %03d_%s.sql... ", migration.Version, migration.Name)

		// Execute migration SQL
		_, err := tx.Exec(migration.SQL)
		if err != nil {
			fmt.Println("✗")
			return &MigrationError{
				Version: migration.Version,
				Name:    migration.Name,
				Err:     err,
			}
		}

		// Update migrations table
		var updateSQL string
		if storeType == "sqlite" {
			updateSQL = "INSERT INTO migrations (id) VALUES (?) ON CONFLICT(id) DO NOTHING"
		} else {
			updateSQL = "INSERT INTO migrations (id) VALUES ($1) ON CONFLICT(id) DO NOTHING"
		}

		_, err = tx.Exec(updateSQL, migration.Version)
		if err != nil {
			fmt.Println("✗")
			return fmt.Errorf("failed to update migrations table: %w", err)
		}

		fmt.Println("✓")
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
	suggestions := suggestFixes(e.Err)
	return fmt.Sprintf("Migration %03d_%s failed: %v\n\n%s",
		e.Version, e.Name, e.Err, suggestions)
}

func (e *MigrationError) Unwrap() error {
	return e.Err
}

// suggestFixes provides helpful suggestions based on the error
func suggestFixes(err error) string {
	errStr := err.Error()

	suggestions := "Suggestions:\n"

	if regexp.MustCompile(`(?i)permission|access denied`).MatchString(errStr) {
		suggestions += "- Check database user permissions\n"
		suggestions += "- Verify the database user has DDL privileges\n"
	} else if regexp.MustCompile(`(?i)syntax error`).MatchString(errStr) {
		suggestions += "- Review the SQL syntax in the migration file\n"
		suggestions += "- Ensure the SQL is compatible with the target database\n"
	} else if regexp.MustCompile(`(?i)constraint|violation|foreign key`).MatchString(errStr) {
		suggestions += "- Check if existing data violates the new constraints\n"
		suggestions += "- Consider adding a data migration step before the constraint\n"
	} else if regexp.MustCompile(`(?i)already exists|duplicate`).MatchString(errStr) {
		suggestions += "- The object may already exist from a previous migration\n"
		suggestions += "- Use IF NOT EXISTS clauses where appropriate\n"
	} else {
		suggestions += "- Review the migration SQL\n"
		suggestions += "- Check database logs for more details\n"
	}

	return suggestions
}
