// Shared package for handling database migrations.
package migrations

import (
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Plan int

const (
	Default Plan = iota // 0 is the default value
	DryRun
	Apply
)

type Migration struct {
	Version int
	Content []byte
}

type MigrationPlan []Migration

func (m MigrationPlan) String() string {
	var sb strings.Builder
	sb.WriteString("Operations to perform:\n")
	sb.WriteString("Apply all migrations:")
	for _, migration := range m {
		sb.WriteString(fmt.Sprintf(" %d", migration.Version))
	}
	return sb.String()
}

// readVersion reads the current schema version from the database.
func ReadVersion(tx *sql.Tx) (int, error) {
	var version int
	err := tx.QueryRow("SELECT id FROM migrations").Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			return -1, nil
		}
		return 0, err
	}
	return version, nil
}

// generateMigrationPlan reads the migrations from the filesystem and returns a plan of migrations to apply.
func GenerateMigrationPlan(migrationsFS embed.FS, currentVersion int) (MigrationPlan, error) {
	migrations := []Migration{}
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		filename := entry.Name()
		content, err := migrationsFS.ReadFile("migrations/" + filename)
		if err != nil {
			return nil, err
		}
		version, err := migrationVersion(filename)
		if err != nil {
			return nil, err
		}
		// Skip migrations that are at or below the current version.
		if version <= currentVersion {
			continue
		}
		migrations = append(migrations, Migration{
			Version: version,
			Content: content,
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})

	return migrations, nil
}

// applyMigrationPlan applies the migrations to the database as a single transaction.
func ApplyMigrationPlan(tx *sql.Tx, plan MigrationPlan) error {
	for _, m := range plan {
		_, err := tx.Exec(string(m.Content))
		if err != nil {
			return fmt.Errorf("failed to execute migration version %d: %w", m.Version, err)
		}
	}
	return nil
}

// filenameVersion extracts the version number from a migration filename.
func migrationVersion(filename string) (int, error) {
	re := regexp.MustCompile(`\d+`)
	versionStr := re.FindString(filepath.Base(filename))
	if versionStr == "" {
		return 0, fmt.Errorf("could not extract version from filename: %s", filename)
	}
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, fmt.Errorf("could not convert version string to int: %v", err)
	}
	return version, nil
}
