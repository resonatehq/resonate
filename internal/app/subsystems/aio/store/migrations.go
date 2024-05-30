package store

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
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

func Start(currVersion int, db *sql.DB, txTimeout time.Duration, migrationsFS embed.FS, plan Plan) error {
	dbVersion, err := readVersion(db)
	if err != nil {
		return err
	}

	if currVersion < dbVersion {
		return fmt.Errorf("current version %d is less than database version %d please updated to latest resonate release", currVersion, dbVersion)

	}
	if currVersion == dbVersion {
		return nil
	}

	// If the database version is -1, it means the migrations table does not exist.
	if dbVersion == -1 { // versioning with version.go
		plan = Apply
	}

	switch plan {
	case Default:
		return fmt.Errorf("database version %d does not match current version %d please run `resonate migrate --plan` to see migrations needed", dbVersion, currVersion)
	case DryRun:
		plan, err := generateMigrationPlan(migrationsFS, dbVersion)
		if err != nil {
			return err
		}
		fmt.Println("Migrations to apply:")
		fmt.Printf("Migrations to apply: %v", plan)
	case Apply:
		plan, err := generateMigrationPlan(migrationsFS, dbVersion)
		if err != nil {
			return err
		}
		return applyMigrationPlan(db, plan, txTimeout)
	default:
		return fmt.Errorf("invalid plan: %v", plan)
	}

	return nil
}

// db.QueryRow does not return a specific error type when the table does not exist so we need to check the error message.
func isTableNotFoundError(err error) bool {
	errStr := err.Error()
	if strings.Contains(errStr, "no such table") || strings.Contains(errStr, "does not exist") {
		return true
	}
	return false
}

// readVersion reads the current schema version from the database.
func readVersion(db *sql.DB) (int, error) {
	var version int
	err := db.QueryRow("SELECT id FROM migrations").Scan(&version)
	if err != nil {
		if isTableNotFoundError(err) {
			return -1, nil
		}
		return 0, err
	}
	return version, nil
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

// generateMigrationPlan reads the migrations from the filesystem and returns a plan of migrations to apply.
func generateMigrationPlan(migrationsFS embed.FS, currentVersion int) (MigrationPlan, error) {
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
func applyMigrationPlan(db *sql.DB, plan MigrationPlan, txTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = fmt.Errorf("tx failed: %v, unable to rollback: %v", err, rbErr)
			}
		}

		err = fmt.Errorf("tx failed, performed a rollback: %v", err)
	}()

	for _, m := range plan {
		_, err = tx.Exec(string(m.Content))
		if err != nil {
			return fmt.Errorf("failed to execute migration version %d: %w", m.Version, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}
