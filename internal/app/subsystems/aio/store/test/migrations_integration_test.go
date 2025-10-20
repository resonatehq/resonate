package test

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/resonatehq/resonate/internal/app/subsystems/aio/store/migrations"
)

func TestSQLiteMigrationLifecycle(t *testing.T) {
	// Create in-memory SQLite database
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory SQLite: %v", err)
	}
	defer db.Close()

	// Test 1: Database without migrations table should return an error
	t.Run("database without migrations table returns error", func(t *testing.T) {
		store := migrations.NewSqliteMigrationStore(db)
		_, err := store.GetCurrentVersion()
		if err == nil {
			t.Fatal("GetCurrentVersion() should have failed when migrations table doesn't exist")
		}
	})

	// Test 2: Initialize schema with migrations table at version 1
	t.Run("initialize schema with version 1", func(t *testing.T) {
		_, err := db.Exec("CREATE TABLE migrations (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create migrations table: %v", err)
		}
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Failed to insert initial version: %v", err)
		}

		store := migrations.NewSqliteMigrationStore(db)
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 1 {
			t.Errorf("GetCurrentVersion() = %d, want 1", version)
		}
	})

	// Test 3: Insert additional migration versions
	t.Run("add migration version 2", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO migrations (id) VALUES (2)")
		if err != nil {
			t.Fatalf("Failed to insert migration version: %v", err)
		}

		store := migrations.NewSqliteMigrationStore(db)
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 2 {
			t.Errorf("GetCurrentVersion() = %d, want 2", version)
		}
	})

	// Test 4: Insert multiple versions
	t.Run("add multiple migration versions", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO migrations (id) VALUES (3), (4)")
		if err != nil {
			t.Fatalf("Failed to insert migration versions: %v", err)
		}

		store := migrations.NewSqliteMigrationStore(db)
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 4 {
			t.Errorf("GetCurrentVersion() = %d, want 4", version)
		}
	})

	// Test 5: Insert out of order (should still return highest)
	t.Run("out of order versions", func(t *testing.T) {
		_, err := db.Exec("INSERT INTO migrations (id) VALUES (6), (5)")
		if err != nil {
			t.Fatalf("Failed to insert migration versions: %v", err)
		}

		store := migrations.NewSqliteMigrationStore(db)
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		// Should return the highest version
		if version != 6 {
			t.Errorf("GetCurrentVersion() = %d, want 6", version)
		}
	})
}

func TestApplyMigrations(t *testing.T) {
	// Test applying migrations in a transaction
	t.Run("apply migrations successfully", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to open in-memory SQLite: %v", err)
		}
		defer db.Close()

		// Initialize schema with migrations table at version 1
		_, err = db.Exec("CREATE TABLE migrations (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create migrations table: %v", err)
		}
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Failed to insert initial version: %v", err)
		}

		// Apply test migrations starting from version 2
		testMigrations := []migrations.Migration{
			{
				Version: 2,
				Name:    "create_promises",
				SQL:     "CREATE TABLE promises (id TEXT PRIMARY KEY, state TEXT, timeout INTEGER)",
			},
			{
				Version: 3,
				Name:    "create_schedules",
				SQL:     "CREATE TABLE schedules (id TEXT PRIMARY KEY, description TEXT, cron TEXT, promise_id TEXT)",
			},
		}

		store := migrations.NewSqliteMigrationStore(db)

		err = migrations.ApplyMigrations(testMigrations, store, true)
		if err != nil {
			t.Fatalf("ApplyMigrations() failed: %v", err)
		}

		// Verify version was updated to 3
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 3 {
			t.Errorf("GetCurrentVersion() = %d, want 3", version)
		}

		// Verify tables were created
		var tableCount int
		err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('promises', 'schedules')").Scan(&tableCount)
		if err != nil {
			t.Fatalf("Failed to count tables: %v", err)
		}
		if tableCount != 2 {
			t.Errorf("Expected 2 tables to be created, got %d", tableCount)
		}
	})

	t.Run("apply migration that adds column", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to open in-memory SQLite: %v", err)
		}
		defer db.Close()

		// Initialize schema with migrations table at version 1
		_, err = db.Exec("CREATE TABLE migrations (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create migrations table: %v", err)
		}
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Failed to insert initial version: %v", err)
		}

		// Apply migrations: create table then add column
		testMigrations := []migrations.Migration{
			{
				Version: 2,
				Name:    "create_tasks",
				SQL:     "CREATE TABLE tasks (id TEXT PRIMARY KEY, counter INTEGER)",
			},
			{
				Version: 3,
				Name:    "add_tasks_frequency",
				SQL:     "ALTER TABLE tasks ADD COLUMN frequency TEXT",
			},
		}

		store := migrations.NewSqliteMigrationStore(db)
		err = migrations.ApplyMigrations(testMigrations, store, true)
		if err != nil {
			t.Fatalf("ApplyMigrations() failed: %v", err)
		}

		// Verify version was updated to 3
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 3 {
			t.Errorf("GetCurrentVersion() = %d, want 3", version)
		}

		// Verify the column was added by checking table schema
		var columnCount int
		err = db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name IN ('id', 'counter', 'frequency')").Scan(&columnCount)
		if err != nil {
			t.Fatalf("Failed to count columns: %v", err)
		}
		if columnCount != 3 {
			t.Errorf("Expected 3 columns in tasks table, got %d", columnCount)
		}

		// Verify we can insert data with the new column
		_, err = db.Exec("INSERT INTO tasks (id, counter, frequency) VALUES ('task1', 5, 'daily')")
		if err != nil {
			t.Fatalf("Failed to insert data with new column: %v", err)
		}
	})

	t.Run("rollback on error", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to open in-memory SQLite: %v", err)
		}
		defer db.Close()

		// Initialize schema with migrations table at version 1
		_, err = db.Exec("CREATE TABLE migrations (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create migrations table: %v", err)
		}
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Failed to insert initial version: %v", err)
		}

		// Apply migrations where the second one fails
		testMigrations := []migrations.Migration{
			{
				Version: 2,
				Name:    "create_promises",
				SQL:     "CREATE TABLE promises (id TEXT PRIMARY KEY, state TEXT)",
			},
			{
				Version: 3,
				Name:    "bad_migration",
				SQL:     "THIS IS INVALID SQL",
			},
		}

		store := migrations.NewSqliteMigrationStore(db)
		err = migrations.ApplyMigrations(testMigrations, store, true)
		if err == nil {
			t.Fatal("ApplyMigrations() should have failed but succeeded")
		}

		// Verify rollback - version should still be 1
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 1 {
			t.Errorf("GetCurrentVersion() = %d, want 1 (rolled back)", version)
		}

		// Verify promises table was NOT created (rolled back)
		var tableCount int
		err = db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='promises'").Scan(&tableCount)
		if err != nil {
			t.Fatalf("Failed to count tables: %v", err)
		}
		if tableCount != 0 {
			t.Errorf("Expected promises table to be rolled back, but it exists")
		}
	})

	t.Run("apply empty migrations list", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to open in-memory SQLite: %v", err)
		}
		defer db.Close()

		// Initialize schema with migrations table at version 1
		_, err = db.Exec("CREATE TABLE migrations (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create migrations table: %v", err)
		}
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Failed to insert initial version: %v", err)
		}

		store := migrations.NewSqliteMigrationStore(db)

		// Apply empty migrations - should be no-op
		err = migrations.ApplyMigrations([]migrations.Migration{}, store, true)
		if err != nil {
			t.Fatalf("ApplyMigrations() failed with empty list: %v", err)
		}

		// Version should still be 1
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 1 {
			t.Errorf("GetCurrentVersion() = %d, want 1", version)
		}
	})

	t.Run("duplicate migration version uses ON CONFLICT", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to open in-memory SQLite: %v", err)
		}
		defer db.Close()

		// Initialize schema with migrations table at version 1
		_, err = db.Exec("CREATE TABLE migrations (id INTEGER PRIMARY KEY)")
		if err != nil {
			t.Fatalf("Failed to create migrations table: %v", err)
		}
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (1)")
		if err != nil {
			t.Fatalf("Failed to insert initial version: %v", err)
		}

		// First application
		testMigrations := []migrations.Migration{
			{
				Version: 2,
				Name:    "create_locks",
				SQL:     "CREATE TABLE locks (resource_id TEXT PRIMARY KEY, process_id TEXT)",
			},
		}

		store := migrations.NewSqliteMigrationStore(db)
		err = migrations.ApplyMigrations(testMigrations, store, true)
		if err != nil {
			t.Fatalf("ApplyMigrations() failed on first run: %v", err)
		}

		// Manually insert the same version again to test ON CONFLICT
		_, err = db.Exec("INSERT INTO migrations (id) VALUES (2) ON CONFLICT(id) DO NOTHING")
		if err != nil {
			t.Fatalf("Failed to test ON CONFLICT: %v", err)
		}

		// Should still be at version 2
		version, err := store.GetCurrentVersion()
		if err != nil {
			t.Fatalf("GetCurrentVersion() failed: %v", err)
		}
		if version != 2 {
			t.Errorf("GetCurrentVersion() = %d, want 2", version)
		}

		// Verify only one entry for version 2
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM migrations WHERE id = 2").Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count version 2 entries: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 entry for version 2, got %d", count)
		}
	})
}

func TestGetPendingMigrations(t *testing.T) {
	tests := []struct {
		name           string
		currentVersion int
		wantCount      int
	}{
		{
			name:           "pending migrations from version 0",
			currentVersion: 0,
			wantCount:      1, // Should have 001_initial_schema
		},
		{
			name:           "pending migrations from version 1",
			currentVersion: 1,
			wantCount:      0, // All migrations applied
		},
		{
			name:           "no pending from very high version",
			currentVersion: 999,
			wantCount:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create in-memory SQLite database for testing
			db, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("Failed to open in-memory SQLite: %v", err)
			}
			defer db.Close()

			store := migrations.NewSqliteMigrationStore(db)
			pending, err := migrations.GetPendingMigrations(tt.currentVersion, store)
			if err != nil {
				t.Fatalf("GetPendingMigrations() failed: %v", err)
			}

			if tt.wantCount >= 0 && len(pending) != tt.wantCount {
				t.Errorf("GetPendingMigrations() returned %d migrations, want %d", len(pending), tt.wantCount)
			}

			// Verify all pending migrations have version > currentVersion
			for _, m := range pending {
				if m.Version <= tt.currentVersion {
					t.Errorf("Pending migration has version %d, which is <= current version %d", m.Version, tt.currentVersion)
				}
			}
		})
	}
}
