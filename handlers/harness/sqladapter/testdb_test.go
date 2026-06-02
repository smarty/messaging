package sqladapter

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// Integration tests in this package require a local MySQL server.
//
// Tests run against a throwaway schema (default `messaging`) which
// is dropped and re-created before each run.

const testSchemaName = "messaging"

func ensureDatabaseReadiness(t *testing.T) {
	bootstrap, err := openDSN(buildDSN(""))
	if err != nil {
		t.Fatal("Database not available (is mysql running?):", err)
	}
	defer func() { _ = bootstrap.Close() }()
	if err := setupSchema(bootstrap); err != nil {
		t.Fatal("Schema did not set up properly:", err)
	}
}

func openTestDatabase() (*sql.DB, error) {
	return openDSN(buildDSN(testSchemaName))
}

func openDSN(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func buildDSN(schema string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?multiStatements=true&parseTime=true", "root", "", "127.0.0.1", "3306", schema)
}

func setupSchema(db *sql.DB) error {
	statement := fmt.Sprintf("DROP SCHEMA IF EXISTS %s; CREATE SCHEMA %s; USE %s;",
		testSchemaName, testSchemaName, testSchemaName)
	if _, err := db.Exec(statement); err != nil {
		return err
	}
	content, err := os.ReadFile(findSchemaFile())
	if err != nil {
		return err
	}
	if _, err := db.Exec(string(content)); err != nil {
		return err
	}
	return nil
}

// findSchemaFile locates doc/mysql/schema.sql relative to this test file,
// since tests run from the package directory regardless of CWD.
func findSchemaFile() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "doc", "mysql", "schema.sql")
}
