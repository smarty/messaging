package sqladapter

import (
	"cmp"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// TODO: move away from environment variables for connection parameters (just use sensible defaults from db-connector)

// Integration tests in this package require a local MySQL server. Connection
// parameters are taken from environment variables (with sensible defaults):
//
//	HARNESS_TEST_MYSQL_DSN — overrides the full DSN
//	MYSQL_HOST             — default 127.0.0.1
//	MYSQL_PORT             — default 3306
//	MYSQL_USER             — default root
//	MYSQL_PASSWORD         — default (empty)
//
// Tests run against a throwaway schema (default `messaging_harness_test`) which
// is dropped and re-created before each run.

const testSchemaName = "messaging_harness_test"

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
	if override := os.Getenv("HARNESS_TEST_MYSQL_DSN"); override != "" {
		return override
	}
	host := cmp.Or(os.Getenv("MYSQL_HOST"), "127.0.0.1")
	port := cmp.Or(os.Getenv("MYSQL_PORT"), "3306")
	user := cmp.Or(os.Getenv("MYSQL_USER"), "root")
	password := os.Getenv("MYSQL_PASSWORD")
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?multiStatements=true&parseTime=true", user, password, host, port, schema)
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

// findSchemaFile locates sqlmq/_schema_mysql.sql relative to this test file,
// since tests run from the package directory regardless of CWD.
func findSchemaFile() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "sqlmq", "_schema_mysql.sql")
}
