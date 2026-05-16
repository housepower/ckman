package sqlcli

import (
	"os"
	"testing"

	"github.com/housepower/ckman/log"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func TestOpenDB_SQLite(t *testing.T) {
	dir := t.TempDir()
	cfgMap := map[string]interface{}{
		"config_dir":  dir,
		"config_file": "testdb",
	}
	db, backend, err := OpenDB("local", cfgMap)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if backend != BackendSQLite {
		t.Fatalf("unexpected backend: %v", backend)
	}
	if db == nil {
		t.Fatalf("nil db")
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()
}

func TestOpenDB_UnsupportedPolicy(t *testing.T) {
	_, _, err := OpenDB("oracle", map[string]interface{}{})
	if err == nil {
		t.Fatalf("expected error for unsupported policy")
	}
}
