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

func TestTranslate_ShowDatabases(t *testing.T) {
	cases := []struct {
		backend Backend
		want    string
	}{
		{BackendSQLite, "PRAGMA database_list"},
		{BackendMySQL, "SHOW DATABASES"},
		{BackendDM8, "SHOW DATABASES"},
		{BackendPostgres, "SELECT datname FROM pg_database WHERE datistemplate = false"},
	}
	for _, c := range cases {
		got := Translate("SHOW DATABASES", c.backend)
		if got != c.want {
			t.Errorf("%v: got %q, want %q", c.backend, got, c.want)
		}
	}
}

func TestTranslate_ShowTables(t *testing.T) {
	cases := []struct {
		backend Backend
		want    string
	}{
		{BackendSQLite, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"},
		{BackendMySQL, "SHOW TABLES"},
		{BackendDM8, "SHOW TABLES"},
		{BackendPostgres, "SELECT tablename FROM pg_tables WHERE schemaname='public'"},
	}
	for _, c := range cases {
		got := Translate("show tables", c.backend) // case-insensitive
		if got != c.want {
			t.Errorf("%v: got %q, want %q", c.backend, got, c.want)
		}
	}
}

func TestTranslate_Desc(t *testing.T) {
	cases := []struct {
		input   string
		backend Backend
		want    string
	}{
		{"DESC tbl_cluster", BackendSQLite, "PRAGMA table_info(tbl_cluster)"},
		{"DESCRIBE tbl_cluster", BackendSQLite, "PRAGMA table_info(tbl_cluster)"},
		{"DESC tbl_cluster", BackendMySQL, "DESC tbl_cluster"},
		{"DESC tbl_cluster", BackendPostgres,
			"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl_cluster'"},
	}
	for _, c := range cases {
		got := Translate(c.input, c.backend)
		if got != c.want {
			t.Errorf("%s on %v: got %q, want %q", c.input, c.backend, got, c.want)
		}
	}
}

func TestTranslate_PassThrough(t *testing.T) {
	in := "SELECT * FROM tbl_cluster WHERE cluster_name = 'ck1'"
	if got := Translate(in, BackendSQLite); got != in {
		t.Errorf("expected pass-through, got %q", got)
	}
}
