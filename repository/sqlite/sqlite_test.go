package sqlite

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/housepower/ckman/log"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

// newTestSP 在临时目录创建 SQLitePersistent，保证测试 _meta.migrated_from == "(fresh install)"。
func newTestSP(t *testing.T) *SQLitePersistent {
	t.Helper()
	dir := t.TempDir()
	sp := NewSQLitePersistent()
	if err := sp.Init(LocalConfig{ConfigDir: dir, ConfigFile: "testdb"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	return sp
}

func TestSQLite_FreshInit(t *testing.T) {
	sp := newTestSP(t)
	v, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	if v != META_FRESH_INSTALL {
		t.Fatalf("expected fresh install, got %q", v)
	}
	if _, err := os.Stat(filepath.Join(sp.Config.ConfigDir, "testdb.db")); err != nil {
		t.Fatalf("db file missing: %v", err)
	}
}
