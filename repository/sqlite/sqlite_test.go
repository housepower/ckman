package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
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

func TestSQLite_ClusterCRUD(t *testing.T) {
	sp := newTestSP(t)
	cfg := model.CKManClickHouseConfig{Cluster: "ck1", Comment: "test"}
	if err := sp.CreateCluster(cfg); err != nil {
		t.Fatalf("create: %v", err)
	}
	if !sp.ClusterExists("ck1") {
		t.Fatalf("ClusterExists should return true")
	}
	got, err := sp.GetClusterbyName("ck1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Cluster != "ck1" || got.Comment != "test" {
		t.Fatalf("unexpected: %+v", got)
	}

	cfg.Comment = "updated"
	if err := sp.UpdateCluster(cfg); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, _ := sp.GetClusterbyName("ck1")
	if got2.Comment != "updated" {
		t.Fatalf("update lost: %+v", got2)
	}

	all, _ := sp.GetAllClusters()
	if len(all) != 1 {
		t.Fatalf("GetAll size: %d", len(all))
	}

	if err := sp.DeleteCluster("ck1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if sp.ClusterExists("ck1") {
		t.Fatalf("ClusterExists should return false after delete")
	}
}

func TestSQLite_LogicCRUD(t *testing.T) {
	sp := newTestSP(t)
	if err := sp.CreateLogicCluster("L1", []string{"ck1", "ck2"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	physics, err := sp.GetLogicClusterbyName("L1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(physics) != 2 {
		t.Fatalf("expected 2 physics, got %d", len(physics))
	}

	if err := sp.UpdateLogicCluster("L1", []string{"ck1", "ck2", "ck3"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	physics2, _ := sp.GetLogicClusterbyName("L1")
	if len(physics2) != 3 {
		t.Fatalf("update lost: %v", physics2)
	}

	if err := sp.DeleteLogicCluster("L1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestSQLite_TxCommit(t *testing.T) {
	sp := newTestSP(t)
	if err := sp.Begin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := sp.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := sp.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := sp.GetClusterbyName("ck1"); err != nil {
		t.Fatalf("get after commit: %v", err)
	}
}

func TestSQLite_TxRollback(t *testing.T) {
	sp := newTestSP(t)
	if err := sp.Begin(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := sp.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := sp.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	_, err := sp.GetClusterbyName("ck1")
	if !errors.Is(err, repository.ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound, got %v", err)
	}
}
