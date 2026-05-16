package migrate

import (
	"os"
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/repository/legacyjson"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func newLegacyJSON(t *testing.T) repository.PersistentMgr {
	t.Helper()
	dir := t.TempDir()
	ps, err := legacyjson.NewReader(legacyjson.LocalConfig{
		Format:     "json",
		ConfigDir:  dir,
		ConfigFile: "test_clusters",
	})
	if err != nil {
		t.Fatalf("init legacyjson: %v", err)
	}
	return ps
}

func TestMigrateBetween_CoversAllEntities(t *testing.T) {
	src := newLegacyJSON(t)
	dst := newLegacyJSON(t)

	if err := src.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1"}); err != nil {
		t.Fatalf("src create cluster: %v", err)
	}
	if err := src.CreateLogicCluster("L1", []string{"ck1"}); err != nil {
		t.Fatalf("src create logic: %v", err)
	}
	if err := src.CreateQueryHistory(model.QueryHistory{CheckSum: "qh1", Cluster: "ck1", QuerySql: "SELECT 1"}); err != nil {
		t.Fatalf("src create qh: %v", err)
	}
	if err := src.CreateTask(model.Task{TaskId: "t1"}); err != nil {
		t.Fatalf("src create task: %v", err)
	}
	if err := src.CreateBackup(model.Backup{BackupId: "b1", ClusterName: "ck1"}); err != nil {
		t.Fatalf("src create backup: %v", err)
	}
	if err := src.CreateBackupPolicy(model.BackupPolicy{PolicyID: "bp1", ClusterName: "ck1"}); err != nil {
		t.Fatalf("src create policy: %v", err)
	}
	if err := src.CreateBackupRun(model.BackupRun{RunID: "br1", PolicyID: "bp1"}); err != nil {
		t.Fatalf("src create run: %v", err)
	}

	if err := MigrateBetween(src, dst); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := dst.GetClusterbyName("ck1"); err != nil {
		t.Fatalf("dst missing cluster: %v", err)
	}
	if _, err := dst.GetLogicClusterbyName("L1"); err != nil {
		t.Fatalf("dst missing logic: %v", err)
	}
	if _, err := dst.GetQueryHistoryByCheckSum("qh1"); err != nil {
		t.Fatalf("dst missing query history: %v", err)
	}
	if _, err := dst.GetTaskbyTaskId("t1"); err != nil {
		t.Fatalf("dst missing task: %v", err)
	}
	if _, err := dst.GetBackupById("b1"); err != nil {
		t.Fatalf("dst missing backup: %v", err)
	}
	if _, err := dst.GetBackupPolicy("bp1"); err != nil {
		t.Fatalf("dst missing policy: %v", err)
	}
	if _, err := dst.GetBackupRun("br1"); err != nil {
		t.Fatalf("dst missing run: %v", err)
	}
}
