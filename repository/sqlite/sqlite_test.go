package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestSQLite_QueryHistoryCRUD(t *testing.T) {
	sp := newTestSP(t)
	qh := model.QueryHistory{CheckSum: "qh1", Cluster: "ck1", QuerySql: "SELECT 1"}
	if err := sp.CreateQueryHistory(qh); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sp.GetQueryHistoryByCheckSum("qh1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuerySql != "SELECT 1" {
		t.Fatalf("unexpected: %+v", got)
	}

	qh.QuerySql = "SELECT 2"
	if err := sp.UpdateQueryHistory(qh); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, _ := sp.GetQueryHistoryByCheckSum("qh1")
	if got2.QuerySql != "SELECT 2" {
		t.Fatalf("update lost: %+v", got2)
	}

	if c := sp.GetQueryHistoryCount("ck1"); c != 1 {
		t.Fatalf("count: %d", c)
	}
	all, _ := sp.GetAllQueryHistory()
	if len(all) != 1 {
		t.Fatalf("all: %d", len(all))
	}
	byCluster, _ := sp.GetQueryHistoryByCluster("ck1")
	if len(byCluster) != 1 {
		t.Fatalf("by cluster: %d", len(byCluster))
	}
	if _, err := sp.GetEarliestQuery("ck1"); err != nil {
		t.Fatalf("earliest: %v", err)
	}

	if err := sp.DeleteQueryHistory("qh1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestSQLite_TaskCRUD(t *testing.T) {
	sp := newTestSP(t)
	task := model.Task{TaskId: "t1", Status: model.TaskStatusWaiting, ServerIp: "1.2.3.4"}
	if err := sp.CreateTask(task); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sp.GetTaskbyTaskId("t1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.TaskId != "t1" {
		t.Fatalf("unexpected: %+v", got)
	}

	// While Waiting, GetPengdingTasks for matching IP should return it.
	pending, _ := sp.GetPengdingTasks("1.2.3.4")
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending task while waiting, got %d", len(pending))
	}

	task.Status = model.TaskStatusRunning
	if err := sp.UpdateTask(task); err != nil {
		t.Fatalf("update: %v", err)
	}

	pending2, _ := sp.GetPengdingTasks("1.2.3.4")
	if len(pending2) != 0 {
		t.Fatalf("running task should not be pending: %d", len(pending2))
	}

	if c := sp.GetEffectiveTaskCount(); c != 1 {
		t.Fatalf("effective count: %d", c)
	}

	if err := sp.DeleteTask("t1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestSQLite_BackupCRUD(t *testing.T) {
	sp := newTestSP(t)
	b := model.Backup{
		BackupId: "b1", ClusterName: "ck1",
		Database: "db", Table: "t",
		Operation: "BACKUP", ScheduleType: "ONCE",
	}
	if err := sp.CreateBackup(b); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sp.GetBackupById("b1")
	if err != nil || got.BackupId != "b1" {
		t.Fatalf("get by id: %v %+v", err, got)
	}
	byTable, err := sp.GetBackupByTable("ck1", "db", "t")
	if err != nil || byTable.BackupId != "b1" {
		t.Fatalf("get by table: %v %+v", err, byTable)
	}
	all, _ := sp.GetAllBackups("ck1")
	if len(all) != 1 {
		t.Fatalf("all: %d", len(all))
	}
	byOp, _ := sp.GetbackupByOperation("BACKUP")
	if len(byOp) != 1 {
		t.Fatalf("by op: %d", len(byOp))
	}
	bySched, _ := sp.GetBackupByShechuleType("ONCE")
	if len(bySched) != 1 {
		t.Fatalf("by sched: %d", len(bySched))
	}

	b.Operation = "RESTORE"
	if err := sp.UpdateBackup(b); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := sp.DeleteBackup("b1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestSQLite_BackupPolicyCRUD(t *testing.T) {
	sp := newTestSP(t)
	p := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ck1",
		Database: "db", Table: "t",
		Instance: "1.2.3.4", ScheduleType: model.BACKUP_SCHEDULED,
		Enabled: true,
	}
	if err := sp.CreateBackupPolicy(p); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sp.GetBackupPolicy("p1")
	if err != nil || got.PolicyID != "p1" {
		t.Fatalf("get: %v %+v", err, got)
	}

	got.Crontab = "0 5 * * *"
	if err := sp.UpdateBackupPolicy(got); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, _ := sp.GetBackupPolicy("p1")
	if got2.Crontab != "0 5 * * *" {
		t.Fatalf("update lost: %+v", got2)
	}

	byCluster, _ := sp.GetBackupPoliciesByCluster("ck1")
	if len(byCluster) != 1 {
		t.Fatalf("by cluster: %d", len(byCluster))
	}

	active, _ := sp.GetActiveScheduledPolicies("1.2.3.4")
	if len(active) != 1 {
		t.Fatalf("active: %d", len(active))
	}

	all, _ := sp.GetAllBackupPolicies()
	if len(all) != 1 {
		t.Fatalf("all: %d", len(all))
	}

	if err := sp.DeleteBackupPolicy("p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got3, _ := sp.GetBackupPolicy("p1")
	if !got3.Deleted {
		t.Fatalf("delete should soft-delete: %+v", got3)
	}
}

func TestSQLite_BackupRunCRUD(t *testing.T) {
	sp := newTestSP(t)
	r := model.BackupRun{
		RunID: "r1", PolicyID: "p1", ClusterName: "ck1",
		Database: "db", Table: "t",
		Status:    model.BACKUP_STATUS_QUEUED,
		StartedAt: time.Now(),
	}
	if err := sp.CreateBackupRun(r); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sp.GetBackupRun("r1")
	if err != nil || got.RunID != "r1" {
		t.Fatalf("get: %v %+v", err, got)
	}

	inflight, _ := sp.GetRunsInFlightByPolicy("p1")
	if len(inflight) != 1 {
		t.Fatalf("inflight by policy: %d", len(inflight))
	}

	ok, err := sp.MarkRunRunningIfQueued("r1", "1.2.3.4", time.Now())
	if err != nil || !ok {
		t.Fatalf("mark running: ok=%v err=%v", ok, err)
	}

	got2, _ := sp.GetBackupRun("r1")
	if got2.Status != model.BACKUP_STATUS_RUNNING {
		t.Fatalf("status not updated: %+v", got2)
	}

	inflight2, _ := sp.GetRunsInFlightByInstance("1.2.3.4")
	if len(inflight2) != 1 {
		t.Fatalf("inflight by instance: %d", len(inflight2))
	}

	byPolicy, _ := sp.GetRunsByPolicy("p1", 10, time.Time{})
	if len(byPolicy) != 1 {
		t.Fatalf("by policy: %d", len(byPolicy))
	}

	byTable, _ := sp.GetRunsByTable("ck1", "db", "t", 7)
	if len(byTable) != 1 {
		t.Fatalf("by table: %d", len(byTable))
	}

	all, _ := sp.GetAllBackupRuns()
	if len(all) != 1 {
		t.Fatalf("all: %d", len(all))
	}

	if err := sp.DeleteBackupRun("r1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

// ─── Migration tests ──────────────────────────────────────────────────────────

func TestMigrateFromJSON(t *testing.T) {
	dir := t.TempDir()
	src, err := os.ReadFile("testdata/legacy_clusters.json")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	legacyPath := filepath.Join(dir, "clusters.json")
	if err := os.WriteFile(legacyPath, src, 0644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	sp := NewSQLitePersistent()
	if err := sp.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("init: %v", err)
	}

	if _, err := sp.GetClusterbyName("ck1"); err != nil {
		t.Fatalf("cluster ck1 missing after migrate: %v", err)
	}
	if _, err := sp.GetLogicClusterbyName("L1"); err != nil {
		t.Fatalf("logic L1 missing: %v", err)
	}
	if _, err := sp.GetBackupPolicy("p1"); err != nil {
		t.Fatalf("policy p1 missing: %v", err)
	}
	if _, err := sp.GetBackupRun("br1"); err != nil {
		t.Fatalf("run br1 missing: %v", err)
	}

	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy file should have been renamed away (err=%v)", err)
	}
	matches, _ := filepath.Glob(legacyPath + ".migrated.*")
	if len(matches) != 1 {
		t.Fatalf("expected one .migrated.* file, got %d", len(matches))
	}

	v, _ := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
	if v == "" || v == META_FRESH_INSTALL {
		t.Fatalf("migrated_from not set correctly: %q", v)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	dir := t.TempDir()
	src, _ := os.ReadFile("testdata/legacy_clusters.json")
	_ = os.WriteFile(filepath.Join(dir, "clusters.json"), src, 0644)

	sp1 := NewSQLitePersistent()
	if err := sp1.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("first init: %v", err)
	}
	v1, _ := readMeta(sp1.Client, METAKEY_MIGRATED_FROM)

	sp2 := NewSQLitePersistent()
	if err := sp2.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("second init: %v", err)
	}
	v2, _ := readMeta(sp2.Client, METAKEY_MIGRATED_FROM)
	if v1 != v2 {
		t.Fatalf("meta changed on idempotent re-init: %q -> %q", v1, v2)
	}
}

func TestMigrateLegacyCorrupt(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "clusters.json"), []byte("{not valid json"), 0644)

	sp := NewSQLitePersistent()
	err := sp.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"})
	if err == nil {
		t.Fatalf("expected init to fail on corrupt legacy file")
	}
}
