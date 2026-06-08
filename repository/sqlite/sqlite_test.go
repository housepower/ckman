package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
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

// Task.Step is a new field (2b-1) round-tripped through the JSON blob in
// TblTask.Task. Verifies persistence works without a schema migration, which
// is the same story for the other DB backends.
func TestSQLite_TaskStepRoundTrip(t *testing.T) {
	sp := newTestSP(t)
	task := model.Task{
		TaskId:   "step-1",
		Status:   model.TaskStatusRunning,
		ServerIp: "1.2.3.4",
		Step:     model.StepShardingMove,
	}
	if err := sp.CreateTask(task); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sp.GetTaskbyTaskId("step-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Step != model.StepShardingMove {
		t.Fatalf("step not preserved on create: %+v", got.Step)
	}

	task.Step = model.StepShardingInsert
	if err := sp.UpdateTask(task); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err = sp.GetTaskbyTaskId("step-1")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.Step != model.StepShardingInsert {
		t.Fatalf("step not updated: %+v", got.Step)
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

// TestMigrateRecoveryFromHalfCommit_OtherTable 验证：legacy 文件里 clusters={}
// 但 query_history 非空，第一次迁移后清掉 _meta，重启时仍然能识别为已迁移。
// 这条覆盖 codex review 提到的"cluster=0 但其他表非空"的边界。
func TestMigrateRecoveryFromHalfCommit_OtherTable(t *testing.T) {
	dir := t.TempDir()
	historyOnly := []byte(`{
		"clusters": {},
		"logics": {},
		"query_history": {"qh1": {"CheckSum": "qh1", "Cluster": "ck", "QuerySql": "SELECT 1"}},
		"tasks": {}, "backup": {}, "backup_policy": {}, "backup_run": {}
	}`)
	legacyPath := filepath.Join(dir, "clusters.json")
	if err := os.WriteFile(legacyPath, historyOnly, 0644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	sp1 := NewSQLitePersistent()
	if err := sp1.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("first init: %v", err)
	}
	matches, _ := filepath.Glob(legacyPath + ".migrated.*")
	if len(matches) != 1 {
		t.Fatalf("expected one migrated backup, got %d", len(matches))
	}
	if err := os.Rename(matches[0], legacyPath); err != nil {
		t.Fatalf("restore legacy: %v", err)
	}
	if err := sp1.Client.Exec("DELETE FROM tbl_meta WHERE key = ?", METAKEY_MIGRATED_FROM).Error; err != nil {
		t.Fatalf("clear meta: %v", err)
	}

	sp2 := NewSQLitePersistent()
	if err := sp2.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("second init should recover even with empty cluster table: %v", err)
	}
	v, _ := readMeta(sp2.Client, METAKEY_MIGRATED_FROM)
	if !contains(v, "(recovered)") {
		t.Fatalf("recovery marker missing: %q", v)
	}
}

// TestMigrateRecoveryFromHalfCommit 验证：上次迁移已提交数据但 _meta 写入失败的场景下，
// 下次启动不会报 UNIQUE 冲突，而是补上 _meta 标记继续运行。
func TestMigrateRecoveryFromHalfCommit(t *testing.T) {
	dir := t.TempDir()
	src, _ := os.ReadFile("testdata/legacy_clusters.json")
	legacyPath := filepath.Join(dir, "clusters.json")
	if err := os.WriteFile(legacyPath, src, 0644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	// 第一次启动：完成迁移（包括改名 + meta）
	sp1 := NewSQLitePersistent()
	if err := sp1.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("first init: %v", err)
	}

	// 模拟"meta 丢失"：把改名后的 legacy 文件改回 + 清空 _meta
	matches, _ := filepath.Glob(legacyPath + ".migrated.*")
	if len(matches) != 1 {
		t.Fatalf("expected one migrated backup, got %d", len(matches))
	}
	if err := os.Rename(matches[0], legacyPath); err != nil {
		t.Fatalf("restore legacy: %v", err)
	}
	if err := sp1.Client.Exec("DELETE FROM tbl_meta WHERE key = ?", METAKEY_MIGRATED_FROM).Error; err != nil {
		t.Fatalf("clear meta: %v", err)
	}

	// 第二次启动：应当检测到已有数据，写恢复标记，不重跑迁移
	sp2 := NewSQLitePersistent()
	if err := sp2.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
		t.Fatalf("second init should recover, got: %v", err)
	}
	v, _ := readMeta(sp2.Client, METAKEY_MIGRATED_FROM)
	if v == "" {
		t.Fatalf("recovery did not write meta")
	}
	if !contains(v, "(recovered)") {
		t.Fatalf("recovery marker missing: %q", v)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestSeedAdminFromInitPersistent(t *testing.T) {
	// Stage a temp config so InitPersistent picks SQLite.
	dir := t.TempDir()
	saved := config.GlobalConfig
	t.Cleanup(func() { config.GlobalConfig = saved })

	config.GlobalConfig.Server.PersistentPolicy = SQLitePersistentName
	config.GlobalConfig.PersistentConfig = map[string]map[string]interface{}{
		SQLitePersistentName: {
			"config_dir":  dir,
			"config_file": "test.db",
		},
	}
	repository.Ps = nil
	if err := repository.InitPersistent(); err != nil {
		t.Fatalf("first InitPersistent failed: %v", err)
	}

	got, err := repository.Ps.GetUserByName(common.DefaultAdminName)
	if err != nil {
		t.Fatalf("admin not seeded: %v", err)
	}
	if got.Policy != common.ADMIN {
		t.Errorf("expected admin policy, got %q", got.Policy)
	}
	if !got.Enabled {
		t.Errorf("expected enabled=true")
	}

	// Idempotent: re-initializing must not duplicate or fail.
	repository.Ps = nil
	if err := repository.InitPersistent(); err != nil {
		t.Fatalf("second InitPersistent failed: %v", err)
	}
	all, err := repository.Ps.GetAllUsers()
	if err != nil {
		t.Fatalf("get all users failed: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("expected 1 user after re-init, got %d", len(all))
	}
}

func TestUserCRUD(t *testing.T) {
	sp := newTestSP(t)

	user := model.CkmanUser{
		Username:     "alice",
		PasswordHash: "hashvalue",
		Policy:       "ordinary",
		Enabled:      true,
	}
	if err := sp.CreateUser(user); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if !sp.UserExists("alice") {
		t.Fatalf("UserExists should return true")
	}

	got, err := sp.GetUserByName("alice")
	if err != nil {
		t.Fatalf("GetUserByName: %v", err)
	}
	if got.Username != "alice" {
		t.Fatalf("unexpected username: %q", got.Username)
	}
	if got.Policy != "ordinary" {
		t.Fatalf("unexpected policy: %q", got.Policy)
	}
	if !got.Enabled {
		t.Fatalf("expected enabled=true")
	}

	// Duplicate insert → ErrRecordExists
	err = sp.CreateUser(user)
	if !errors.Is(err, repository.ErrRecordExists) {
		t.Fatalf("expected ErrRecordExists, got %v", err)
	}

	// Update
	user.Policy = "guest"
	user.Enabled = false
	user.PasswordHash = "newhash"
	if err := sp.UpdateUser(user); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	got2, _ := sp.GetUserByName("alice")
	if got2.Policy != "guest" {
		t.Fatalf("policy not updated: %q", got2.Policy)
	}
	if got2.Enabled {
		t.Fatalf("expected enabled=false")
	}
	if got2.PasswordHash != "newhash" {
		t.Fatalf("password_hash not updated: %q", got2.PasswordHash)
	}

	// GetAll
	all, err := sp.GetAllUsers()
	if err != nil {
		t.Fatalf("GetAllUsers: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 user, got %d", len(all))
	}

	// Delete
	if err := sp.DeleteUser("alice"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	if sp.UserExists("alice") {
		t.Fatalf("UserExists should return false after delete")
	}
	_, err = sp.GetUserByName("alice")
	if !errors.Is(err, repository.ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound, got %v", err)
	}

	// Delete missing → ErrRecordNotFound
	err = sp.DeleteUser("ghost")
	if !errors.Is(err, repository.ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound for ghost, got %v", err)
	}
}

func TestCreateAfterDeleteAllowsReuse(t *testing.T) {
	sp := newTestSP(t)

	user := model.CkmanUser{
		Username:     "alice",
		PasswordHash: "h1",
		Policy:       "ordinary",
		Enabled:      true,
	}
	if err := sp.CreateUser(user); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := sp.DeleteUser("alice"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	// Same username must be reusable after delete (proves hard delete).
	user2 := model.CkmanUser{
		Username:     "alice",
		PasswordHash: "h2",
		Policy:       "guest",
		Enabled:      false,
	}
	if err := sp.CreateUser(user2); err != nil {
		t.Fatalf("CreateUser after delete: %v", err)
	}

	got, err := sp.GetUserByName("alice")
	if err != nil {
		t.Fatalf("GetUserByName: %v", err)
	}
	if got.Policy != "guest" {
		t.Fatalf("expected policy=guest, got %q", got.Policy)
	}
	if got.PasswordHash != "h2" {
		t.Fatalf("expected password_hash=h2, got %q", got.PasswordHash)
	}
	if got.Enabled {
		t.Fatalf("expected enabled=false")
	}
}

func TestSQLite_GetRunsByTable_UnlimitedWindow(t *testing.T) {
	sp := newTestSP(t)
	now := time.Now()
	// 一条 30 天前、一条 400 天前(超出任何 365 窗口)
	recent := model.BackupRun{RunID: "rr", PolicyID: "p1", ClusterName: "ck1",
		Database: "db", Table: "t", Status: model.BACKUP_STATUS_SUCCESS, StartedAt: now.AddDate(0, 0, -30)}
	old := model.BackupRun{RunID: "ro", PolicyID: "p1", ClusterName: "ck1",
		Database: "db", Table: "t", Status: model.BACKUP_STATUS_SUCCESS, StartedAt: now.AddDate(0, 0, -400)}
	if err := sp.CreateBackupRun(recent); err != nil {
		t.Fatal(err)
	}
	if err := sp.CreateBackupRun(old); err != nil {
		t.Fatal(err)
	}
	// 365 天窗口只看到近的
	in365, _ := sp.GetRunsByTable("ck1", "db", "t", 365)
	if len(in365) != 1 || in365[0].RunID != "rr" {
		t.Fatalf("sinceDays=365 should return only recent, got %+v", in365)
	}
	// sinceDays<=0 看到全部(含 400 天前)
	for _, d := range []int{0, -1} {
		all, _ := sp.GetRunsByTable("ck1", "db", "t", d)
		if len(all) != 2 {
			t.Fatalf("sinceDays=%d should return all history, got %d", d, len(all))
		}
	}
}
