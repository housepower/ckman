package local

import (
	"os"
	"testing"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

func TestMain(m *testing.M) {
	log.InitLoggerConsole()
	os.Exit(m.Run())
}

func newTestLP(t *testing.T) *LocalPersistent {
	t.Helper()
	dir := t.TempDir()
	lp := &LocalPersistent{}
	if err := lp.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "ckman_clusters.json"}); err != nil {
		t.Fatalf("init: %v", err)
	}
	return lp
}

func TestLocal_BackupPolicyCRUD(t *testing.T) {
	lp := newTestLP(t)
	p := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Enabled: true,
	}
	if err := lp.CreateBackupPolicy(p); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := lp.GetBackupPolicy("p1")
	if err != nil || got.PolicyID != "p1" {
		t.Fatalf("get: %v %+v", err, got)
	}
	got.Crontab = "0 5 * * *"
	if err := lp.UpdateBackupPolicy(got); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, _ := lp.GetBackupPolicy("p1")
	if got2.Crontab != "0 5 * * *" {
		t.Fatalf("update lost: %+v", got2)
	}
	if err := lp.DeleteBackupPolicy("p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got3, _ := lp.GetBackupPolicy("p1")
	if !got3.Deleted {
		t.Fatalf("delete should soft-delete: %+v", got3)
	}
}

func TestLocal_GetBackupPoliciesByCluster(t *testing.T) {
	lp := newTestLP(t)
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1"})
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p2", ClusterName: "ckA", Database: "dba", Table: "t2"})
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p3", ClusterName: "ckB", Database: "dbb", Table: "t3"})
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p4", ClusterName: "ckA", Database: "dba", Table: "t4", Deleted: true})

	got, err := lp.GetBackupPoliciesByCluster("ckA")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// 3 个 ckA 的，但 p4 deleted 不应返回
	if len(got) != 2 {
		t.Fatalf("expected 2 ckA policies (excluding deleted), got %d", len(got))
	}
}

func TestLocal_GetActiveScheduledPolicies(t *testing.T) {
	lp := newTestLP(t)
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA", Instance: "ckman-01", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED})
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p2", ClusterName: "ckA", Instance: "ckman-01", Enabled: false, ScheduleType: model.BACKUP_SCHEDULED})         // disabled
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p3", ClusterName: "ckA", Instance: "ckman-02", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED})          // 不同 instance
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p4", ClusterName: "ckA", Instance: "ckman-01", Enabled: true, ScheduleType: model.BACKUP_IMMEDIATE})          // immediate
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p5", ClusterName: "ckA", Instance: "ckman-01", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Deleted: true}) // deleted

	got, err := lp.GetActiveScheduledPolicies("ckman-01")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(got) != 1 || got[0].PolicyID != "p1" {
		t.Fatalf("expected only p1, got %+v", got)
	}
}

func TestLocal_BackupRunCRUD_AndQueries(t *testing.T) {
	lp := newTestLP(t)
	now := time.Now()
	mustCreate := func(id, policy, status, instance string, ts time.Time, cluster, db, table string) {
		t.Helper()
		r := model.BackupRun{
			RunID: id, PolicyID: policy, ClusterName: cluster, Database: db, Table: table,
			Status: status, StartedAt: ts, CreateTime: ts, Instance: instance,
		}
		if err := lp.CreateBackupRun(r); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	mustCreate("r1", "p1", model.BACKUP_STATUS_SUCCESS, "ckman-01", now.Add(-3*24*time.Hour), "ckA", "dba", "t1")
	mustCreate("r2", "p1", model.BACKUP_STATUS_SUCCESS, "ckman-01", now.Add(-2*24*time.Hour), "ckA", "dba", "t1")
	mustCreate("r3", "p1", model.BACKUP_STATUS_RUNNING, "ckman-01", now.Add(-time.Hour), "ckA", "dba", "t1")
	mustCreate("r4", "p2", model.BACKUP_STATUS_QUEUED, "ckman-01", now, "ckB", "dbb", "t2")

	runs, err := lp.GetRunsByPolicy("p1", 10, time.Time{})
	if err != nil || len(runs) != 3 {
		t.Fatalf("p1 runs: %v len=%d", err, len(runs))
	}
	tableRuns, _ := lp.GetRunsByTable("ckA", "dba", "t1", 7)
	if len(tableRuns) != 3 {
		t.Fatalf("table runs len=%d, expected 3", len(tableRuns))
	}
	inFlight, _ := lp.GetRunsInFlightByPolicy("p1")
	if len(inFlight) != 1 || inFlight[0].RunID != "r3" {
		t.Fatalf("in flight: %+v", inFlight)
	}
	insRuns, _ := lp.GetRunsInFlightByInstance("ckman-01")
	if len(insRuns) != 2 {
		t.Fatalf("instance in-flight: %d", len(insRuns))
	}
}

func TestLocal_GetRunsByPolicy_LimitAndBefore(t *testing.T) {
	lp := newTestLP(t)
	now := time.Now()
	for i := 0; i < 5; i++ {
		_ = lp.CreateBackupRun(model.BackupRun{
			RunID: "r" + string(rune('0'+i)), PolicyID: "p1",
			Status:     model.BACKUP_STATUS_SUCCESS,
			StartedAt:  now.Add(-time.Duration(i) * time.Hour),
			CreateTime: now.Add(-time.Duration(i) * time.Hour),
		})
	}
	limited, _ := lp.GetRunsByPolicy("p1", 2, time.Time{})
	if len(limited) != 2 {
		t.Fatalf("limit: %d", len(limited))
	}
	before, _ := lp.GetRunsByPolicy("p1", 10, now.Add(-2*time.Hour))
	// 严格在 before 之前的：r2/r3/r4（StartedAt = now-2h/-3h/-4h，需 Before(now-2h) 严格小于）
	if len(before) < 2 || len(before) > 3 {
		t.Fatalf("before: %d (expected 2 or 3)", len(before))
	}
}

func TestLocal_MarkRunRunningIfQueued(t *testing.T) {
	lp := newTestLP(t)
	r := model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_QUEUED, Instance: "ckman-01"}
	_ = lp.CreateBackupRun(r)

	ok, err := lp.MarkRunRunningIfQueued("r1", "ckman-01", time.Now())
	if err != nil || !ok {
		t.Fatalf("first mark: ok=%v err=%v", ok, err)
	}
	ok2, err := lp.MarkRunRunningIfQueued("r1", "ckman-01", time.Now())
	if err != nil || ok2 {
		t.Fatalf("second mark should false: ok=%v err=%v", ok2, err)
	}
}

func TestLocal_GetAllBackupPolicies(t *testing.T) {
	lp := newTestLP(t)
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA"})
	_ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p2", ClusterName: "ckB"})
	all, err := lp.GetAllBackupPolicies()
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(all))
	}
}

func TestLocal_GetAllBackupRuns(t *testing.T) {
	lp := newTestLP(t)
	_ = lp.CreateBackupRun(model.BackupRun{RunID: "r1", PolicyID: "p1"})
	_ = lp.CreateBackupRun(model.BackupRun{RunID: "r2", PolicyID: "p2"})
	all, err := lp.GetAllBackupRuns()
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(all))
	}
}
