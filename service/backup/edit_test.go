package backup

import (
	"testing"
	"time"

	"github.com/housepower/ckman/model"
)

func timeFromUnix(s int64) time.Time { return time.Unix(s, 0) }

func TestUpdatePolicy_RejectsImmutableFieldChanges(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	cases := []model.BackupPolicy{
		{PolicyID: "p1", ClusterName: "ckB", Database: "dba", Table: "t1", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *"},
		{PolicyID: "p1", ClusterName: "ckA", Database: "dbb", Table: "t1", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *"},
		{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t2", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *"},
		{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1", ScheduleType: model.BACKUP_IMMEDIATE},
	}
	for i, p := range cases {
		if err := svc.UpdatePolicy(p); err == nil {
			t.Errorf("case %d should reject", i)
		}
	}
}

// 编辑时若 secret 为空（前端通常不重传敏感字段），应保留旧值，
// 否则会静默清空 S3 凭证导致后续 run 失败。
func TestUpdatePolicy_PreservesSecretWhenEmpty(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_IMMEDIATE,
		S3: model.TargetS3{
			AccessKeyID:     "AK",
			SecretAccessKey: "SK-OLD",
		},
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	// 模拟前端编辑：只改 bucket，不带 SecretAccessKey
	upd := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_IMMEDIATE,
		S3: model.TargetS3{
			AccessKeyID: "AK",
			Bucket:      "new-bucket",
			// SecretAccessKey 空
		},
	}
	if err := svc.UpdatePolicy(upd); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ := repo.GetPolicy("p1")
	if got.S3.SecretAccessKey != "SK-OLD" {
		t.Fatalf("secret was wiped: got %q want SK-OLD", got.S3.SecretAccessKey)
	}
	if got.S3.Bucket != "new-bucket" {
		t.Fatalf("bucket not updated: %q", got.S3.Bucket)
	}
}

func TestUpdatePolicy_AllowsEditableFields(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *", Instance: "ckman-01",
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	upd := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 5 * * *", Instance: "ckman-02", Enabled: true,
	}
	if err := svc.UpdatePolicy(upd); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ := repo.GetPolicy("p1")
	if got.Crontab != "0 5 * * *" || got.Instance != "ckman-02" {
		t.Fatalf("not updated: %+v", got)
	}
}

func TestUpdatePolicy_ValidatesCrontab(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	upd := model.BackupPolicy{
		PolicyID: "p1", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "* * * * *",
	}
	if err := svc.UpdatePolicy(upd); err == nil {
		t.Fatal("invalid crontab should reject")
	}
}

func TestUpdatePolicy_PreservesCreateTime(t *testing.T) {
	importTime := timeFromUnix(100)
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
		CreateTime: importTime,
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	upd := model.BackupPolicy{
		PolicyID: "p1", ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 5 * * *",
	}
	_ = svc.UpdatePolicy(upd)
	got, _ := repo.GetPolicy("p1")
	if !got.CreateTime.Equal(importTime) {
		t.Fatalf("CreateTime should preserve: %v", got.CreateTime)
	}
}

func TestDeletePolicy_HardDeletesPolicyAndRuns(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", Enabled: true}
	// 两条本 policy 的终态 run + 一条其它 policy 的 run(不应被删)
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_SUCCESS}
	repo.runs["r2"] = model.BackupRun{RunID: "r2", PolicyID: "p1", Status: model.BACKUP_STATUS_FAILED}
	repo.runs["r3"] = model.BackupRun{RunID: "r3", PolicyID: "other", Status: model.BACKUP_STATUS_SUCCESS}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	if err := svc.DeletePolicy("p1"); err != nil {
		t.Fatal(err)
	}
	// policy 物理删:行不再存在(不是 deleted=true 的软删)
	if _, ok := repo.policies["p1"]; ok {
		t.Fatalf("policy should be hard-deleted, still present: %+v", repo.policies["p1"])
	}
	// 本 policy 的 run 台账全删
	if _, ok := repo.runs["r1"]; ok {
		t.Fatal("run r1 of deleted policy should be removed")
	}
	if _, ok := repo.runs["r2"]; ok {
		t.Fatal("run r2 of deleted policy should be removed")
	}
	// 其它 policy 的 run 必须保留
	if _, ok := repo.runs["r3"]; !ok {
		t.Fatal("run r3 of other policy must be kept")
	}
}

func TestDeletePolicy_RejectsInFlight(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", Enabled: true}
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	if err := svc.DeletePolicy("p1"); err == nil {
		t.Fatal("expected delete to be rejected while a run is in-flight")
	}
	// 拒绝后不得删除任何东西
	if _, ok := repo.policies["p1"]; !ok {
		t.Fatal("policy must remain when delete rejected")
	}
	if _, ok := repo.runs["r1"]; !ok {
		t.Fatal("in-flight run must remain when delete rejected")
	}
}

func TestUpdatePolicy_RejectsTaskIDChange(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
		TaskID: "task-aaa", TaskName: "original-task",
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	upd := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
		TaskID: "task-bbb", // different — must be rejected
	}
	if err := svc.UpdatePolicy(upd); err == nil {
		t.Fatal("changing TaskID should be rejected")
	}
}

func TestUpdatePolicy_AllowsTaskNameChange(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
		TaskID: "task-aaa", TaskName: "original-task",
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	upd := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Crontab: "0 3 * * *",
		TaskID: "task-aaa", TaskName: "renamed-task", // same TaskID, different name — allowed
	}
	if err := svc.UpdatePolicy(upd); err != nil {
		t.Fatalf("changing TaskName should be allowed: %v", err)
	}
	got, _ := repo.GetPolicy("p1")
	if got.TaskName != "renamed-task" {
		t.Fatalf("expected TaskName=%q, got %q", "renamed-task", got.TaskName)
	}
	if got.TaskID != "task-aaa" {
		t.Fatalf("TaskID must not change: %q", got.TaskID)
	}
}

func TestTriggerPolicy_CreatesRun(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED,
	}
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)
	runID, err := svc.TriggerPolicy("p1")
	if err != nil {
		t.Fatal(err)
	}
	if runID == "" || len(pool.in) != 1 {
		t.Fatalf("trigger should enqueue: runID=%s pool=%v", runID, pool.in)
	}
}

func TestUpdatePolicy_ValidatesBandwidth(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_IMMEDIATE, TargetType: model.BACKUP_S3,
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	svc.getClusterByName = func(string) (model.CKManClickHouseConfig, error) {
		return model.CKManClickHouseConfig{Version: "22.8.5.29"}, nil
	}
	p := repo.policies["p1"]
	p.MaxBandwidthMB = 10
	if err := svc.UpdatePolicy(p); err == nil {
		t.Fatal("expected old cluster to be rejected")
	}
	p.MaxBandwidthMB = 0
	if err := svc.UpdatePolicy(p); err != nil {
		t.Fatalf("unlimited should be allowed on old cluster: %v", err)
	}
}
