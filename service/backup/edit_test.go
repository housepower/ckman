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

func TestDeletePolicy_SoftDelete(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", Enabled: true}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	if err := svc.DeletePolicy("p1"); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetPolicy("p1")
	if !got.Deleted || got.Enabled {
		t.Fatalf("expected soft delete: %+v", got)
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
