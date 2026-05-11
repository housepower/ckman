package backup

import (
	"testing"

	"github.com/housepower/ckman/model"
)

// TestSubmitBackupRequest_SharedTaskID verifies that N tables submitted together
// all receive the same TaskID.
func TestSubmitBackupRequest_SharedTaskID(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	req := model.BackupRequest{
		ScheduleType: model.BACKUP_IMMEDIATE,
		Database:     "dba",
		Tables:       []string{"t1", "t2", "t3"},
		Target:       model.BACKUP_LOCAL,
		Instance:     "ckman-01",
	}
	runIDs, err := svc.SubmitBackupRequest("ckA", req)
	if err != nil {
		t.Fatalf("SubmitBackupRequest: %v", err)
	}
	if len(runIDs) != 3 {
		t.Fatalf("expected 3 runIDs, got %d", len(runIDs))
	}
	if len(repo.policies) != 3 {
		t.Fatalf("expected 3 policies, got %d", len(repo.policies))
	}

	// All policies must share the same non-empty TaskID.
	taskIDs := map[string]struct{}{}
	for _, p := range repo.policies {
		if p.TaskID == "" {
			t.Errorf("policy %s has empty TaskID", p.PolicyID)
		}
		taskIDs[p.TaskID] = struct{}{}
	}
	if len(taskIDs) != 1 {
		t.Fatalf("expected all policies to share 1 TaskID, got %d distinct IDs: %v", len(taskIDs), taskIDs)
	}
}

// TestSubmitBackupRequest_DefaultTaskNameSingleTable verifies that when the user
// omits TaskName and only one table is submitted, the generated name is "db.table".
func TestSubmitBackupRequest_DefaultTaskNameSingleTable(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	req := model.BackupRequest{
		ScheduleType: model.BACKUP_IMMEDIATE,
		Database:     "dba",
		Tables:       []string{"t1"},
		Target:       model.BACKUP_LOCAL,
		Instance:     "ckman-01",
	}
	if _, err := svc.SubmitBackupRequest("ckA", req); err != nil {
		t.Fatalf("SubmitBackupRequest: %v", err)
	}
	if len(repo.policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(repo.policies))
	}
	var p model.BackupPolicy
	for _, v := range repo.policies {
		p = v
	}
	want := "dba.t1"
	if p.TaskName != want {
		t.Errorf("expected TaskName=%q, got %q", want, p.TaskName)
	}
}

// TestSubmitBackupRequest_DefaultTaskNameMultiTable verifies that the generated
// name is "db.tables[0] (+N more)" when N > 1 tables are submitted.
func TestSubmitBackupRequest_DefaultTaskNameMultiTable(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	req := model.BackupRequest{
		ScheduleType: model.BACKUP_IMMEDIATE,
		Database:     "dba",
		Tables:       []string{"t1", "t2", "t3"},
		Target:       model.BACKUP_LOCAL,
		Instance:     "ckman-01",
	}
	if _, err := svc.SubmitBackupRequest("ckA", req); err != nil {
		t.Fatalf("SubmitBackupRequest: %v", err)
	}
	want := "dba.t1 (+2 more)"
	for _, p := range repo.policies {
		if p.TaskName != want {
			t.Errorf("expected TaskName=%q, got %q", want, p.TaskName)
		}
	}
}

// TestSubmitBackupRequest_UserProvidedTaskName verifies that a user-supplied
// TaskName is passed through unchanged to all policies.
func TestSubmitBackupRequest_UserProvidedTaskName(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	req := model.BackupRequest{
		ScheduleType: model.BACKUP_IMMEDIATE,
		TaskName:     "my-custom-task",
		Database:     "dba",
		Tables:       []string{"t1", "t2"},
		Target:       model.BACKUP_LOCAL,
		Instance:     "ckman-01",
	}
	if _, err := svc.SubmitBackupRequest("ckA", req); err != nil {
		t.Fatalf("SubmitBackupRequest: %v", err)
	}
	for _, p := range repo.policies {
		if p.TaskName != "my-custom-task" {
			t.Errorf("expected TaskName=%q, got %q", "my-custom-task", p.TaskName)
		}
	}
}

// TestSubmitBackupRequest_ScheduledReturnsNoRunIDs verifies that a scheduled
// backup produces policies but no runIDs (scheduler triggers later).
func TestSubmitBackupRequest_ScheduledReturnsNoRunIDs(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)

	req := model.BackupRequest{
		ScheduleType: model.BACKUP_SCHEDULED,
		Crontab:      "0 3 * * *",
		Database:     "dba",
		Tables:       []string{"t1", "t2"},
		Target:       model.BACKUP_LOCAL,
		Instance:     "ckman-01",
	}
	runIDs, err := svc.SubmitBackupRequest("ckA", req)
	if err != nil {
		t.Fatalf("SubmitBackupRequest: %v", err)
	}
	if len(runIDs) != 0 {
		t.Fatalf("scheduled backup should return no runIDs, got %v", runIDs)
	}
	// Policies should still share a TaskID.
	taskIDs := map[string]struct{}{}
	for _, p := range repo.policies {
		taskIDs[p.TaskID] = struct{}{}
	}
	if len(taskIDs) != 1 {
		t.Fatalf("expected 1 shared TaskID, got %d", len(taskIDs))
	}
}
