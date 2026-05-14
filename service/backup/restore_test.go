package backup

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestSubmitRestore_RejectsMissingSourceRun(t *testing.T) {
	repo := newMemRepo()
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	_, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "missing", Partitions: []string{"p1"}})
	if err == nil {
		t.Fatal("expected error for missing source run")
	}
}

func TestSubmitRestore_RejectsClusterMismatch(t *testing.T) {
	// 修 spec #11
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_SUCCESS,
		ClusterName: "ckA", Database: "d", Table: "t", PolicyID: "p1",
		Partitions: []model.BackupRunPartition{{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS}},
	}
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1"}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	_, err := svc.SubmitRestore("ckB", RestoreRequest{SourceRunID: "src", Partitions: []string{"20250508"}})
	if err == nil || !strings.Contains(err.Error(), "cluster") {
		t.Fatalf("cluster mismatch should fail: %v", err)
	}
}

func TestSubmitRestore_RejectsNonSuccessSourceRun(t *testing.T) {
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_FAILED,
		ClusterName: "ckA",
		Partitions: []model.BackupRunPartition{{Partition: "p1", Status: model.BACKUP_PARTITION_STATUS_SUCCESS}},
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	_, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "src", Partitions: []string{"p1"}})
	if err == nil {
		t.Fatal("non-success source should reject")
	}
}

func TestSubmitRestore_RejectsUnsuccessfulPartition(t *testing.T) {
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_SUCCESS,
		ClusterName: "ckA",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_FAILED},
		},
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	_, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "src", Partitions: []string{"20250508"}})
	if err == nil {
		t.Fatal("failed partition should reject")
	}
}

func TestSubmitRestore_RejectsEmptyPartitions(t *testing.T) {
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_SUCCESS, ClusterName: "ckA",
		Partitions: []model.BackupRunPartition{{Partition: "p", Status: model.BACKUP_PARTITION_STATUS_SUCCESS}},
	}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	_, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "src"})
	if err == nil {
		t.Fatal("empty partitions should reject")
	}
}

func TestSubmitRestore_HappyPath(t *testing.T) {
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_SUCCESS,
		ClusterName: "ckA", Database: "d", Table: "t", PolicyID: "p1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS, Size: 100, Rows: 10, FileNum: 5},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS, Size: 200, Rows: 20, FileNum: 8},
		},
	}
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1"}
	pool := &fakePool{}
	svc := newServiceForTest("ckman-01", repo, pool)
	runID, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "src", Partitions: []string{"20250508"}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.Operation != model.OP_RESTORE {
		t.Fatalf("op: %s", rn.Operation)
	}
	if rn.PolicyID != "p1" {
		t.Fatalf("policy_id: %s", rn.PolicyID)
	}
	if rn.ClusterName != "ckA" || rn.Database != "d" || rn.Table != "t" {
		t.Fatalf("cluster/db/table not copied: %+v", rn)
	}
	if rn.TriggerType != model.TRIGGER_MANUAL_RESTORE {
		t.Fatalf("trigger: %s", rn.TriggerType)
	}
	if len(rn.Partitions) != 1 || rn.Partitions[0].Partition != "20250508" {
		t.Fatalf("partitions: %+v", rn.Partitions)
	}
	if rn.Partitions[0].Status != model.BACKUP_PARTITION_STATUS_WAITING {
		t.Fatalf("partition status should reset: %s", rn.Partitions[0].Status)
	}
	// 复制 size/rows/fileNum
	if rn.Partitions[0].Size != 200 || rn.Partitions[0].Rows != 20 || rn.Partitions[0].FileNum != 8 {
		t.Fatalf("size/rows/fileNum not copied: %+v", rn.Partitions[0])
	}
	if len(pool.in) != 1 {
		t.Fatalf("pool not enqueued: %v", pool.in)
	}
}

func TestSubmitRestore_StoragePrefix_PropagatedFromSource(t *testing.T) {
	// 新备份的 source run 带 StoragePrefix=cluster；restore 新 run 必须透传，不能用当前 cluster 名重算。
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_SUCCESS,
		ClusterName: "ckA", Database: "d", Table: "t", PolicyID: "p1",
		StoragePrefix: "ckA",
		Partitions:    []model.BackupRunPartition{{Partition: "p1", Status: model.BACKUP_PARTITION_STATUS_SUCCESS}},
	}
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1"}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	runID, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "src", Partitions: []string{"p1"}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.StoragePrefix != "ckA" {
		t.Fatalf("storage_prefix not propagated: got %q want %q", rn.StoragePrefix, "ckA")
	}
}

func TestSubmitRestore_StoragePrefix_LegacyRunStaysEmpty(t *testing.T) {
	// 老备份 source.StoragePrefix=""，restore 新 run 也必须为空（否则会去新路径找不到老对象）。
	repo := newMemRepo()
	repo.runs["src"] = model.BackupRun{
		RunID: "src", Status: model.BACKUP_STATUS_SUCCESS,
		ClusterName: "ckA", Database: "d", Table: "t", PolicyID: "p1",
		// StoragePrefix 留空 = 老备份
		Partitions: []model.BackupRunPartition{{Partition: "p1", Status: model.BACKUP_PARTITION_STATUS_SUCCESS}},
	}
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1"}
	svc := newServiceForTest("ckman-01", repo, &fakePool{})
	runID, err := svc.SubmitRestore("ckA", RestoreRequest{SourceRunID: "src", Partitions: []string{"p1"}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.StoragePrefix != "" {
		t.Fatalf("legacy source run should result in empty storage_prefix, got %q", rn.StoragePrefix)
	}
}
