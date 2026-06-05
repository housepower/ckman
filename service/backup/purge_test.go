package backup

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

func mkRun(id, status, op, prefix, policyID string, parts ...model.BackupRunPartition) model.BackupRun {
	return model.BackupRun{
		RunID: id, PolicyID: policyID, ClusterName: "ckA",
		Database: "dba", Table: "t1", Operation: op,
		Status: status, StoragePrefix: prefix, Partitions: parts,
	}
}

func pt(name, status string) model.BackupRunPartition {
	return model.BackupRunPartition{Partition: name, Status: status}
}

// 同分区 success 散落多条 run 时必须全删,否则老记录复活继续去重;
// run 被删空时连 run 一起删,避免留下 success+0 分区的尸体记录。
func TestDeletePartitionRecords_RemovesAcrossRuns(t *testing.T) {
	repo := newMemRepo()
	// r1: 目标分区 + 其它分区混合 → 只摘目标条目,run 保留
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "ckA", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS),
		pt("20260605", model.BACKUP_PARTITION_STATUS_SUCCESS))
	// r2: 只含目标分区(更老的 run) → 删空后整条删除
	repo.runs["r2"] = mkRun("r2", model.BACKUP_STATUS_FAILED, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"], repo.runs["r2"]}, nil
	}

	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.RemovedRecords != 2 || res.DeletedRuns != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	r1 := repo.runs["r1"]
	if len(r1.Partitions) != 1 || r1.Partitions[0].Partition != "20260605" {
		t.Fatalf("r1 partitions wrong: %+v", r1.Partitions)
	}
	if _, ok := repo.runs["r2"]; ok {
		t.Fatal("r2 should be deleted entirely (all partitions removed)")
	}
	// 删除后该分区必须脱离去重集合
	for _, p := range successfulPartitionsFromRuns([]model.BackupRun{repo.runs["r1"]}) {
		if p.Partition == "20260604" {
			t.Fatal("20260604 still in dedup set")
		}
	}
}

// 有未结束 run 时拒绝:Executor 持有 run 副本,并发 UpdateRun 会把删掉的条目写回。
func TestDeletePartitionRecords_RejectsInFlight(t *testing.T) {
	repo := newMemRepo()
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_RUNNING, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_RUNNING))
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil
	}
	_, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, false)
	if err == nil || !strings.Contains(err.Error(), "in-flight") {
		t.Fatalf("expected in-flight rejection, got %v", err)
	}
}

func TestDeletePartitionRecords_InvalidInput(t *testing.T) {
	s := NewService("self", newMemRepo(), nil)
	if _, err := s.DeletePartitionRecords("ckA", "dba", "t1", nil, false); err == nil {
		t.Fatal("empty partitions should be rejected")
	}
	if _, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"2026'; DROP"}, false); err == nil {
		t.Fatal("invalid identifier should be rejected")
	}
}
