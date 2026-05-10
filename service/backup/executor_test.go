package backup

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

type fakeExecRepo struct {
	*memRepo
	policy model.BackupPolicy
}

func newFakeExecRepo(p model.BackupPolicy) *fakeExecRepo {
	return &fakeExecRepo{memRepo: newMemRepo(), policy: p}
}

func (r *fakeExecRepo) GetPolicyForRun(policyID string) (model.BackupPolicy, error) {
	return r.policy, nil
}

func newFakeShardConn(host string) *shardConn { return &shardConn{host: host} }

func TestExecutor_Init_NoShardConnect(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", ClusterName: "missing"})
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING}

	e := &Executor{
		repo:        repo,
		connFactory: func(string) ([]*shardConn, error) { return nil, errors.New("cluster not found") },
	}
	err := e.Init(context.Background(), "r1")
	if err == nil || !strings.Contains(err.Error(), "cluster not found") {
		t.Fatalf("expected error, got %v", err)
	}
}

func TestExecutor_Init_NoShardReachable(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA"})
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING}

	e := &Executor{
		repo:        repo,
		connFactory: func(string) ([]*shardConn, error) { return []*shardConn{}, nil },
	}
	err := e.Init(context.Background(), "r1")
	if err == nil || !strings.Contains(err.Error(), "no shard") {
		t.Fatalf("expected no shard error, got %v", err)
	}
}

func TestExecutor_Init_PartitionListErrorDoesNotDropList(t *testing.T) {
	// 修 #4：getLastRunPartitions 报错时仍采用 newPartitions
	repo := newFakeExecRepo(model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		BackupType: model.BACKUP_TYPE_DAILY_PARTITION, DaysBefore: 7,
	})
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING}

	called := false
	e := &Executor{
		repo: repo,
		connFactory: func(string) ([]*shardConn, error) {
			return []*shardConn{newFakeShardConn("h1")}, nil
		},
		listPartitions: func(_ *shardConn, db, table, before string) ([]string, error) {
			return []string{"20250508", "20250509"}, nil
		},
		getLastRunPartitions: func(cluster, db, table string) ([]model.BackupRunPartition, error) {
			called = true
			return nil, errors.New("repo down")
		},
	}
	if err := e.Init(context.Background(), "r1"); err != nil {
		t.Fatalf("init: %v", err)
	}
	if !called {
		t.Fatal("getLastRunPartitions not called")
	}
	rn, _ := repo.GetRun("r1")
	if len(rn.Partitions) != 2 {
		t.Fatalf("expected 2 partitions despite repo error, got %d (regression of #4!)", len(rn.Partitions))
	}
}

func TestExecutor_Init_MergesNewPartitionsWithPrev(t *testing.T) {
	// prev 有 20250506 (success)，新枚举 20250506/07/08；合并后应有 3 条，
	// 老的保留 success，新的标 waiting
	repo := newFakeExecRepo(model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		BackupType: model.BACKUP_TYPE_DAILY_PARTITION, DaysBefore: 7,
	})
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING}

	e := &Executor{
		repo: repo,
		connFactory: func(string) ([]*shardConn, error) {
			return []*shardConn{newFakeShardConn("h1")}, nil
		},
		listPartitions: func(_ *shardConn, db, table, before string) ([]string, error) {
			return []string{"20250506", "20250507", "20250508"}, nil
		},
		getLastRunPartitions: func(cluster, db, table string) ([]model.BackupRunPartition, error) {
			return []model.BackupRunPartition{
				{Partition: "20250506", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			}, nil
		},
	}
	if err := e.Init(context.Background(), "r1"); err != nil {
		t.Fatalf("init: %v", err)
	}
	rn, _ := repo.GetRun("r1")
	if len(rn.Partitions) != 3 {
		t.Fatalf("expected 3 merged, got %d", len(rn.Partitions))
	}
	statusByPart := map[string]string{}
	for _, p := range rn.Partitions {
		statusByPart[p.Partition] = p.Status
	}
	if statusByPart["20250506"] != model.BACKUP_PARTITION_STATUS_SUCCESS {
		t.Fatalf("prev success should be retained")
	}
	if statusByPart["20250507"] != model.BACKUP_PARTITION_STATUS_WAITING {
		t.Fatalf("new partitions should be waiting")
	}
}
