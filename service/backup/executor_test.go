package backup

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

// ── Prepare-stage test helpers ────────────────────────────────────────────────

type fakeQueryRows struct{ closeFn func() }

func (f *fakeQueryRows) Close() {
	if f.closeFn != nil {
		f.closeFn()
	}
}

type fakeStorage struct {
	cleanErr error
	cleaned  []string
}

func (f *fakeStorage) Init() error                                                         { return nil }
func (f *fakeStorage) BackupSQL(d, t, p, k string) string                                  { return "" }
func (f *fakeStorage) RestoreSQL(d, t, p, k string) string                                 { return "" }
func (f *fakeStorage) CleanPartition(d, t, h, p string) error {
	f.cleaned = append(f.cleaned, p)
	return f.cleanErr
}
func (f *fakeStorage) CheckPartition(h, d, t, p string, _ map[string]model.PathInfo) error { return nil }
func (f *fakeStorage) Type() string                                                         { return "fake" }

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

// ── Prepare stage tests ───────────────────────────────────────────────────────

func TestRealStages_Prepare_ChecksumErrorAggregated(t *testing.T) {
	// 3 host 跑 md5sum，h2 失败；errgroup 应收集错误（修 #5）
	// 即便有错误，已 query 成功的 rows 也必须被 close（修 rows.Close 泄漏）
	closed := map[string]bool{}
	conns := []*shardConn{newFakeShardConn("h1"), newFakeShardConn("h2"), newFakeShardConn("h3")}
	queryFn := func(host string) (queryResult, error) {
		if host == "h2" {
			return nil, errors.New("h2 down")
		}
		return &fakeQueryRows{closeFn: func() { closed[host] = true }}, nil
	}
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", Checksum: true})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{
		repo: repo, conns: conns, queryRows: queryFn,
		storage: &fakeStorage{},
	}
	err := realStages{}.Prepare(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "h2 down") {
		t.Fatalf("expected error containing 'h2 down', got %v", err)
	}
	// 已 query 成功的 conn 必须 close
	if !closed["h1"] || !closed["h3"] {
		t.Fatalf("rows.Close should be called on successful conns, got %+v", closed)
	}
}

func TestRealStages_Prepare_CleanFailureFailsRun(t *testing.T) {
	// storage.CleanPartition 失败 → 整体 run failed
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", Checksum: false})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{
		repo:  repo,
		conns: []*shardConn{newFakeShardConn("h1")},
		queryRows: func(string) (queryResult, error) {
			return &fakeQueryRows{}, nil
		},
		storage: &fakeStorage{cleanErr: errors.New("rm permission denied")},
	}
	err := realStages{}.Prepare(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "rm permission denied") {
		t.Fatalf("clean failure should fail run: %v", err)
	}
}

func TestRealStages_Prepare_NoChecksumSkipsMd5(t *testing.T) {
	// Checksum=false 时不应 call queryRows
	queryCalled := false
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", Checksum: false})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{
		repo:  repo,
		conns: []*shardConn{newFakeShardConn("h1")},
		queryRows: func(string) (queryResult, error) {
			queryCalled = true
			return &fakeQueryRows{}, nil
		},
		storage: &fakeStorage{},
	}
	if err := (realStages{}).Prepare(context.Background(), e, "r1"); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if queryCalled {
		t.Fatal("queryRows should not be called when Checksum=false")
	}
}
