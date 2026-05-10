package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

// ── Prepare-stage test helpers ────────────────────────────────────────────────

type fakeQueryRows struct{ closeFn func() }

func (f *fakeQueryRows) Close() error {
	if f.closeFn != nil {
		f.closeFn()
	}
	return nil
}

type fakeStorage struct {
	cleanErr error
	cleaned  []string
}

func (f *fakeStorage) Init() error                                                         { return nil }
func (f *fakeStorage) BackupSQL(d, t, p, k string) string                                  { return fmt.Sprintf(" PARTITION '%s'", p) }
func (f *fakeStorage) RestoreSQL(d, t, p, k string) string                                 { return fmt.Sprintf(" PARTITION '%s'", p) }
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

// ── Backup stage tests ────────────────────────────────────────────────────────

func TestRealStages_Backup_PartitionIsolation(t *testing.T) {
	// 3 partition，第 2 个 BACKUP TABLE 失败；其余继续；run 整体 failed
	conns := []*shardConn{newFakeShardConn("h1")}
	execCalls := []string{}
	execFn := func(host, sql string) error {
		execCalls = append(execCalls, sql)
		if strings.Contains(sql, "20250508") {
			return errors.New("backup engine OOM")
		}
		return nil
	}
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_WAITING},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{
		repo:    repo,
		conns:   conns,
		execSQL: execFn,
		storage: &fakeStorage{},
	}
	err := realStages{}.Backup(context.Background(), e, "r1")
	if err == nil {
		t.Fatal("expected error since one partition failed")
	}
	got, _ := repo.GetRun("r1")
	want := []string{
		model.BACKUP_PARTITION_STATUS_SUCCESS,
		model.BACKUP_PARTITION_STATUS_FAILED,
		model.BACKUP_PARTITION_STATUS_SUCCESS,
	}
	for i, w := range want {
		if got.Partitions[i].Status != w {
			t.Fatalf("partition[%d] status got=%s want=%s", i, got.Partitions[i].Status, w)
		}
	}
	if len(execCalls) != 3 {
		t.Fatalf("expected 3 BACKUP attempts (no traction), got %d", len(execCalls))
	}
	// 失败 partition 必须有 Msg
	if got.Partitions[1].Msg == "" {
		t.Fatal("failed partition should have Msg")
	}
}

func TestRealStages_Backup_AllSuccess(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "d", Table: "t",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{
		repo:    repo,
		conns:   []*shardConn{newFakeShardConn("h1")},
		execSQL: func(host, sql string) error { return nil },
		storage: &fakeStorage{},
	}
	if err := (realStages{}).Backup(context.Background(), e, "r1"); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	got, _ := repo.GetRun("r1")
	if got.Partitions[0].Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
		t.Fatalf("status: %s", got.Partitions[0].Status)
	}
}

func TestRealStages_Backup_SkipsNonWaiting(t *testing.T) {
	// 已是 SUCCESS 的不重试
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	called := false
	e := &Executor{
		repo:    repo,
		conns:   []*shardConn{newFakeShardConn("h1")},
		execSQL: func(string, string) error { called = true; return nil },
		storage: &fakeStorage{},
	}
	_ = realStages{}.Backup(context.Background(), e, "r1")
	if called {
		t.Fatal("non-waiting partition should be skipped")
	}
}

// ── Restore stage tests ───────────────────────────────────────────────────────

func TestRealStages_Restore_PartitionIsolation(t *testing.T) {
	conns := []*shardConn{newFakeShardConn("h1")}
	calls := []string{}
	execFn := func(_ string, sql string) error {
		calls = append(calls, sql)
		if strings.Contains(sql, "20250508") {
			return errors.New("restore failed")
		}
		return nil
	}
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", Operation: model.OP_RESTORE,
		Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_WAITING},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{repo: repo, conns: conns, execSQL: execFn, storage: &fakeStorage{}}
	err := realStages{}.Restore(context.Background(), e, "r1")
	if err == nil {
		t.Fatal("expected fail")
	}
	for _, sql := range calls {
		if !strings.Contains(sql, "RESTORE TABLE") {
			t.Fatalf("expected RESTORE TABLE: %s", sql)
		}
		if !strings.Contains(sql, "allow_non_empty_tables=true") {
			t.Fatalf("expected SETTINGS allow_non_empty_tables=true: %s", sql)
		}
	}
	got, _ := repo.GetRun("r1")
	if got.Partitions[0].Status != model.BACKUP_PARTITION_STATUS_SUCCESS {
		t.Fatalf("first partition should succeed: %+v", got.Partitions[0])
	}
	if got.Partitions[1].Status != model.BACKUP_PARTITION_STATUS_FAILED {
		t.Fatalf("second partition should fail: %+v", got.Partitions[1])
	}
}

func TestRealStages_Restore_SkipsNonWaiting(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", Operation: model.OP_RESTORE,
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	called := false
	e := &Executor{
		repo: repo, conns: []*shardConn{newFakeShardConn("h1")},
		execSQL: func(string, string) error { called = true; return nil },
		storage: &fakeStorage{},
	}
	_ = realStages{}.Restore(context.Background(), e, "r1")
	if called {
		t.Fatal("non-waiting partition should be skipped")
	}
}

// ── Check stage tests ─────────────────────────────────────────────────────────

type checksumStorage struct {
	check func(host, db, table, partition string, pi map[string]model.PathInfo) error
}

func (s *checksumStorage) Init() error                                      { return nil }
func (s *checksumStorage) BackupSQL(_, _, _, _ string) string               { return "" }
func (s *checksumStorage) RestoreSQL(_, _, _, _ string) string              { return "" }
func (s *checksumStorage) CleanPartition(_, _, _, _ string) error           { return nil }
func (s *checksumStorage) CheckPartition(host, db, table, partition string, pi map[string]model.PathInfo) error {
	return s.check(host, db, table, partition, pi)
}
func (s *checksumStorage) Type() string { return "fake" }

func TestRealStages_Check_AllPartitionsValidated(t *testing.T) {
	// 修 #3：3 个 success partition 全要被校验，旧版 return 写在 for 内只校验第一个
	checked := map[string]int{}
	storage := &checksumStorage{
		check: func(host, db, table, p string, _ map[string]model.PathInfo) error {
			checked[p]++
			return nil
		},
	}
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	e := &Executor{
		repo:    repo,
		conns:   []*shardConn{newFakeShardConn("h1")},
		storage: storage,
	}
	if err := (realStages{}).Check(context.Background(), e, "r1"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(checked) != 3 {
		t.Fatalf("expected 3 partitions checked, got %d (regression of #3!)", len(checked))
	}
}

func TestRealStages_Check_FirstFailureRetainedButContinues(t *testing.T) {
	// p2 失败，p1 / p3 仍校验。返回首个错。
	count := 0
	storage := &checksumStorage{
		check: func(_, _, _, p string, _ map[string]model.PathInfo) error {
			count++
			if p == "20250508" {
				return errors.New("md5 mismatch")
			}
			return nil
		},
	}
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", Database: "d", Table: "t",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	e := &Executor{repo: repo, conns: []*shardConn{newFakeShardConn("h1")}, storage: storage}
	err := (realStages{}).Check(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "md5 mismatch") {
		t.Fatalf("expected md5 mismatch, got %v", err)
	}
	if count != 3 {
		t.Fatalf("expected all 3 checked despite failure, got %d", count)
	}
}

func TestRealStages_Check_SkipsNonSuccess(t *testing.T) {
	// 仅校验 SUCCESS 状态的 partition
	called := 0
	storage := &checksumStorage{check: func(_, _, _, _ string, _ map[string]model.PathInfo) error { called++; return nil }}
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1"})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1",
		Partitions: []model.BackupRunPartition{
			{Partition: "p1", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "p2", Status: model.BACKUP_PARTITION_STATUS_FAILED},
			{Partition: "p3", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{repo: repo, conns: []*shardConn{newFakeShardConn("h1")}, storage: storage}
	_ = realStages{}.Check(context.Background(), e, "r1")
	if called != 1 {
		t.Fatalf("expected only 1 success partition checked, got %d", called)
	}
}

// ── Close stage tests ─────────────────────────────────────────────────────────

func TestRealStages_Close_DropPartitionFailureFailsRun(t *testing.T) {
	// Clean=true 时 DROP PARTITION 失败 → 整体 run failed（修 #7）
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", Clean: true})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "d", Table: "t",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	e := &Executor{
		repo: repo, conns: []*shardConn{newFakeShardConn("h1")},
		execSQL: func(_, sql string) error {
			if strings.HasPrefix(sql, "ALTER TABLE") {
				return errors.New("permission denied")
			}
			return nil
		},
	}
	err := realStages{}.Close(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "cleanup_failed") {
		t.Fatalf("DROP PARTITION failure must surface, got %v", err)
	}
}

func TestRealStages_Close_NoCleanReturnsNil(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", Clean: false})
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1"}
	e := &Executor{
		repo:    repo,
		conns:   []*shardConn{newFakeShardConn("h1")},
		execSQL: func(_, _ string) error { return nil },
	}
	if err := (realStages{}).Close(context.Background(), e, "r1"); err != nil {
		t.Fatalf("Clean=false should be no-op: %v", err)
	}
}

func TestRealStages_Close_OnlySuccessPartitionsDropped(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{PolicyID: "p1", Clean: true})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "d", Table: "t",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_FAILED},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	dropped := []string{}
	e := &Executor{
		repo: repo, conns: []*shardConn{newFakeShardConn("h1")},
		execSQL: func(_, sql string) error {
			if strings.HasPrefix(sql, "ALTER TABLE") {
				dropped = append(dropped, sql)
			}
			return nil
		},
	}
	if err := (realStages{}).Close(context.Background(), e, "r1"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if len(dropped) != 1 {
		t.Fatalf("only success partition should be dropped, got %d: %v", len(dropped), dropped)
	}
	if !strings.Contains(dropped[0], "20250507") {
		t.Fatalf("dropped wrong partition: %s", dropped[0])
	}
}
