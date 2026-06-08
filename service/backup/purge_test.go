package backup

import (
	"errors"
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

// 独立验证守卫(2)的兜底能力:即便 getRunsByTable 因任何原因(实现差异、未来回退等)
// 未返回 queued run,InFlightRunsByCluster 仍能拦截。
// 生产全历史查询(days=0)下守卫(1)其实已能看到零值 StartedAt 的 queued run;
// 此测试通过让注入的 getRunsByTable 故意不返回 rq,单独验证守卫(2)的存在价值。
func TestDeletePartitionRecords_RejectsQueuedInvisibleToTableQuery(t *testing.T) {
	repo := newMemRepo()
	// queued run 只进 repo(可被 InFlightRunsByCluster 看到),注入的 getRunsByTable 故意不返回它
	repo.runs["rq"] = mkRun("rq", model.BACKUP_STATUS_QUEUED, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_WAITING))
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil // 守卫(1)看不见 rq;守卫(2)兜底
	}
	_, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, false)
	if err == nil || !strings.Contains(err.Error(), "in-flight") {
		t.Fatalf("expected queued-run rejection, got %v", err)
	}
}

// hostRecordingStorage 记录 CleanPartition 的 host+key 配对,验证 host 参数正确分发。
type hostRecordingStorage struct {
	fakeStorage
	calls []string // "host|key"
}

func (h *hostRecordingStorage) CleanPartition(host, keyPrefix string) error {
	h.calls = append(h.calls, host+"|"+keyPrefix)
	return h.fakeStorage.CleanPartition(host, keyPrefix)
}

// cleanRemote=true 时按 run 的 policy 组装 storage,对全部副本 host 清 key。
// 备份 key 含执行当时的 replica host 而 run 未记录,故全副本清理(幂等)。
// hostRecordingStorage 额外验证 CleanPartition 收到的 host 参数与 key 尾段一致。
func TestDeletePartitionRecords_CleanRemote(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", TargetType: model.BACKUP_S3}
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "ckA", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	fs := &hostRecordingStorage{}
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil
	}
	s.getClusterByName = func(name string) (model.CKManClickHouseConfig, error) {
		return model.CKManClickHouseConfig{Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "h1"}, {Ip: "h2"}}},
		}}, nil
	}
	s.storageFactory = func(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage { return fs }

	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, true)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("unexpected warnings: %v", res.Warnings)
	}
	want := map[string]bool{
		"ckA/20260604/dba.t1/h1": true,
		"ckA/20260604/dba.t1/h2": true,
	}
	if len(fs.cleaned) != 2 {
		t.Fatalf("cleaned=%v", fs.cleaned)
	}
	for _, k := range fs.cleaned {
		if !want[k] {
			t.Fatalf("unexpected clean key %s", k)
		}
	}
	// 验证 host 参数分发正确:key 尾段 host 与实际传参 host 必须一致。
	wantCalls := map[string]bool{
		"h1|ckA/20260604/dba.t1/h1": true,
		"h2|ckA/20260604/dba.t1/h2": true,
	}
	if len(fs.calls) != 2 {
		t.Fatalf("calls=%v", fs.calls)
	}
	for _, c := range fs.calls {
		if !wantCalls[c] {
			t.Fatalf("unexpected host|key call %s", c)
		}
	}
}

// 清理失败只记 warning,记录照删(下次重备 Prepare 还会再清一遍,有兜底)。
func TestDeletePartitionRecords_CleanFailureStillRemovesRecords(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", TargetType: model.BACKUP_S3}
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	fs := &fakeStorage{cleanErr: errors.New("s3 down")}
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil
	}
	s.getClusterByName = func(name string) (model.CKManClickHouseConfig, error) {
		return model.CKManClickHouseConfig{Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "h1"}}},
		}}, nil
	}
	s.storageFactory = func(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage { return fs }

	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, true)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected clean warnings")
	}
	if _, ok := repo.runs["r1"]; ok {
		t.Fatal("records should be removed despite clean failure")
	}
}

// policy 不存在(migrated 老 run):记录照删,远端清理跳过并告警。
func TestDeletePartitionRecords_PolicyMissingWarns(t *testing.T) {
	repo := newMemRepo() // 不注册 p1
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil
	}
	s.getClusterByName = func(name string) (model.CKManClickHouseConfig, error) {
		return model.CKManClickHouseConfig{Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "h1"}}},
		}}, nil
	}

	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, true)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	found := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "policy") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected policy-missing warning, got %v", res.Warnings)
	}
	if _, ok := repo.runs["r1"]; ok {
		t.Fatal("records should be removed despite missing policy")
	}
}

type failingDeleteRepo struct {
	*memRepo
	failDelete bool
}

func (r *failingDeleteRepo) DeleteRun(id string) error {
	if r.failDelete {
		return errors.New("disk full")
	}
	return r.memRepo.DeleteRun(id)
}

// DeleteRun 失败时该 run 的条目并没有真正删除,RemovedRecords 不得虚报。
func TestDeletePartitionRecords_DeleteFailureNotCounted(t *testing.T) {
	repo := &failingDeleteRepo{memRepo: newMemRepo(), failDelete: true}
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil
	}
	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.RemovedRecords != 0 || res.DeletedRuns != 0 {
		t.Fatalf("counts must reflect actual deletions: %+v", res)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected warning")
	}
}

// 删除应回看全部历史(sinceDays<=0),否则滚出窗口的老 success 记录删不到、又会被重备。
func TestDeletePartitionRecords_PassesUnlimitedWindow(t *testing.T) {
	repo := newMemRepo()
	gotDays := 999
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		gotDays = days
		return nil, nil
	}
	_, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if gotDays > 0 {
		t.Fatalf("expected unlimited window (<=0), got days=%d", gotDays)
	}
}

// 删除分区记录会一并删掉该分区的 restore 条目(让分区从列表彻底消失),
// 但 restore 没有独立的远端备份数据,不触发远端清理。
func TestDeletePartitionRecords_RemovesRestoreEntriesNoRemoteClean(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", TargetType: model.BACKUP_S3}
	repo.runs["rb"] = mkRun("rb", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "ckA", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	repo.runs["rr"] = mkRun("rr", model.BACKUP_STATUS_SUCCESS, model.OP_RESTORE, "ckA", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	fs := &fakeStorage{}
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["rb"], repo.runs["rr"]}, nil
	}
	s.getClusterByName = func(name string) (model.CKManClickHouseConfig, error) {
		return model.CKManClickHouseConfig{Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "h1"}}},
		}}, nil
	}
	s.storageFactory = func(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage { return fs }

	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, true)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	// backup + restore 两条都删,分区彻底消失
	if res.RemovedRecords != 2 {
		t.Fatalf("expected 2 removed (backup + restore), got %d", res.RemovedRecords)
	}
	if _, ok := repo.runs["rb"]; ok {
		t.Fatal("backup run should be deleted (emptied)")
	}
	if _, ok := repo.runs["rr"]; ok {
		t.Fatal("restore run should be deleted (emptied)")
	}
	// 远端只清 backup 对应的 1 个 key;restore 不进远端清理
	if len(fs.cleaned) != 1 {
		t.Fatalf("expected 1 remote clean (backup only), got %v", fs.cleaned)
	}
}

type failingUpdateRepo struct {
	*memRepo
	failUpdate bool
}

func (r *failingUpdateRepo) UpdateRun(rn model.BackupRun) error {
	if r.failUpdate {
		return errors.New("disk full")
	}
	return r.memRepo.UpdateRun(rn)
}

// 台账 UpdateRun 失败时,该 run 的分区不得被清远端——否则 success 记录仍在、
// 去重仍跳过,远端却被删,形成不可恢复的「记录指向空对象」。
func TestDeletePartitionRecords_NoRemoteCleanWhenLedgerFails(t *testing.T) {
	repo := &failingUpdateRepo{memRepo: newMemRepo(), failUpdate: true}
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", TargetType: model.BACKUP_S3}
	// kept 非空(20260605 不删)→ 走 UpdateRun 路径,且 UpdateRun 被注入失败
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "ckA", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS),
		pt("20260605", model.BACKUP_PARTITION_STATUS_SUCCESS))
	fs := &fakeStorage{}
	s := NewService("self", repo, nil)
	s.getRunsByTable = func(cluster, db, table string, days int) ([]model.BackupRun, error) {
		return []model.BackupRun{repo.runs["r1"]}, nil
	}
	s.getClusterByName = func(name string) (model.CKManClickHouseConfig, error) {
		return model.CKManClickHouseConfig{Shards: []model.CkShard{
			{Replicas: []model.CkReplica{{Ip: "h1"}}},
		}}, nil
	}
	s.storageFactory = func(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage { return fs }

	res, err := s.DeletePartitionRecords("ckA", "dba", "t1", []string{"20260604"}, true)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if res.RemovedRecords != 0 {
		t.Fatalf("ledger update failed, RemovedRecords must be 0, got %d", res.RemovedRecords)
	}
	if len(fs.cleaned) != 0 {
		t.Fatalf("ledger failed → remote must NOT be cleaned, got %v", fs.cleaned)
	}
	if len(res.Warnings) == 0 {
		t.Fatal("expected update-failure warning")
	}
}
