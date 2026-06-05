# 备份分区记录管理与空分区 run 修复 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 空分区 run 标 skipped+no_partitions;新增"删除分区备份记录"API+UI(可选清理远端数据);立即备份反转窗口拦截;修复 toDate 误判 daily 兼容。

**Architecture:** 后端改动集中在 `service/backup` 包(Executor 早退、Service 新方法 purge.go、validate 增强、daily_compat 正则);API 走既有 controller/router v1 模式。前端改动在 `../ckman-fe`(git 子模块的源仓库,**不要在 ckman/frontend 里改**):partition-list-dialog 增删除入口、backup-form-dialog 增阻断规则。

**Tech Stack:** Go 1.24 + Gin(后端);Vue2 + element-ui(前端);spec 见 `docs/superpowers/specs/2026-06-05-backup-partition-records-design.md`。

**约定:**
- 后端测试命令统一:`cd /data/root/go/src/github.com/housepower/ckman && go test ./service/backup/ -run <TestName> -v -count=1`
- 后端提交在 ckman 仓库 main 分支;前端提交在 `/data/root/go/src/github.com/housepower/ckman-fe` main 分支
- commit message 结尾加 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

### Task 1: 空分区 run → skipped + no_partitions

**Files:**
- Modify: `model/data_manage.go`(REASON_* 常量块,约 151-156 行)
- Modify: `service/backup/executor.go`(Run 方法约 86-118 行;markSuccess 附近加 helper)
- Test: `service/backup/executor_test.go`

- [ ] **Step 1: 添加常量(纯声明,让测试可编译)**

在 `model/data_manage.go` 的 REASON_ 常量块(`REASON_INST_CHANGED` 之后)加:

```go
	REASON_NO_PARTITIONS = "no_partitions" // 本次 run 无分区可备份(窗口空/全部已去重)
```

- [ ] **Step 2: 写失败测试**

在 `service/backup/executor_test.go` 末尾(`TestResolveDailyPartitionRange_AnchoredToTriggerTime` 之后)加:

```go
// recordingStages 记录各阶段调用,用于断言空分区 run 不进入任何阶段。
type recordingStages struct{ called []string }

func (s *recordingStages) Prepare(context.Context, *Executor, string) error {
	s.called = append(s.called, "prepare")
	return nil
}
func (s *recordingStages) Backup(context.Context, *Executor, string) error {
	s.called = append(s.called, "backup")
	return nil
}
func (s *recordingStages) Restore(context.Context, *Executor, string) error {
	s.called = append(s.called, "restore")
	return nil
}
func (s *recordingStages) Check(context.Context, *Executor, string) error {
	s.called = append(s.called, "check")
	return nil
}
func (s *recordingStages) Close(context.Context, *Executor, string) error {
	s.called = append(s.called, "close")
	return nil
}

// 窗口反转/无新分区时 run 应标 skipped + no_partitions,不进任何 stage,
// 不能像现在一样空跑后假装 success(与真备份成功无法区分)。
func TestExecutor_Run_EmptyPartitions_SkippedNoPartitions(t *testing.T) {
	repo := newFakeExecRepo(model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		BackupStyle: model.BACKUP_STYLE_INCR,
		BackupType:  model.BACKUP_TYPE_DAILY_PARTITION,
		StartDate:   "20260604", DaysBefore: 1,
	})
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Operation: model.OP_BACKUP,
		Status:     model.BACKUP_STATUS_RUNNING,
		CreateTime: time.Date(2026, 6, 4, 1, 0, 0, 0, time.Local), // 当天触发,窗口反转
	}
	st := &recordingStages{}
	e := &Executor{
		repo:   repo,
		stages: st,
		connFactory: func(string) ([]*shardConn, error) {
			return []*shardConn{newFakeShardConn("h1")}, nil
		},
		listPartitions: func(_ *shardConn, db, table, from, to string) ([]string, error) {
			return nil, nil // 反转窗口扫不到任何分区
		},
	}
	if err := e.Run(context.Background(), "r1"); err != nil {
		t.Fatalf("run: %v", err)
	}
	rn, _ := repo.GetRun("r1")
	if rn.Status != model.BACKUP_STATUS_SKIPPED {
		t.Fatalf("expected skipped, got %s", rn.Status)
	}
	if rn.StatusReason != model.REASON_NO_PARTITIONS {
		t.Fatalf("expected no_partitions, got %q", rn.StatusReason)
	}
	if rn.FinishedAt.IsZero() {
		t.Fatal("FinishedAt not set")
	}
	if len(st.called) != 0 {
		t.Fatalf("stages should not run for empty-partition run, got %v", st.called)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `go test ./service/backup/ -run TestExecutor_Run_EmptyPartitions -v -count=1`
Expected: FAIL,`expected skipped, got success`(当前空分区照常 markSuccess)

- [ ] **Step 4: 最小实现**

`service/backup/executor.go` Run 方法,在 `r, _ := e.repo.GetRun(runID)` 与 `st := e.resolveStages()` 之间插入:

```go
	// 空分区:窗口内无分区 / 全部已去重 → skipped(no_partitions),不进任何阶段,
	// 避免产生「success + 0 分区」的误导记录(无法与真备份成功区分)。
	if r.Operation == model.OP_BACKUP && len(r.Partitions) == 0 {
		return e.markSkippedNoPartitions(runID)
	}
```

在 `markSuccess` 之后加 helper:

```go
// markSkippedNoPartitions 把无分区可备份的 run 收口为 skipped 终态。
// 不是错误:表没新数据/窗口暂时为空是正常情形,返回 nil 让 worker 正常收尾。
func (e *Executor) markSkippedNoPartitions(runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	r.Status = model.BACKUP_STATUS_SKIPPED
	r.StatusReason = model.REASON_NO_PARTITIONS
	r.FinishedAt = e.clock()
	r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	return e.repo.UpdateRun(r)
}
```

- [ ] **Step 5: 跑测试确认通过 + 包级回归**

Run: `go test ./service/backup/ -v -count=1 2>&1 | tail -20`
Expected: 全部 PASS(尤其 `TestExecutor_Init_FullBackup_NoPartitions` 行为不变——full 模式 0 分区仍在 Init 报错)

- [ ] **Step 6: Commit**

```bash
git add model/data_manage.go service/backup/executor.go service/backup/executor_test.go
git commit -m "fix(backup): 空分区 run 标 skipped+no_partitions,不再伪装 success"
```

---

### Task 2: 立即备份反转窗口后端拦截

**Files:**
- Modify: `service/backup/validate.go:95-103`(validateDailyRange 滚动窗口分支)
- Test: `service/backup/validate_test.go`

- [ ] **Step 1: 写失败测试**

在 `service/backup/validate_test.go` 末尾加(日期用 time.Now 动态生成,避免日期跨天脆弱):

```go
// 立即备份 + 滚动窗口反转(start_date > 今天−days_before)时本次必然空跑,应直接拒绝;
// 定时备份不拦:「从未来某天开始备份」合法,空窗期会被标 skipped(no_partitions)。
func TestValidateDailyRange_ImmediateInvertedWindow(t *testing.T) {
	today := time.Now().Format("20060102")
	yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")

	// 窗口 [今天, 昨天] 反转 → 拒绝
	if err := validateDailyRange("immediate", "incremental", "daily", today, "", "", 1); err == nil {
		t.Fatal("expected inverted window rejection for immediate")
	}
	// 窗口 [昨天, 昨天] 恰好一个分区 → 放行(回归:闭区间临界值)
	if err := validateDailyRange("immediate", "incremental", "daily", yesterday, "", "", 1); err != nil {
		t.Fatalf("boundary date should pass: %v", err)
	}
	// 定时备份未来 start_date → 放行
	if err := validateDailyRange("scheduled", "incremental", "daily", today, "", "", 1); err != nil {
		t.Fatalf("scheduled future start_date should pass: %v", err)
	}
	// start_date 为空 → 放行(回归)
	if err := validateDailyRange("immediate", "incremental", "daily", "", "", "", 1); err != nil {
		t.Fatalf("empty start_date should pass: %v", err)
	}
}
```

若文件未 import `time`,补上。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./service/backup/ -run TestValidateDailyRange_ImmediateInvertedWindow -v -count=1`
Expected: FAIL,`expected inverted window rejection for immediate`

- [ ] **Step 3: 最小实现**

`service/backup/validate.go`,把滚动窗口分支(原 95-103 行)改为:

```go
	if endDaysBefore <= 0 {
		return errors.New("daily backup requires days_before > 0")
	}
	if startDate != "" {
		start, err := parseYYYYMMDD("start_date", startDate)
		if err != nil {
			return err
		}
		// 立即备份:窗口 [start_date, 今天−days_before] 反转时本次必然空跑,直接拒绝。
		// 定时备份不拦——「从未来某天开始备份」是合法语义,空窗期 run 会标 skipped。
		if scheduleType != "scheduled" {
			windowEnd := time.Now().AddDate(0, 0, -endDaysBefore).Format("20060102")
			if start.Format("20060102") > windowEnd {
				return fmt.Errorf("immediate daily backup window is empty: start_date %s is after today-%dd (%s)",
					startDate, endDaysBefore, windowEnd)
			}
		}
	}
	return nil
```

- [ ] **Step 4: 跑测试确认通过 + 包级回归**

Run: `go test ./service/backup/ -v -count=1 2>&1 | tail -10`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add service/backup/validate.go service/backup/validate_test.go
git commit -m "fix(backup): 立即备份滚动窗口反转时拒绝提交,定时备份保持放行"
```

---

### Task 3: toDate 误判 daily 兼容修复

**Files:**
- Modify: `service/backup/daily_compat.go:6-8`
- Test: `service/backup/daily_compat_test.go:11,36`

- [ ] **Step 1: 修改测试期望(先红)**

`service/backup/daily_compat_test.go`:
- 第 11 行 `{"toDate(event_time)", true},` 改为 `{"toDate(event_time)", false},`
- 第 36 行 `{"toDate(t)", "day"},` 改为 `{"toDate(t)", "custom"},`

并在第 11 行处加注释:

```go
		// toDate 分区值是 '2026-06-04'(带横线),与扫描 SQL 的 YYYYMMDD 字符串比较永不匹配
		{"toDate(event_time)", false},
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./service/backup/ -run "TestIsDailyCompatible|TestPartitionFmt" -v -count=1`
Expected: FAIL(toDate 当前被判为兼容)

- [ ] **Step 3: 修正正则**

`service/backup/daily_compat.go`:

```go
// 日级别（partition value 是 YYYYMMDD 格式）
// 注意:toDate 不在内——PARTITION BY toDate(x) 的分区值是 '2026-06-04'(带横线),
// 与 ListPartitions 的 YYYYMMDD 字符串比较永不匹配,误判会让 daily 备份永远扫不到分区。
var dailyCompatibleRe = regexp.MustCompile(
	`(?i)\b(toYYYYMMDD|formatDateTime\s*\([^,]+,\s*'%Y%m%d')`,
)
```

- [ ] **Step 4: 跑测试确认通过**

Run: `go test ./service/backup/ -run "TestIsDailyCompatible|TestPartitionFmt" -v -count=1`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add service/backup/daily_compat.go service/backup/daily_compat_test.go
git commit -m "fix(backup): toDate 分区键不再误判为 daily 兼容"
```

---

### Task 4: DeletePartitionRecords 核心(记录删除)

**Files:**
- Modify: `service/backup/service.go:33-38`(Service struct 加注入字段)
- Create: `service/backup/purge.go`
- Create: `service/backup/purge_test.go`

- [ ] **Step 1: Service struct 加可注入依赖字段**

`service/backup/service.go`,Service struct 改为:

```go
// Service 是 backup 提交的入口（HTTP / cron 都通过它）。
type Service struct {
	self string
	repo ServiceRepo
	pool ServicePool
	now  func() time.Time

	// DeletePartitionRecords 的可注入依赖(测试用;nil 时走生产默认,见 purge.go)
	getRunsByTable   func(cluster, db, table string, days int) ([]model.BackupRun, error)
	getClusterByName func(name string) (model.CKManClickHouseConfig, error)
	storageFactory   func(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage
}
```

- [ ] **Step 2: 写失败测试**

创建 `service/backup/purge_test.go`:

```go
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
```

- [ ] **Step 3: 跑测试确认编译失败/测试失败**

Run: `go test ./service/backup/ -run TestDeletePartitionRecords -v -count=1`
Expected: 编译错误 `s.DeletePartitionRecords undefined`(方法尚不存在)

- [ ] **Step 4: 实现 purge.go(本步只实现记录删除,远端清理留空壳)**

创建 `service/backup/purge.go`:

```go
package backup

import (
	"errors"
	"fmt"

	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/backup/storage"
)

// DeletePartitionRecordsResult 删除分区备份记录的执行结果。
type DeletePartitionRecordsResult struct {
	RemovedRecords int      `json:"removed_records"` // 删除的分区条目数
	DeletedRuns    int      `json:"deleted_runs"`    // 分区被删光后连带删除的 run 数
	Warnings       []string `json:"warnings"`        // best-effort 步骤(远端清理等)的警告
}

// purgeCleanItem 一条待清理的远端备份数据(在改写记录前收集)。
type purgeCleanItem struct {
	policyID      string
	storagePrefix string
	partition     string
}

// isTerminalRunStatus 判断 run 是否已进入终态(不会再被 Executor 改写)。
func isTerminalRunStatus(status string) bool {
	switch status {
	case model.BACKUP_STATUS_SUCCESS, model.BACKUP_STATUS_FAILED,
		model.BACKUP_STATUS_SKIPPED, model.BACKUP_STATUS_INTERRUPTED:
		return true
	}
	return false
}

// DeletePartitionRecords 按分区名删除该表 365 天内所有终态 run 中的分区条目,
// 让这些分区脱离增量去重、下次备份时重新备份。
//
//   - 必须全历史删除:同分区的 success 可能散落多条 run(含 migrated 老记录),
//     只删一条,更老的 success 会重新生效继续去重(见 successfulPartitionsFromRuns)。
//   - run 的分区被删光时连 run 一起删,避免留下 success+0 分区的尸体记录。
//   - cleanRemote=true 时 best-effort 清理远端备份数据:失败仅记 warning,
//     记录照删——下次重备时 Prepare 阶段会再清一遍目标端残留,有兜底。
func (s *Service) DeletePartitionRecords(cluster, database, table string, partitions []string, cleanRemote bool) (DeletePartitionRecordsResult, error) {
	var result DeletePartitionRecordsResult
	if len(partitions) == 0 {
		return result, errors.New("partitions required")
	}
	target := make(map[string]bool, len(partitions))
	for _, p := range partitions {
		if err := ValidateIdentifier(p); err != nil {
			return result, fmt.Errorf("invalid partition %q: %w", p, err)
		}
		target[p] = true
	}

	runs, err := s.fetchRunsByTable(cluster, database, table, 365)
	if err != nil {
		return result, fmt.Errorf("list runs: %w", err)
	}
	// 守卫:有未结束 run 时拒绝——Executor 持有 run 副本,并发 UpdateRun
	// 会把删掉的条目原样写回(竞态)。
	for _, r := range runs {
		if !isTerminalRunStatus(r.Status) {
			return result, fmt.Errorf("table %s.%s has in-flight run %s (status=%s), retry later",
				database, table, r.RunID, r.Status)
		}
	}

	// 改写前收集远端清理项:storagePrefix 取自 run,target 配置取自 policy。
	var cleanItems []purgeCleanItem
	seenClean := map[string]bool{}

	for _, r := range runs {
		kept := make([]model.BackupRunPartition, 0, len(r.Partitions))
		removed := 0
		for _, p := range r.Partitions {
			if !target[p.Partition] {
				kept = append(kept, p)
				continue
			}
			removed++
			if r.Operation != model.OP_RESTORE { // restore 记录没有对应的远端备份数据
				k := r.PolicyID + "|" + r.StoragePrefix + "|" + p.Partition
				if !seenClean[k] {
					seenClean[k] = true
					cleanItems = append(cleanItems, purgeCleanItem{r.PolicyID, r.StoragePrefix, p.Partition})
				}
			}
		}
		if removed == 0 {
			continue
		}
		result.RemovedRecords += removed
		if len(kept) == 0 {
			if derr := s.repo.DeleteRun(r.RunID); derr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("delete empty run %s: %v", r.RunID, derr))
			} else {
				result.DeletedRuns++
			}
			continue
		}
		r.Partitions = kept
		if uerr := s.repo.UpdateRun(r); uerr != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("update run %s: %v", r.RunID, uerr))
		}
	}

	if cleanRemote && len(cleanItems) > 0 {
		s.cleanRemotePartitions(cluster, database, table, cleanItems, &result)
	}
	return result, nil
}

// cleanRemotePartitions best-effort 清理远端备份数据(Task 5 完整实现)。
func (s *Service) cleanRemotePartitions(cluster, database, table string, items []purgeCleanItem, result *DeletePartitionRecordsResult) {
}

// ── 可注入依赖(测试注入 fake;nil 时走生产默认) ────────────────────────

func (s *Service) fetchRunsByTable(cluster, db, table string, days int) ([]model.BackupRun, error) {
	if s.getRunsByTable != nil {
		return s.getRunsByTable(cluster, db, table, days)
	}
	return repository.Ps.GetRunsByTable(cluster, db, table, days)
}

func (s *Service) lookupCluster(name string) (model.CKManClickHouseConfig, error) {
	if s.getClusterByName != nil {
		return s.getClusterByName(name)
	}
	return repository.Ps.GetClusterbyName(name)
}

func (s *Service) makeStorage(policy model.BackupPolicy, cc model.CKManClickHouseConfig) BackupStorage {
	if s.storageFactory != nil {
		return s.storageFactory(policy, cc)
	}
	return NewStorageForPolicy(policy, cc)
}
```

注意:本步 `storage` import 暂未使用会编译失败——先把 import 行 `"github.com/housepower/ckman/service/backup/storage"` 删掉,Task 5 再加回。

- [ ] **Step 5: 跑测试确认通过**

Run: `go test ./service/backup/ -run TestDeletePartitionRecords -v -count=1`
Expected: 3 个测试全 PASS

- [ ] **Step 6: 包级回归 + Commit**

Run: `go test ./service/backup/ -count=1 2>&1 | tail -3`
Expected: ok

```bash
git add service/backup/service.go service/backup/purge.go service/backup/purge_test.go
git commit -m "feat(backup): DeletePartitionRecords 按分区名全历史删除备份记录"
```

---

### Task 5: 远端数据清理(best-effort)

**Files:**
- Modify: `service/backup/purge.go`(填充 cleanRemotePartitions)
- Test: `service/backup/purge_test.go`

- [ ] **Step 1: 写失败测试**

`service/backup/purge_test.go` 末尾追加(fakeStorage 复用 executor_test.go 的定义,同包可见;若 import 缺 `errors` 补上):

```go
// cleanRemote=true 时按 run 的 policy 组装 storage,对全部副本 host 清 key。
// 备份 key 含执行当时的 replica host 而 run 未记录,故全副本清理(幂等)。
func TestDeletePartitionRecords_CleanRemote(t *testing.T) {
	repo := newMemRepo()
	repo.policies["p1"] = model.BackupPolicy{PolicyID: "p1", TargetType: model.BACKUP_S3}
	repo.runs["r1"] = mkRun("r1", model.BACKUP_STATUS_SUCCESS, model.OP_BACKUP, "ckA", "p1",
		pt("20260604", model.BACKUP_PARTITION_STATUS_SUCCESS))
	fs := &fakeStorage{}
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
```

注:`model.CkShard`/`model.CkReplica` 字段名以 `model/` 包实际定义为准(ConnFactory 中用法:`cc.Shards`→`shard.Replicas`→`replica.Ip`),如有出入按实际调整。

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./service/backup/ -run TestDeletePartitionRecords -v -count=1`
Expected: 新增 3 个测试 FAIL(cleanRemotePartitions 还是空壳,cleaned 为空 / 无 warning)

- [ ] **Step 3: 实现 cleanRemotePartitions**

替换 `service/backup/purge.go` 中的空壳,并把 `"github.com/housepower/ckman/service/backup/storage"` 加回 import:

```go
// cleanRemotePartitions best-effort 清理远端备份数据。备份 key 含执行当时的
// replica host(JoinRunKey),而 run 未记录该 host,故对全部副本 host 逐一清理
// (CleanPartition 对不存在的 key 是幂等 no-op)。任何失败只记 warning 不中断。
func (s *Service) cleanRemotePartitions(cluster, database, table string, items []purgeCleanItem, result *DeletePartitionRecordsResult) {
	cc, err := s.lookupCluster(cluster)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("get cluster %s: %v; remote data not cleaned", cluster, err))
		return
	}
	var hosts []string
	for _, shard := range cc.Shards {
		for _, rep := range shard.Replicas {
			hosts = append(hosts, rep.Ip)
		}
	}
	if len(hosts) == 0 {
		result.Warnings = append(result.Warnings, fmt.Sprintf("cluster %s has no hosts; remote data not cleaned", cluster))
		return
	}
	storages := map[string]BackupStorage{} // policyID → storage;装配失败记 nil,同 policy 不重试
	for _, item := range items {
		st, ok := storages[item.policyID]
		if !ok {
			st = s.assembleStorageForPurge(item.policyID, cc, result)
			storages[item.policyID] = st
		}
		if st == nil {
			continue
		}
		for _, h := range hosts {
			key := storage.JoinRunKey(item.storagePrefix, item.partition, database, table, h)
			if cerr := st.CleanPartition(h, key); cerr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("clean %s on %s: %v", key, h, cerr))
			}
		}
	}
}

// assembleStorageForPurge 按 policy 组装并初始化 storage;失败记 warning 返回 nil。
func (s *Service) assembleStorageForPurge(policyID string, cc model.CKManClickHouseConfig, result *DeletePartitionRecordsResult) BackupStorage {
	policy, perr := s.repo.GetPolicy(policyID)
	if perr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("policy %s not found; its remote data not cleaned", policyID))
		return nil
	}
	st := s.makeStorage(policy, cc)
	if st == nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("policy %s: unsupported target %q; remote data not cleaned", policyID, policy.TargetType))
		return nil
	}
	if ierr := st.Init(); ierr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("policy %s: storage init: %v; remote data not cleaned", policyID, ierr))
		return nil
	}
	return st
}
```

- [ ] **Step 4: 跑测试确认通过 + 包级回归**

Run: `go test ./service/backup/ -count=1 2>&1 | tail -3`
Expected: ok

- [ ] **Step 5: Commit**

```bash
git add service/backup/purge.go service/backup/purge_test.go
git commit -m "feat(backup): 删除分区记录时可选 best-effort 清理远端备份数据"
```

---

### Task 6: API 接线(model + controller + 路由 + swagger)

**Files:**
- Modify: `model/data_manage.go`(请求结构,放在 BackupRequest 附近)
- Modify: `controller/data_manage.go`(DeleteRun 方法之后)
- Modify: `router/v1.go:173` 之后
- Test: 编译验证(controller 层无既有单测模式,不强加)

- [ ] **Step 1: model 请求结构**

`model/data_manage.go`,在 BackupRequest 定义附近加:

```go
// DeletePartitionRecordsRequest 删除分区备份记录:按分区名作用于该表 365 天内全部终态 run。
type DeletePartitionRecordsRequest struct {
	Partitions  []string `json:"partitions"`   // 分区名列表,必填
	CleanRemote bool     `json:"clean_remote"` // 是否同时清理远端备份数据(best-effort)
}
```

- [ ] **Step 2: controller**

`controller/data_manage.go`,DeleteRun 方法之后加:

```go
// DeletePartitionRecords POST /data_manage/backup/table/:cluster/:database/:table/partitions/delete
// 按分区名删除该表 365 天内所有终态 run 中的分区记录(全历史,防老 success 复活),
// 让这些分区脱离增量去重、下次备份重新备份;clean_remote 时顺带清理远端数据。
// @Summary 删除分区备份记录
// @Description 按分区名删除备份台账记录,可选清理远端备份数据
// @version 1.0
// @Security ApiKeyAuth
// @Tags data_manage
// @Accept  json
// @Param req body model.DeletePartitionRecordsRequest true "request body"
// @Failure 200 {string} json "{"retCode":"5004","retMsg":"删除数据失败","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":{"removed_records":2,"deleted_runs":1,"warnings":[]}}"
// @Router /api/v1/data_manage/backup/table/{clusterName}/{database}/{table}/partitions/delete [post]
func (c *DataManageController) DeletePartitionRecords(ctx *gin.Context) {
	cluster := ctx.Param(ClickHouseClusterPath)
	database := ctx.Param("database")
	table := ctx.Param("table")
	var req model.DeletePartitionRecordsRequest
	if err := model.DecodeRequestBody(ctx.Request, &req); err != nil {
		c.wrapfunc(ctx, model.E_INVALID_PARAMS, err)
		return
	}
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	result, err := svc.DeletePartitionRecords(cluster, database, table, req.Partitions, req.CleanRemote)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_DELETE_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, result)
}
```

- [ ] **Step 3: 路由**

`router/v1.go`,第 173 行(ListRunsByTable 路由)之后加:

```go
	groupV1.POST(fmt.Sprintf("/data_manage/backup/table/:%s/:database/:table/partitions/delete", controller.ClickHouseClusterPath), dataManageController.DeletePartitionRecords)
```

- [ ] **Step 4: 编译 + swagger**

Run: `go build ./... && go test ./service/backup/ ./controller/ ./router/ -count=1 2>&1 | tail -5`
Expected: 编译通过,测试 ok

Run: `which swag >/dev/null 2>&1 && swag init || echo "swag 未安装,跳过文档生成(提醒用户)"`
若 swag init 成功,`docs/` 下生成文件一并提交。

- [ ] **Step 5: Commit**

```bash
git add model/data_manage.go controller/data_manage.go router/v1.go docs/
git commit -m "feat(backup): 删除分区备份记录 API(POST .../partitions/delete)"
```

---

### Task 7: 前端 API 方法 + i18n 文案

**工作区:`/data/root/go/src/github.com/housepower/ckman-fe`(main 分支)。开工前确认 `git status` 干净。**

**Files:**
- Modify: `src/apis/dataManage.ts:62-67`(listRunsByTable 之后)
- Modify: `src/services/i18n.ts`(en 约 662 行后,zh 约 1636 行后)

- [ ] **Step 1: API 方法**

`src/apis/dataManage.ts`,`listRunsByTable` 之后加:

```ts
  // 删除分区备份记录(按分区名作用于全部历史 run);clean_remote 同时清理远端数据
  deletePartitionRecords(
    clusterName: string,
    database: string,
    table: string,
    data: { partitions: string[]; clean_remote: boolean }
  ) {
    return axios.post(`${url}/backup/table/${clusterName}/${database}/${table}/partitions/delete`, data);
  },
```

- [ ] **Step 2: i18n key(en + zh 都要加,放在各自 'Run Deleted' 之后)**

en(约 662 行后):

```ts
      'Delete Partition Records': 'Delete backup records',
      'Delete Selected Partition Records': 'Delete Records of Selected ({count})',
      'Confirm Delete Partition Records': 'This deletes ALL backup records of the following partitions (across every run in the last 365 days). They will be re-backed up on the next run. Records cannot be recovered.',
      'Also Clean Remote Data': 'Also delete backup data on S3 / local storage',
      'Partition Records Deleted': 'Backup records deleted',
      'Partition Records Deleted With Warnings': 'Backup records deleted with {count} warning(s), check ckman log',
```

zh(约 1636 行后):

```ts
      'Delete Partition Records': '删除备份记录',
      'Delete Selected Partition Records': '删除选中分区记录 ({count})',
      'Confirm Delete Partition Records': '将删除以下分区在近 365 天所有 run 中的全部备份记录;删除后这些分区将在下次备份时重新备份,记录不可恢复:',
      'Also Clean Remote Data': '同时清理 S3 / 本地存储上的备份数据',
      'Partition Records Deleted': '备份记录已删除',
      'Partition Records Deleted With Warnings': '备份记录已删除,但有 {count} 条警告,请检查 ckman 日志',
```

- [ ] **Step 3: lint 验证 + Commit(在 ckman-fe 仓库)**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe && make lint
git add src/apis/dataManage.ts src/services/i18n.ts
git commit -m "feat(backup): 删除分区备份记录 API 封装与文案"
```

---

### Task 8: 立即备份反转窗口前端阻断

**Files:**
- Modify: `../ckman-fe/src/views/data-manage/component/backup-form-dialog.vue`(约 243-259 行的 start-date form-item;rules 对象约 450 行)

- [ ] **Step 1: form-item 加 prop**

start-date 的 `<el-form-item>`(约 242 行)加 `prop="startDate"`:

```html
                <el-form-item
                    key="start-date-input"
                    v-if="form.backupStyle === 'incremental' && form.backupType === 'daily' && rangeModeEffective === 'rolling'"
                    :label="$t('backup.Start Date')"
                    prop="startDate"
                >
```

- [ ] **Step 2: rules 加校验器**

`rules` 对象中(`fixedRange` 之后)加:

```js
                startDate: [
                    {
                        required: false,
                        trigger: 'change',
                        validator: (rule, value, callback) => {
                            // 立即备份 + 滚动窗口反转(起始日期晚于窗口终点)时本次必然空跑,阻断提交;
                            // 定时备份不拦(空窗期后端会标 skipped),保留下方红字提示即可
                            if (this.form.scheduleType === 'immediate'
                                && this.form.backupStyle === 'incremental'
                                && this.form.backupType === 'daily'
                                && this.rangeModeEffective === 'rolling'
                                && this.effectiveRangeSkip) {
                                callback(new Error(this.$t('backup.Effective Range Skip')));
                                return;
                            }
                            callback();
                        }
                    }
                ],
```

说明:提交时 `validate()` 会跑全部 rules,无需额外 watch;`effectiveRangeSkip` 是既有 computed(第 702 行)。

- [ ] **Step 3: lint + Commit(在 ckman-fe 仓库)**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe && make lint
git add src/views/data-manage/component/backup-form-dialog.vue
git commit -m "fix(backup): 立即备份滚动窗口反转时表单阻断提交"
```

---

### Task 9: 分区列表删除入口(批量 + 单分区,替换危险的删 run 按钮)

**Files:**
- Modify: `../ckman-fe/src/views/data-manage/component/partition-list-dialog.vue`

- [ ] **Step 1: 展开行按钮改语义**

约 91-96 行,把:

```html
                <el-button
                  type="text"
                  size="mini"
                  style="color:#F56C6C"
                  @click.stop="onDeleteRun(op)"
                >{{ $t('history.Delete') }}</el-button>
```

改为(op 行级按钮的目标是"该分区",不再是"整条 run"):

```html
                <el-button
                  type="text"
                  size="mini"
                  style="color:#F56C6C"
                  @click.stop="openDeleteRecords([row.partition])"
                >{{ $t('history.Delete Partition Records') }}</el-button>
```

- [ ] **Step 2: footer 加批量按钮**

footer(约 154-159 行)的恢复按钮之前加:

```html
      <el-button type="danger" plain :disabled="selectedPartitions.length === 0" @click="openDeleteRecords(selectedPartitions)">
        {{ $t('history.Delete Selected Partition Records', { count: selectedPartitions.length }) }}
      </el-button>
```

- [ ] **Step 3: 确认弹窗(主 el-dialog 内、footer 之后加嵌套 dialog)**

```html
    <!-- 删除分区备份记录确认 -->
    <el-dialog
      :visible.sync="deleteDialogVisible"
      :title="$t('history.Delete Partition Records')"
      width="480px"
      append-to-body
    >
      <p style="color:#F56C6C;margin:0 0 8px">{{ $t('history.Confirm Delete Partition Records') }}</p>
      <div class="mono" style="max-height:160px;overflow:auto;margin-bottom:12px;word-break:break-all">
        {{ deleteTargets.join(', ') }}
      </div>
      <el-checkbox v-model="deleteCleanRemote">{{ $t('history.Also Clean Remote Data') }}</el-checkbox>
      <span slot="footer">
        <el-button @click="deleteDialogVisible = false">{{ $t('common.Cancel') }}</el-button>
        <el-button type="danger" :loading="deleting" @click="confirmDeleteRecords">
          {{ $t('history.Confirm Delete Btn') }}
        </el-button>
      </span>
    </el-dialog>
```

- [ ] **Step 4: data 字段 + 方法;删掉 onDeleteRun**

data() 中加:

```js
      deleteDialogVisible: false,
      deleteTargets: [],
      deleteCleanRemote: true,
      deleting: false,
```

methods 中**删除** `onDeleteRun` 整个方法(约 402-426 行),加:

```js
    openDeleteRecords(partitions) {
      if (!partitions || !partitions.length) return;
      this.deleteTargets = [...partitions];
      this.deleteCleanRemote = true; // 每次打开重置为默认勾选
      this.deleteDialogVisible = true;
    },
    async confirmDeleteRecords() {
      this.deleting = true;
      try {
        const res = await DataManageApi.deletePartitionRecords(
          this.policy.cluster_name, this.policy.database, this.policy.table,
          { partitions: this.deleteTargets, clean_remote: this.deleteCleanRemote }
        );
        if (res.data.retCode === '0000') {
          const warnings = (res.data.entity && res.data.entity.warnings) || [];
          if (warnings.length) {
            this.$message.warning(this.$t('history.Partition Records Deleted With Warnings', { count: warnings.length }));
          } else {
            this.$message.success(this.$t('history.Partition Records Deleted'));
          }
          this.deleteDialogVisible = false;
          this.fetchRuns();
        } else {
          this.$message.error(res.data.retMsg || this.$t('history.Delete Failed'));
        }
      } catch (e) {
        this.$message.error(this.$t('history.Delete Failed') + ': ' + (e.message || ''));
      } finally {
        this.deleting = false;
      }
    },
```

注:`history.Confirm Delete Run` / `history.Run Deleted` 两个 i18n key 不再被引用,保留不删(避免误伤其它引用;grep 确认后可在后续清理)。

- [ ] **Step 5: lint + build 验证(在 ckman-fe 仓库)**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe && make lint && make build
```
Expected: lint/build 通过

- [ ] **Step 6: Commit(在 ckman-fe 仓库)**

```bash
git add src/views/data-manage/component/partition-list-dialog.vue
git commit -m "feat(backup): 分区列表支持删除分区备份记录(批量/单分区,替换整 run 删除)"
```

---

### Task 10: 收尾验证

- [ ] **Step 1: 后端全量测试**

```bash
cd /data/root/go/src/github.com/housepower/ckman && go build ./... && go test ./... -count=1 2>&1 | grep -v "^ok" | head -20
```
Expected: 无 FAIL

- [ ] **Step 2: 核对 spec 覆盖**

对照 `docs/superpowers/specs/2026-06-05-backup-partition-records-design.md` 四项改动逐条确认有对应 commit。

- [ ] **Step 3: 汇报**

向用户汇报:后端/前端各 commit 列表;提醒前端改动在 ckman-fe main 分支,**push ckman-fe 并更新 ckman 子模块指针**(`cd frontend && git fetch && git checkout <new-sha>`)需要用户确认后执行;若 swag 未安装提醒手动 `swag init`。
