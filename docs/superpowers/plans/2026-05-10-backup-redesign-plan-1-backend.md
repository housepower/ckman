# Backup 重设计 Plan 1：Backend Foundation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地新数据模型 (`BackupPolicy` + `BackupRun`) + 服务骨架 (`service/backup/`) + 修复 spec §2 列出的执行期 Bug；不切 HTTP 入口、不动前端，老接口仍工作，新代码仅由单元测试覆盖。

**Architecture:**
- 新数据：两张持久化表（policy 调度配置、run 执行历史不可变快照），partition 内嵌 JSON
- 新模块 `service/backup/`：service / scheduler / pool / executor + storage 子包
- 4 个 repository backend（local、MySQL、PostgreSQL、DM8）一并扩接口，保持现有持久层模式

**Tech Stack:** Go 1.24 / `gin` / `robfig/cron` / `errgroup` / `clickhouse-go` / `lodash-es-equiv` 不需要

**对应 spec：** `docs/superpowers/specs/2026-05-10-backup-restore-redesign.md` § 4 / § 5 / § 8

**前置：** 当前 main 分支 commit `8247805`（spec 已提交）。

---

## 文件总览

### 新增

| 文件 | 职责 |
|---|---|
| `service/backup/service.go` | `BackupService`：Submit / SubmitRestore / 重叠检测 / 台账查询 |
| `service/backup/scheduler.go` | cron 注册 + 60 秒 Reconcile loop |
| `service/backup/pool.go` | 内存 channel 队列 + worker pool |
| `service/backup/executor.go` | RunExecutor（init→prepare→backup/restore→check→close 状态机） |
| `service/backup/storage/storage.go` | Storage 接口 |
| `service/backup/storage/s3.go` | S3 实现（迁自 `service/clickhouse/data_manage.go` 的 `S3Client`） |
| `service/backup/storage/local.go` | 本地实现，路径白名单 + checksum 完整实现 |
| `service/backup/service_test.go` 等 | 各文件的测试 |

### 修改

| 文件 | 改动 |
|---|---|
| `model/data_manage.go` | 新增 `BackupPolicy`、`BackupRun`、`BackupRunPartition` 类型；重命名 `BACKUP_BY_DAILY` → `BACKUP_TYPE_DAILY_PARTITION`；保留 `BACKUP_TYPE_DAILY` 作为兼容别名 |
| `repository/persistent.go` | 新增 `PersistentBackupPolicyService`、`PersistentBackupRunService` 接口；`PersistentMgr` 组合进去 |
| `repository/local/local.go` + `model.go` | 实现新接口 |
| `repository/mysql/mysql.go` + `model.go` | 实现新接口 |
| `repository/postgres/postgres.go` + `model.go` | 实现新接口 |
| `repository/dm8/dm8.go` + `model.go` | 实现新接口 |
| `cmd/ckman/main.go` 或同等启动文件 | 启动时调 `service.backup.Init()`（启动顺序：repository → backup → router） |

### 不动

- `controller/data_manage.go`（HTTP 切换在 Plan 2 做）
- `service/cron/cron_service.go`（旧 cron 路径并行存在；Plan 2 切到新 scheduler 后再删）
- `service/clickhouse/data_manage.go`（保留作为旧实现的事实参照，Plan 2 切走后再删）
- frontend / **(全在 Plan 2)**

---

## 风险与对策

- **新旧调度并存**：Plan 1 完成后 cron 仍由老 `addJobFromBackups` 触发老 `BackupManage`；新 scheduler 不注册 cron job（reconcile loop 设计已隔离），不会双重触发
- **持久层 4 backend**：每加一个新接口要在 4 处实现；用 mock + 表驱动测试覆盖一致性
- **数据迁移**：本 Plan 不做迁移脚本（放 Plan 3 跟 cutover 一起）。新模型与老模型并存，互不读写

---

## Phase A：数据模型 (Tasks 1-2)

### Task 1：BackupPolicy / BackupRun / BackupRunPartition 类型定义

**Files:**
- Modify: `model/data_manage.go`（追加，不动现有 `Backup`）
- Test: `model/data_manage_v2_test.go`

- [ ] **Step 1：写测试 — 序列化 / 反序列化与默认值**

```go
// model/data_manage_v2_test.go
package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBackupPolicy_RoundTrip(t *testing.T) {
	p := BackupPolicy{
		PolicyID:     "p-1",
		ClusterName:  "ckA",
		Database:     "dba",
		Table:        "t1",
		ScheduleType: BACKUP_SCHEDULED,
		Crontab:      "0 3 * * *",
		Instance:     "ckman-01",
		BackupStyle:  BACKUP_STYLE_INCR,
		BackupType:   BACKUP_TYPE_PARTITION,
		DaysBefore:   7,
		Partitions:   []string{"20250508"},
		TargetType:   BACKUP_S3,
		Compression:  "zstd",
		Checksum:     true,
		Clean:        false,
		Enabled:      true,
		Deleted:      false,
		CreateTime:   time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
		UpdateTime:   time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	}
	bs, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got BackupPolicy
	if err := json.Unmarshal(bs, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.PolicyID != p.PolicyID || got.Crontab != p.Crontab || len(got.Partitions) != 1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

func TestBackupRun_PartitionEmbedded(t *testing.T) {
	r := BackupRun{
		RunID:    "r-1",
		PolicyID: "p-1",
		Status:   BACKUP_STATUS_SUCCESS,
		Partitions: []BackupRunPartition{
			{Partition: "20250508", Status: BACKUP_PARTITION_STATUS_SUCCESS, Size: 1234},
		},
	}
	bs, _ := json.Marshal(r)
	var got BackupRun
	if err := json.Unmarshal(bs, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Partitions) != 1 || got.Partitions[0].Size != 1234 {
		t.Fatalf("partitions not embedded correctly: %+v", got)
	}
}
```

- [ ] **Step 2：跑测试，确认编译失败**

```bash
go test ./model -run "TestBackupPolicy_RoundTrip|TestBackupRun_PartitionEmbedded" -v
# 期望：FAIL — 类型未定义
```

- [ ] **Step 3：在 `model/data_manage.go` 末尾追加类型与新常量**

```go
// model/data_manage.go 末尾追加

// ============== 新数据模型（Plan 1 引入，老 Backup 类型仍保留） ==============

const (
	// BackupStyle: 全量 vs 增量（迁移自 BACKUP_STYLE_FULL/INCR，已存在）

	// BackupType 增量方式枚举
	BACKUP_TYPE_DAILY_PARTITION = "daily" // partition key 必须是日级别
	// 兼容别名（旧字符串值仍在使用）
	BACKUP_TYPE_DAILY = BACKUP_TYPE_DAILY_PARTITION
)

const (
	// Run trigger types
	TRIGGER_CRON             = "cron"
	TRIGGER_MANUAL_IMMEDIATE = "manual_immediate"
	TRIGGER_MANUAL_RESTORE   = "manual_restore"
	TRIGGER_RETRY            = "retry"
	TRIGGER_MIGRATED         = "migrated"

	// Run skip / interrupt reasons
	REASON_OVERLAP        = "overlap"
	REASON_QUEUE_FULL     = "queue_full"
	REASON_DISABLED       = "disabled"
	REASON_RESTART        = "ckman restart"
	REASON_INST_CHANGED   = "instance changed"
)

type BackupPolicy struct {
	PolicyID     string      `json:"policy_id"`
	ClusterName  string      `json:"cluster_name"`
	Database     string      `json:"database"`
	Table        string      `json:"table"`
	ScheduleType string      `json:"schedule_type"` // immediate | scheduled
	Crontab      string      `json:"crontab"`
	Instance     string      `json:"instance"`
	BackupStyle  string      `json:"backup_style"` // full | incremental
	BackupType   string      `json:"backup_type"`  // partition | daily
	DaysBefore   int         `json:"days_before"`
	Partitions   []string    `json:"partitions"`
	TargetType   string      `json:"target_type"` // s3 | local
	S3           TargetS3    `json:"s3"`
	Local        TargetLocal `json:"local"`
	Compression  string      `json:"compression"`
	Checksum     bool        `json:"checksum"`
	Clean        bool        `json:"clean"`
	Enabled      bool        `json:"enabled"`
	Deleted      bool        `json:"deleted"`
	CreateTime   time.Time   `json:"create_time"`
	UpdateTime   time.Time   `json:"update_time"`
}

type BackupRun struct {
	RunID        string               `json:"run_id"`
	PolicyID     string               `json:"policy_id"`
	ClusterName  string               `json:"cluster_name"`
	Database     string               `json:"database"`
	Table        string               `json:"table"`
	Operation    string               `json:"operation"`    // backup | restore
	TriggerType  string               `json:"trigger_type"` // cron | manual_immediate | ...
	Instance     string               `json:"instance"`
	Status       string               `json:"status"` // queued | running | success | failed | skipped | interrupted
	StatusReason string               `json:"status_reason"`
	Partitions   []BackupRunPartition `json:"partitions"`
	StartedAt    time.Time            `json:"started_at"`
	FinishedAt   time.Time            `json:"finished_at"`
	Elapsed      int                  `json:"elapsed"`
	ErrorMsg     string               `json:"error_msg"`
	CreateTime   time.Time            `json:"create_time"`
}

type BackupRunPartition struct {
	Partition string              `json:"partition"`
	Status    string              `json:"status"`
	Size      uint64              `json:"size"`
	Rows      uint64              `json:"rows"`
	FileNum   uint64              `json:"file_num"`
	Elapsed   int                 `json:"elapsed"`
	Msg       string              `json:"msg"`
	PathInfo  map[string]PathInfo `json:"-"` // 仅 checksum=true 时填充，不入库
}
```

- [ ] **Step 4：跑测试，确认通过**

```bash
go test ./model -run "TestBackupPolicy_RoundTrip|TestBackupRun_PartitionEmbedded" -v
# 期望：PASS
```

- [ ] **Step 5：commit**

```bash
git add model/data_manage.go model/data_manage_v2_test.go
git commit -m "$(cat <<'EOF'
feat(model): 新增 BackupPolicy / BackupRun 数据模型

为备份重设计引入两张表的 Go 类型：BackupPolicy 表示调度配置，
BackupRun 表示不可变执行历史，partition 明细以 JSON 内嵌于 run。
保留老 Backup 类型不动，待 Plan 3 cutover 后再删。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2：值校验 helper

**Files:**
- Create: `service/backup/validate.go`
- Test: `service/backup/validate_test.go`

承担 spec §7.3 / §8.5 / §9 的提交期校验逻辑。

- [ ] **Step 1：写测试**

```go
// service/backup/validate_test.go
package backup

import (
	"strings"
	"testing"
)

func TestValidateLocalPath(t *testing.T) {
	cases := []struct {
		path string
		ok   bool
	}{
		{"/data/backups", true},
		{"/var/lib/ckman/backup-1", true},
		{"backups", false},               // 必须绝对路径
		{"/data/../etc/passwd", false},   // 含 ..
		{"/data;rm -rf /", false},        // 含分号
		{"/data $(echo)", false},         // 含 $
		{"/data with space", false},      // 空格
		{strings.Repeat("/a", 200), false}, // 超长 (>256)
	}
	for _, c := range cases {
		err := ValidateLocalPath(c.path)
		if (err == nil) != c.ok {
			t.Errorf("ValidateLocalPath(%q): want ok=%v got err=%v", c.path, c.ok, err)
		}
	}
}

func TestValidateIdentifier(t *testing.T) {
	for _, name := range []string{"events_log", "dba", "user-action"} {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("identifier %q should pass: %v", name, err)
		}
	}
	for _, name := range []string{"a`b", "a';drop", ""} {
		if err := ValidateIdentifier(name); err == nil {
			t.Errorf("identifier %q should reject", name)
		}
	}
}

func TestValidateCrontabMinInterval(t *testing.T) {
	// 至少 1 小时
	if err := ValidateCrontabMinInterval("0 * * * *"); err != nil {
		t.Errorf("hourly should pass: %v", err)
	}
	if err := ValidateCrontabMinInterval("* * * * *"); err == nil {
		t.Error("every minute should reject")
	}
	if err := ValidateCrontabMinInterval("0 3 * * *"); err != nil {
		t.Errorf("daily should pass: %v", err)
	}
	if err := ValidateCrontabMinInterval("not-a-cron"); err == nil {
		t.Error("invalid cron should reject")
	}
}
```

- [ ] **Step 2：跑测试，确认 fail**

```bash
go test ./service/backup -run "TestValidate" -v
# 期望：FAIL — 函数未定义 / 包不存在
```

- [ ] **Step 3：实现**

```go
// service/backup/validate.go
package backup

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	pathRe       = regexp.MustCompile(`^/[a-zA-Z0-9_./\-]+$`)
	identifierRe = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)
)

const maxLocalPathLen = 256

// ValidateLocalPath 拒绝注入面：要求绝对路径、白名单字符、不含 ..、长度上限
func ValidateLocalPath(p string) error {
	if len(p) == 0 || len(p) > maxLocalPathLen {
		return fmt.Errorf("local path empty or too long (>%d)", maxLocalPathLen)
	}
	if strings.Contains(p, "..") {
		return errors.New("local path may not contain '..'")
	}
	if !pathRe.MatchString(p) {
		return fmt.Errorf("local path %q contains forbidden characters", p)
	}
	return nil
}

// ValidateIdentifier 用于 database / table / 分区名等会被拼到 SQL 的字符串
func ValidateIdentifier(s string) error {
	if s == "" {
		return errors.New("identifier empty")
	}
	if !identifierRe.MatchString(s) {
		return fmt.Errorf("identifier %q contains forbidden characters", s)
	}
	return nil
}

// ValidateCrontabMinInterval 解析 cron，检查相邻两次触发间隔 >= 1 小时
func ValidateCrontabMinInterval(spec string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(spec)
	if err != nil {
		return fmt.Errorf("invalid cron: %w", err)
	}
	now := time.Now()
	first := sched.Next(now)
	second := sched.Next(first)
	if second.Sub(first) < time.Hour {
		return fmt.Errorf("crontab interval too small (%s < 1h)", second.Sub(first))
	}
	return nil
}
```

- [ ] **Step 4：跑测试，确认 PASS**

```bash
go test ./service/backup -run "TestValidate" -v
# 期望：PASS
```

- [ ] **Step 5：commit**

```bash
git add service/backup/validate.go service/backup/validate_test.go
git commit -m "$(cat <<'EOF'
feat(backup): 提交期校验 helper（路径白名单 / identifier / cron 最小间隔）

为备份重设计提供集中校验：
- ValidateLocalPath：路径白名单字符 + 拒绝 .. + 长度 ≤ 256，闭合 RCE 注入面
- ValidateIdentifier：库/表/分区名拼 SQL 前的合法性
- ValidateCrontabMinInterval：相邻触发间隔 ≥ 1 小时

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase B：Repository (Tasks 3-7)

### Task 3：扩展 PersistentMgr 接口

**Files:**
- Modify: `repository/persistent.go`

- [ ] **Step 1：在 `repository/persistent.go` 添加新接口（接在 `PersistentBackupService` 之后）**

```go
// repository/persistent.go 接在 PersistentBackupService 后

type PersistentBackupPolicyService interface {
	CreateBackupPolicy(p model.BackupPolicy) error
	UpdateBackupPolicy(p model.BackupPolicy) error
	DeleteBackupPolicy(policyID string) error // 软删：仅置 deleted=true
	GetBackupPolicy(policyID string) (model.BackupPolicy, error)
	GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error)
	GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) // enabled+scheduled+!deleted+instance==
}

type PersistentBackupRunService interface {
	CreateBackupRun(r model.BackupRun) error
	UpdateBackupRun(r model.BackupRun) error
	GetBackupRun(runID string) (model.BackupRun, error)
	GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error)
	GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error)
	GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error)             // status ∈ queued/running
	GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error)            // 启动时回填用
	MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) // 条件 UPDATE，避免双拉
}
```

- [ ] **Step 2：把它们组合进 `PersistentMgr`**

找到现有的 `PersistentMgr` interface 声明（约第 80 行），追加：

```go
type PersistentMgr interface {
	PersistentBase
	PersistentClusterService
	PersistentLogicService
	PersistentQueryHistoryService
	PersistentTaskService
	PersistentBackupService           // 老接口保留
	PersistentBackupPolicyService     // 新接口
	PersistentBackupRunService        // 新接口
}
```

- [ ] **Step 3：补 import**

`repository/persistent.go` 顶部 import 块加 `"time"`（如果还没有）。

- [ ] **Step 4：编译确认**

```bash
go build ./repository/...
# 期望：FAIL — 4 个 backend 不实现新接口
```

输出形如：
```
*LocalPersistent does not implement PersistentMgr (missing CreateBackupPolicy method)
```

正符合预期，4 backends 接下来的 4 个 task 各自补齐。

- [ ] **Step 5：暂不 commit**（接口与实现要在同一 commit 里，避免主分支编译断）

---

### Task 4：Local backend 实现

**Files:**
- Modify: `repository/local/model.go`、`repository/local/local.go`
- Test: `repository/local/local_v2_test.go`

- [ ] **Step 1：扩展 PersistentData**

`repository/local/model.go` 找到 `PersistentData` struct，加两个字段：

```go
// repository/local/model.go
type PersistentData struct {
	// ... 现有字段保持
	BackupPolicy map[string]model.BackupPolicy `yaml:"backup_policy" json:"backup_policy"`
	BackupRun    map[string]model.BackupRun    `yaml:"backup_run"    json:"backup_run"`
}
```

`Init()` 里给两个新 map make 一下（在 `lp.Data.Backup = make(...)` 行之后）：

```go
// repository/local/local.go Init() 内
lp.Data.BackupPolicy = make(map[string]model.BackupPolicy)
lp.Snapshot.BackupPolicy = make(map[string]model.BackupPolicy)
lp.Data.BackupRun = make(map[string]model.BackupRun)
lp.Snapshot.BackupRun = make(map[string]model.BackupRun)
```

- [ ] **Step 2：写测试**

```go
// repository/local/local_v2_test.go
package local

import (
	"testing"
	"time"

	"github.com/housepower/ckman/model"
)

func newTestLP(t *testing.T) *LocalPersistent {
	t.Helper()
	lp := &LocalPersistent{}
	if err := lp.Init(LocalConfig{Format: "json", PolicyDir: t.TempDir()}); err != nil {
		t.Fatalf("init: %v", err)
	}
	return lp
}

func TestLocal_BackupPolicyCRUD(t *testing.T) {
	lp := newTestLP(t)
	p := model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Enabled: true}
	if err := lp.CreateBackupPolicy(p); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := lp.GetBackupPolicy("p1")
	if err != nil || got.PolicyID != "p1" {
		t.Fatalf("get: %v %+v", err, got)
	}
	got.Crontab = "0 5 * * *"
	if err := lp.UpdateBackupPolicy(got); err != nil {
		t.Fatalf("update: %v", err)
	}
	got2, _ := lp.GetBackupPolicy("p1")
	if got2.Crontab != "0 5 * * *" {
		t.Fatalf("update lost: %+v", got2)
	}
	if err := lp.DeleteBackupPolicy("p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got3, _ := lp.GetBackupPolicy("p1")
	if !got3.Deleted {
		t.Fatalf("delete should soft-delete: %+v", got3)
	}
}

func TestLocal_BackupRunCRUD_AndQueries(t *testing.T) {
	lp := newTestLP(t)
	now := time.Now()
	mustCreate := func(id, policy, status string, ts time.Time) {
		t.Helper()
		r := model.BackupRun{
			RunID: id, PolicyID: policy, ClusterName: "ckA", Database: "dba", Table: "t1",
			Status: status, StartedAt: ts, CreateTime: ts, Instance: "ckman-01",
		}
		if err := lp.CreateBackupRun(r); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	mustCreate("r1", "p1", model.BACKUP_STATUS_SUCCESS, now.Add(-3*24*time.Hour))
	mustCreate("r2", "p1", model.BACKUP_STATUS_SUCCESS, now.Add(-2*24*time.Hour))
	mustCreate("r3", "p1", model.BACKUP_STATUS_RUNNING, now.Add(-time.Hour))
	mustCreate("r4", "p2", model.BACKUP_STATUS_QUEUED, now)

	runs, err := lp.GetRunsByPolicy("p1", 10, time.Time{})
	if err != nil || len(runs) != 3 {
		t.Fatalf("p1 runs: %v len=%d", err, len(runs))
	}
	tableRuns, _ := lp.GetRunsByTable("ckA", "dba", "t1", 7)
	if len(tableRuns) != 3 { // r1/r2/r3 都是 ckA.dba.t1，r4 是 p2 与 t1 无关
		t.Fatalf("table runs len=%d, expected 3", len(tableRuns))
	}
	inFlight, _ := lp.GetRunsInFlightByPolicy("p1")
	if len(inFlight) != 1 || inFlight[0].RunID != "r3" {
		t.Fatalf("in flight: %+v", inFlight)
	}
	insRuns, _ := lp.GetRunsInFlightByInstance("ckman-01")
	if len(insRuns) != 2 { // r3 running + r4 queued
		t.Fatalf("instance in-flight: %d", len(insRuns))
	}
}

func TestLocal_MarkRunRunningIfQueued(t *testing.T) {
	lp := newTestLP(t)
	r := model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_QUEUED, Instance: "ckman-01"}
	_ = lp.CreateBackupRun(r)

	ok, err := lp.MarkRunRunningIfQueued("r1", "ckman-01", time.Now())
	if err != nil || !ok {
		t.Fatalf("first mark: ok=%v err=%v", ok, err)
	}
	// 第二次必须 false（已 running）
	ok2, err := lp.MarkRunRunningIfQueued("r1", "ckman-01", time.Now())
	if err != nil || ok2 {
		t.Fatalf("second mark should false: ok=%v err=%v", ok2, err)
	}
}
```

注：上文用到了 `BACKUP_STATUS_QUEUED` 常量。在 `model/data_manage.go` 现有 `BACKUP_STATUS_*` 常量列表里加一行：

```go
BACKUP_STATUS_QUEUED      = "queued"
BACKUP_STATUS_INTERRUPTED = "interrupted"
BACKUP_STATUS_SKIPPED     = "skipped"
```

- [ ] **Step 3：实现**

`repository/local/local.go` 末尾追加：

```go
// repository/local/local.go 末尾追加

func (lp *LocalPersistent) CreateBackupPolicy(p model.BackupPolicy) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupPolicy[p.PolicyID]; ok {
		return fmt.Errorf("policy %s already exists", p.PolicyID)
	}
	if p.CreateTime.IsZero() {
		p.CreateTime = time.Now()
	}
	p.UpdateTime = time.Now()
	lp.Data.BackupPolicy[p.PolicyID] = p
	return lp.dump()
}

func (lp *LocalPersistent) UpdateBackupPolicy(p model.BackupPolicy) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupPolicy[p.PolicyID]; !ok {
		return fmt.Errorf("policy %s not found", p.PolicyID)
	}
	p.UpdateTime = time.Now()
	lp.Data.BackupPolicy[p.PolicyID] = p
	return lp.dump()
}

func (lp *LocalPersistent) DeleteBackupPolicy(policyID string) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	p, ok := lp.Data.BackupPolicy[policyID]
	if !ok {
		return fmt.Errorf("policy %s not found", policyID)
	}
	p.Deleted = true
	p.Enabled = false
	p.UpdateTime = time.Now()
	lp.Data.BackupPolicy[policyID] = p
	return lp.dump()
}

func (lp *LocalPersistent) GetBackupPolicy(policyID string) (model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	p, ok := lp.Data.BackupPolicy[policyID]
	if !ok {
		return model.BackupPolicy{}, fmt.Errorf("policy %s not found", policyID)
	}
	return p, nil
}

func (lp *LocalPersistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupPolicy
	for _, p := range lp.Data.BackupPolicy {
		if p.ClusterName == cluster && !p.Deleted {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreateTime.Before(out[j].CreateTime) })
	return out, nil
}

func (lp *LocalPersistent) GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupPolicy
	for _, p := range lp.Data.BackupPolicy {
		if !p.Deleted && p.Enabled && p.ScheduleType == model.BACKUP_SCHEDULED && p.Instance == instance {
			out = append(out, p)
		}
	}
	return out, nil
}

func (lp *LocalPersistent) CreateBackupRun(r model.BackupRun) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupRun[r.RunID]; ok {
		return fmt.Errorf("run %s already exists", r.RunID)
	}
	if r.CreateTime.IsZero() {
		r.CreateTime = time.Now()
	}
	lp.Data.BackupRun[r.RunID] = r
	return lp.dump()
}

func (lp *LocalPersistent) UpdateBackupRun(r model.BackupRun) error {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	if _, ok := lp.Data.BackupRun[r.RunID]; !ok {
		return fmt.Errorf("run %s not found", r.RunID)
	}
	lp.Data.BackupRun[r.RunID] = r
	return lp.dump()
}

func (lp *LocalPersistent) GetBackupRun(runID string) (model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	r, ok := lp.Data.BackupRun[runID]
	if !ok {
		return model.BackupRun{}, fmt.Errorf("run %s not found", runID)
	}
	return r, nil
}

func (lp *LocalPersistent) GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.PolicyID != policyID {
			continue
		}
		if !before.IsZero() && !r.StartedAt.Before(before) {
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (lp *LocalPersistent) GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	cutoff := time.Now().AddDate(0, 0, -sinceDays)
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.ClusterName == cluster && r.Database == database && r.Table == table && r.StartedAt.After(cutoff) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, nil
}

func (lp *LocalPersistent) GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.PolicyID == policyID && (r.Status == model.BACKUP_STATUS_QUEUED || r.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (lp *LocalPersistent) GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error) {
	lp.lock.RLock()
	defer lp.lock.RUnlock()
	var out []model.BackupRun
	for _, r := range lp.Data.BackupRun {
		if r.Instance == instance && (r.Status == model.BACKUP_STATUS_QUEUED || r.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (lp *LocalPersistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
	lp.lock.Lock()
	defer lp.lock.Unlock()
	r, ok := lp.Data.BackupRun[runID]
	if !ok {
		return false, fmt.Errorf("run %s not found", runID)
	}
	if r.Status != model.BACKUP_STATUS_QUEUED {
		return false, nil
	}
	r.Status = model.BACKUP_STATUS_RUNNING
	r.Instance = instance
	r.StartedAt = startedAt
	lp.Data.BackupRun[runID] = r
	return true, lp.dump()
}
```

- [ ] **Step 4：跑测试**

```bash
go test ./repository/local -run "TestLocal_Backup" -v
# 期望：PASS
go build ./...   # 编译应通过（local backend 已实现新接口；mysql/pg/dm8 还会爆）
```

注意：mysql/postgres/dm8 还没实现新接口，整体 `go build ./...` 仍会失败。下三个 task 修。

- [ ] **Step 5：commit**（与 mysql/pg/dm8 一同提交不太现实，因为整链断；这里单独 commit 让 git history 清晰）

```bash
git add model/data_manage.go repository/persistent.go repository/local/
git commit -m "$(cat <<'EOF'
feat(repository): 扩 PersistentMgr 接口 + local backend 实现 BackupPolicy/Run

Plan 1 数据层：
- repository/persistent.go：新增 PersistentBackupPolicyService /
  PersistentBackupRunService，并入 PersistentMgr
- repository/local：实现两套接口，partition 内嵌 JSON 持久化在 LocalConfig
  PolicyDir 下；MarkRunRunningIfQueued 提供原子条件流转
- model/data_manage.go：补齐 BACKUP_STATUS_QUEUED / INTERRUPTED / SKIPPED

mysql / postgres / dm8 backend 暂未实现，整体编译会断；下三 commit 修齐。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5：MySQL backend 实现

**Files:**
- Modify: `repository/mysql/model.go`、`repository/mysql/mysql.go`
- Test: `repository/mysql/mysql_v2_test.go`（如已有 mysql 测试基础设施则启用，否则创建）

- [ ] **Step 1：DB schema migration**

ckman MySQL backend 在第一次启动时通过 `mysql.go` 内的 `CreateTable` 序列建表。在 `CreateTable` 列表里加两条 SQL：

```go
// repository/mysql/mysql.go CreateTable() 内追加
const (
    sqlCreateBackupPolicy = `CREATE TABLE IF NOT EXISTS backup_policy (
        policy_id      VARCHAR(64) PRIMARY KEY,
        cluster_name   VARCHAR(128) NOT NULL,
        database_name  VARCHAR(128) NOT NULL,
        table_name     VARCHAR(128) NOT NULL,
        schedule_type  VARCHAR(16) NOT NULL,
        crontab        VARCHAR(128),
        instance       VARCHAR(64),
        backup_style   VARCHAR(16),
        backup_type    VARCHAR(16),
        days_before    INT,
        partitions     JSON,
        target_type    VARCHAR(16),
        s3_config      JSON,
        local_config   JSON,
        compression    VARCHAR(16),
        checksum       BOOLEAN,
        clean          BOOLEAN,
        enabled        BOOLEAN,
        deleted        BOOLEAN,
        create_time    DATETIME,
        update_time    DATETIME,
        INDEX idx_cluster_db_table (cluster_name, database_name, table_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

    sqlCreateBackupRun = `CREATE TABLE IF NOT EXISTS backup_run (
        run_id         VARCHAR(64) PRIMARY KEY,
        policy_id      VARCHAR(64) NOT NULL,
        cluster_name   VARCHAR(128) NOT NULL,
        database_name  VARCHAR(128) NOT NULL,
        table_name     VARCHAR(128) NOT NULL,
        operation      VARCHAR(16),
        trigger_type   VARCHAR(32),
        instance       VARCHAR(64),
        status         VARCHAR(16),
        status_reason  VARCHAR(255),
        partitions     JSON,
        started_at     DATETIME,
        finished_at    DATETIME,
        elapsed        INT,
        error_msg      TEXT,
        create_time    DATETIME,
        INDEX idx_policy_started (policy_id, started_at),
        INDEX idx_table_started (cluster_name, database_name, table_name, started_at),
        INDEX idx_status_instance (status, instance)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
)
```

把这两条 SQL 加入 `CreateTable` 内执行序列。

- [ ] **Step 2：实现 BackupPolicy CRUD**

按 Task 4 局部接口签名，实现 `repository/mysql/mysql.go` 内同名方法。Partitions 列用 `json.Marshal/Unmarshal` 写入 JSON 列，s3_config / local_config 同理。一个示例（`CreateBackupPolicy`）：

```go
func (mp *MysqlPersistent) CreateBackupPolicy(p model.BackupPolicy) error {
	pj, _ := json.Marshal(p.Partitions)
	s3j, _ := json.Marshal(p.S3)
	lcj, _ := json.Marshal(p.Local)
	if p.CreateTime.IsZero() {
		p.CreateTime = time.Now()
	}
	p.UpdateTime = time.Now()
	_, err := mp.DB.Exec(`INSERT INTO backup_policy (
        policy_id, cluster_name, database_name, table_name, schedule_type, crontab, instance,
        backup_style, backup_type, days_before, partitions,
        target_type, s3_config, local_config, compression, checksum, clean,
        enabled, deleted, create_time, update_time
    ) VALUES (?,?,?,?,?,?,?, ?,?,?,?, ?,?,?,?,?,?, ?,?,?,?)`,
		p.PolicyID, p.ClusterName, p.Database, p.Table, p.ScheduleType, p.Crontab, p.Instance,
		p.BackupStyle, p.BackupType, p.DaysBefore, pj,
		p.TargetType, s3j, lcj, p.Compression, p.Checksum, p.Clean,
		p.Enabled, p.Deleted, p.CreateTime, p.UpdateTime,
	)
	return err
}
```

按这个模式实现剩余方法：UpdateBackupPolicy（UPDATE 全字段）、DeleteBackupPolicy（UPDATE deleted=true）、GetBackupPolicy（SELECT + scan）、GetBackupPoliciesByCluster、GetActiveScheduledPolicies、CreateBackupRun、UpdateBackupRun、GetBackupRun、GetRunsByPolicy（带 limit + before）、GetRunsByTable（带 since-days）、GetRunsInFlightByPolicy、GetRunsInFlightByInstance、MarkRunRunningIfQueued。

`MarkRunRunningIfQueued` 用条件 UPDATE：

```go
func (mp *MysqlPersistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
	res, err := mp.DB.Exec(
		`UPDATE backup_run SET status='running', instance=?, started_at=? WHERE run_id=? AND status='queued'`,
		instance, startedAt, runID,
	)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}
```

- [ ] **Step 3：写测试**

测试可与 Task 4 用同样的表驱动用例，复用即可（把 `newTestLP` 换成 `newTestMP` 即可）。需要 docker-compose 起 MySQL，build tag 标 `integration`。CI 默认不跑。

```go
//go:build integration
// repository/mysql/mysql_v2_test.go ... 同 Task 4 的用例换 backend
```

- [ ] **Step 4：编译确认**

```bash
go build ./repository/mysql/...
# 期望：PASS
```

集成测试本地起 MySQL 后跑：

```bash
go test ./repository/mysql -tags=integration -run "TestMysql_Backup" -v
```

- [ ] **Step 5：commit**

```bash
git add repository/mysql/
git commit -m "$(cat <<'EOF'
feat(repository): MySQL backend 实现 BackupPolicy / BackupRun 接口

- 自动建表 backup_policy / backup_run（含 idx），JSON 列存 partitions / target 配置
- MarkRunRunningIfQueued 用条件 UPDATE 实现原子流转
- 集成测试加 build tag integration（CI 默认不跑）

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6：PostgreSQL backend 实现

**Files:**
- Modify: `repository/postgres/`

按 Task 5 完全一致的模式，注意三处 PG 方言差异：

1. `JSON` 列改为 `JSONB`
2. `BOOLEAN` 同 MySQL，无差
3. `INSERT … ON CONFLICT DO UPDATE` 可选（与 ckman 现有写法一致即可）

`MarkRunRunningIfQueued`：

```sql
UPDATE backup_run SET status='running', instance=$1, started_at=$2
 WHERE run_id=$3 AND status='queued'
```

- [ ] **Step 1：建表 SQL（PG 方言）写在 `postgres/postgres.go`**

```sql
CREATE TABLE IF NOT EXISTS backup_policy (
    policy_id VARCHAR(64) PRIMARY KEY,
    cluster_name VARCHAR(128) NOT NULL,
    database_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    schedule_type VARCHAR(16) NOT NULL,
    crontab VARCHAR(128),
    instance VARCHAR(64),
    backup_style VARCHAR(16),
    backup_type VARCHAR(16),
    days_before INT,
    partitions JSONB,
    target_type VARCHAR(16),
    s3_config JSONB,
    local_config JSONB,
    compression VARCHAR(16),
    checksum BOOLEAN,
    clean BOOLEAN,
    enabled BOOLEAN,
    deleted BOOLEAN,
    create_time TIMESTAMP,
    update_time TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_bp_cluster_db_table ON backup_policy(cluster_name, database_name, table_name);

CREATE TABLE IF NOT EXISTS backup_run (
    run_id VARCHAR(64) PRIMARY KEY,
    policy_id VARCHAR(64) NOT NULL,
    cluster_name VARCHAR(128) NOT NULL,
    database_name VARCHAR(128) NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    operation VARCHAR(16),
    trigger_type VARCHAR(32),
    instance VARCHAR(64),
    status VARCHAR(16),
    status_reason VARCHAR(255),
    partitions JSONB,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    elapsed INT,
    error_msg TEXT,
    create_time TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_br_policy_started ON backup_run(policy_id, started_at);
CREATE INDEX IF NOT EXISTS idx_br_table_started ON backup_run(cluster_name, database_name, table_name, started_at);
CREATE INDEX IF NOT EXISTS idx_br_status_instance ON backup_run(status, instance);
```

- [ ] **Step 2：实现接口方法**

完全沿用 Task 5 的代码结构，把 SQL 占位符 `?` 换成 `$1, $2, ...` 即可。

- [ ] **Step 3：测试 + 编译 + commit** 与 Task 5 同结构。

```bash
git add repository/postgres/
git commit -m "feat(repository): postgres backend 实现 BackupPolicy / BackupRun 接口"
```

---

### Task 7：DM8 backend 实现

**Files:**
- Modify: `repository/dm8/`

DM8 SQL 方言接近 Oracle / 标准 SQL。仿 Task 6 即可，注意：

- 表名/列名建议大写（DM8 习惯）
- `BOOLEAN` 改用 `NUMBER(1)` 或 `INT`，0/1
- `JSONB` 用 `CLOB`
- 连接驱动用 `dm8` 现有 driver

按 Task 5/6 结构完成 + commit。

```bash
git add repository/dm8/
git commit -m "feat(repository): dm8 backend 实现 BackupPolicy / BackupRun 接口"
```

> 校验点：完成后 `go build ./...` 应**整体通过**。

---

## Phase C：Storage 抽象 (Tasks 8-10)

### Task 8：Storage 接口

**Files:**
- Create: `service/backup/storage/storage.go`
- Test: `service/backup/storage/storage_test.go`

- [ ] **Step 1：写接口契约测试（用 fake 实现）**

```go
// service/backup/storage/storage_test.go
package storage

import (
	"testing"

	"github.com/housepower/ckman/model"
)

// 编译期断言：fake 满足 Storage
var _ Storage = (*fakeStorage)(nil)

type fakeStorage struct {
	prepared []string // partition keys
	checked  []string
}

func (f *fakeStorage) Init() error                                         { return nil }
func (f *fakeStorage) BackupSQL(table, partition, key string) string       { return "" }
func (f *fakeStorage) RestoreSQL(table, partition, key string) string      { return "" }
func (f *fakeStorage) CleanPartition(database, table, host, partition string) error {
	f.prepared = append(f.prepared, partition)
	return nil
}
func (f *fakeStorage) CheckPartition(host, database, table, partition string,
	pathInfo map[string]model.PathInfo) error {
	f.checked = append(f.checked, partition)
	return nil
}
func (f *fakeStorage) Type() string { return "fake" }

func TestStorage_InterfaceCompiles(t *testing.T) {
	var s Storage = &fakeStorage{}
	if s.Type() != "fake" {
		t.Fatalf("Type")
	}
}
```

- [ ] **Step 2：跑测试，确认 fail（接口未定义）**

```bash
go test ./service/backup/storage -v
# FAIL — Storage interface 未定义
```

- [ ] **Step 3：写接口**

```go
// service/backup/storage/storage.go
package storage

import "github.com/housepower/ckman/model"

// Storage 抽象 BACKUP TABLE / RESTORE TABLE 的目标端 (S3 / Local)
type Storage interface {
	// Init：初始化客户端 / bucket 创建 / 路径白名单校验等
	Init() error

	// BackupSQL：返回 BACKUP TABLE … TO …(…) 的尾部子句
	// 调用方负责拼前面 BACKUP TABLE 部分。partition="all" 表示全表。
	// key 是 partition 在目标的子路径
	BackupSQL(database, table, partition, key string) string

	// RestoreSQL：返回 RESTORE TABLE … FROM …(…) 的尾部子句
	RestoreSQL(database, table, partition, key string) string

	// CleanPartition：清空目标端某 partition 之前的残留对象 / 文件
	CleanPartition(database, table, host, partition string) error

	// CheckPartition：校验目标端文件 md5 = pathInfo 中预期值
	CheckPartition(host, database, table, partition string, pathInfo map[string]model.PathInfo) error

	// Type：s3 / local
	Type() string
}
```

- [ ] **Step 4：跑测试 PASS**

```bash
go test ./service/backup/storage -v
```

- [ ] **Step 5：commit**

```bash
git add service/backup/storage/storage.go service/backup/storage/storage_test.go
git commit -m "feat(backup): Storage 抽象接口 + fake 实现的契约测试"
```

---

### Task 9：S3 实现（迁自现有）

**Files:**
- Create: `service/backup/storage/s3.go`
- Move from: `service/clickhouse/data_manage.go`（现有 `S3Client` / `CheckSum` / `Remove` / `CreateBucket`）
- Test: `service/backup/storage/s3_test.go`

老代码里已有的 `S3Client` 直接迁过来，包装成实现 `Storage` 接口的结构体。

- [ ] **Step 1：写测试（构造 SQL 不需要真 S3）**

```go
// service/backup/storage/s3_test.go
package storage

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestS3_BackupSQL(t *testing.T) {
	s := NewS3(model.TargetS3{
		Endpoint:        "http://s3.example.com:9000",
		Bucket:          "ckman-backup",
		AccessKeyID:     "AK",
		SecretAccessKey: "SK",
	})
	out := s.BackupSQL("dba", "events_log", "20250508", "20250508/dba.events_log/host1")
	if !strings.Contains(out, "S3('http://s3.example.com:9000/ckman-backup/20250508/dba.events_log/host1', 'AK', 'SK')") {
		t.Fatalf("unexpected sql: %s", out)
	}
	if !strings.Contains(out, "PARTITION '20250508'") {
		t.Fatalf("missing partition: %s", out)
	}
}

func TestS3_BackupSQL_AllPartition(t *testing.T) {
	s := NewS3(model.TargetS3{Endpoint: "http://x", Bucket: "b", AccessKeyID: "a", SecretAccessKey: "s"})
	out := s.BackupSQL("d", "t", "all", "all/d.t/h")
	if strings.Contains(out, "PARTITION") {
		t.Fatalf("'all' should not emit PARTITION clause: %s", out)
	}
}

func TestS3_RejectsBadEndpoint(t *testing.T) {
	s := NewS3(model.TargetS3{Endpoint: "ftp://nope"})
	if err := s.Init(); err == nil {
		t.Fatal("ftp endpoint should reject")
	}
}
```

- [ ] **Step 2：跑 fail**

- [ ] **Step 3：实现**

```go
// service/backup/storage/s3.go
package storage

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housepower/ckman/model"
)

type S3 struct {
	cfg model.TargetS3
	// client 字段：复用迁自 service/clickhouse/data_manage.go 的 S3Client（通过 minio-go 等），
	// 在 Init() 里建。这里不直接展开，移植时按现有代码结构搬。
	client *S3Client
}

func NewS3(cfg model.TargetS3) *S3 {
	return &S3{cfg: cfg}
}

func (s *S3) Init() error {
	if !strings.HasPrefix(s.cfg.Endpoint, "http://") && !strings.HasPrefix(s.cfg.Endpoint, "https://") {
		return errors.New("s3 endpoint must start with http:// or https://")
	}
	c, err := NewS3Client(s.cfg) // 迁自现有 NewS3Client
	if err != nil {
		return err
	}
	s.client = c
	_ = c.CreateBucket(s.cfg.Bucket) // 已存在不报错
	return nil
}

func (s *S3) Type() string { return model.BACKUP_S3 }

func (s *S3) BackupSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", escapeSQLLiteral(partition)))
	}
	sb.WriteString(fmt.Sprintf(" TO S3('%s/%s/%s', '%s', '%s')",
		s.cfg.Endpoint, s.cfg.Bucket, key,
		escapeSQLLiteral(s.cfg.AccessKeyID),
		escapeSQLLiteral(s.cfg.SecretAccessKey)))
	return sb.String()
}

func (s *S3) RestoreSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", escapeSQLLiteral(partition)))
	}
	sb.WriteString(fmt.Sprintf(" FROM S3('%s/%s/%s', '%s', '%s')",
		s.cfg.Endpoint, s.cfg.Bucket, key,
		escapeSQLLiteral(s.cfg.AccessKeyID),
		escapeSQLLiteral(s.cfg.SecretAccessKey)))
	return sb.String()
}

func (s *S3) CleanPartition(database, table, host, partition string) error {
	key := fmt.Sprintf("%s/%s.%s/%s", partition, database, table, host)
	return s.client.Remove(s.cfg.Bucket, key) // 现有方法
}

func (s *S3) CheckPartition(host, database, table, partition string, pathInfo map[string]model.PathInfo) error {
	key := fmt.Sprintf("%s/%s.%s/%s", partition, database, table, host)
	_, _, _, err := s.client.CheckSum(host, s.cfg.Bucket, key, pathInfo, s.cfg)
	return err
}

func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
```

`S3Client` / `NewS3Client` 在同包内移植自现 `service/clickhouse/data_manage.go`，去掉对老 `Back` 类型的依赖。

- [ ] **Step 4：跑测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/storage/s3.go service/backup/storage/s3_test.go
git commit -m "$(cat <<'EOF'
feat(backup): storage S3 实现，迁自 service/clickhouse/data_manage.go

- S3Client / CheckSum / Remove / CreateBucket 整体搬过来，去掉对老 Back 类型依赖
- BackupSQL/RestoreSQL 单引号转义防注入；endpoint 前缀校验

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10：Local 实现（新写，含 checksum + 路径白名单）

**Files:**
- Create: `service/backup/storage/local.go`
- Test: `service/backup/storage/local_test.go`

老 `service/clickhouse/data_manage.go` 里 Local 分支 Check 是 `// todo`，本 Plan 必须实现。

- [ ] **Step 1：测试（包括白名单边界）**

```go
// service/backup/storage/local_test.go
package storage

import (
	"testing"

	"github.com/housepower/ckman/model"
)

func TestLocal_RejectBadPath(t *testing.T) {
	for _, bad := range []string{"", "rel/path", "/data/../etc", "/data;rm -rf /", "/data $a"} {
		l := NewLocal(model.TargetLocal{Path: bad}, nil)
		if err := l.Init(); err == nil {
			t.Errorf("path %q should reject", bad)
		}
	}
}

func TestLocal_BackupSQL(t *testing.T) {
	l := NewLocal(model.TargetLocal{Path: "/data/backups"}, nil)
	if err := l.Init(); err != nil {
		t.Fatal(err)
	}
	out := l.BackupSQL("dba", "events_log", "20250508", "20250508/dba.events_log/host1")
	if !contains(out, "TO File('/data/backups/20250508/dba.events_log/host1')") {
		t.Fatalf("unexpected: %s", out)
	}
	if !contains(out, "PARTITION '20250508'") {
		t.Fatalf("missing partition: %s", out)
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (s == sub || (len(s) > len(sub) && (s[:len(sub)] == sub || contains(s[1:], sub)))) }
```

注意：路径白名单依赖 Task 2 的 `backup.ValidateLocalPath`。

- [ ] **Step 2：跑 fail**

- [ ] **Step 3：实现**

```go
// service/backup/storage/local.go
package storage

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/backup"
)

type Local struct {
	cfg     model.TargetLocal
	sshOpts func(host string) common.SshOptions // 注入：避免 storage 直接 import cluster
}

func NewLocal(cfg model.TargetLocal, sshOpts func(string) common.SshOptions) *Local {
	return &Local{cfg: cfg, sshOpts: sshOpts}
}

func (l *Local) Init() error {
	return backup.ValidateLocalPath(l.cfg.Path)
}

func (l *Local) Type() string { return model.BACKUP_LOCAL }

func (l *Local) BackupSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", strings.ReplaceAll(partition, "'", "''")))
	}
	sb.WriteString(fmt.Sprintf(" TO File('%s/%s')", l.cfg.Path, key))
	return sb.String()
}

func (l *Local) RestoreSQL(database, table, partition, key string) string {
	var sb strings.Builder
	if partition != "all" {
		sb.WriteString(fmt.Sprintf(" PARTITION '%s'", strings.ReplaceAll(partition, "'", "''")))
	}
	sb.WriteString(fmt.Sprintf(" FROM File('%s/%s')", l.cfg.Path, key))
	return sb.String()
}

func (l *Local) CleanPartition(database, table, host, partition string) error {
	if l.sshOpts == nil {
		return fmt.Errorf("sshOpts not configured")
	}
	opts := l.sshOpts(host)
	// 强校验数据库 / 表 / 分区名（identifier 范围）防 shell 注入
	for _, s := range []string{database, table, partition} {
		if err := backup.ValidateIdentifier(s); err != nil {
			return err
		}
	}
	cmd := fmt.Sprintf("rm -fr %q/%s.%s/%s/", l.cfg.Path, database, table, host)
	_, err := common.RemoteExecute(opts, cmd)
	return err
}

func (l *Local) CheckPartition(host, database, table, partition string,
	pathInfo map[string]model.PathInfo) error {
	if l.sshOpts == nil {
		return fmt.Errorf("sshOpts not configured")
	}
	opts := l.sshOpts(host)
	root := fmt.Sprintf("%s/%s.%s/%s", l.cfg.Path, database, table, host)
	cmd := fmt.Sprintf("find %q -type f -exec md5sum {} \\;", root)
	out, err := common.RemoteExecute(opts, cmd)
	if err != nil {
		return err
	}
	gotByPath := map[string]string{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		gotByPath[f[1]] = f[0]
	}
	// 与 pathInfo 中预期值比对
	for _, pi := range pathInfo {
		if pi.Host != host {
			continue
		}
		got, ok := gotByPath[pi.LPath]
		if !ok {
			return fmt.Errorf("local checksum: path missing %s on %s", pi.LPath, host)
		}
		if got != pi.MD5 {
			return fmt.Errorf("local checksum mismatch: %s on %s (got=%s expected=%s)", pi.LPath, host, got, pi.MD5)
		}
	}
	return nil
}
```

注：`sshOpts` 由 executor 在创建 storage 时按 cluster 信息注入；这样 storage 包不依赖 cluster 模型。

- [ ] **Step 4：测试 PASS**

```bash
go test ./service/backup/storage/...
```

- [ ] **Step 5：commit**

```bash
git add service/backup/storage/local.go service/backup/storage/local_test.go
git commit -m "$(cat <<'EOF'
feat(backup): storage Local 实现，含路径白名单 + checksum 完整逻辑

- Init 校验 ValidateLocalPath（修闭旧版 RCE 注入面 #9）
- CheckPartition 实现远程 md5sum 比对（修闭旧版 // todo #+衍生）
- 通过依赖注入 sshOpts 让 storage 包不依赖 cluster 模型

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Phase D：Service 骨架 (Tasks 11-14)

### Task 11：Pool（队列 + worker pool 合并）

**Files:**
- Create: `service/backup/pool.go`
- Test: `service/backup/pool_test.go`

- [ ] **Step 1：测试**

```go
// service/backup/pool_test.go
package backup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_DispatchToWorker(t *testing.T) {
	var ran int32
	exec := func(ctx context.Context, runID string) {
		atomic.AddInt32(&ran, 1)
	}
	p := NewPool(2, exec)
	p.Start(context.Background())
	defer p.Stop()

	if !p.Submit("r1") {
		t.Fatal("Submit r1 should succeed")
	}
	deadline := time.After(time.Second)
	for atomic.LoadInt32(&ran) == 0 {
		select {
		case <-deadline:
			t.Fatal("worker did not run")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestPool_QueueFullReturnsFalse(t *testing.T) {
	// 1 worker，队列容量 4，工作中 1 个 + 队列 4 个 = 5 个；第 6 个 Submit 应 false
	block := make(chan struct{})
	exec := func(ctx context.Context, runID string) { <-block }
	p := NewPool(1, exec)
	p.Start(context.Background())
	defer func() { close(block); p.Stop() }()

	for i := 0; i < 5; i++ {
		if !p.Submit("r") {
			t.Fatalf("submit %d should ok", i)
		}
	}
	if p.Submit("r-overflow") {
		t.Fatal("submit beyond capacity should return false")
	}
}

func TestPool_StopDoesNotInterruptRunning(t *testing.T) {
	done := make(chan string, 1)
	exec := func(ctx context.Context, runID string) {
		time.Sleep(50 * time.Millisecond)
		done <- runID
	}
	p := NewPool(1, exec)
	p.Start(context.Background())
	p.Submit("r1")
	time.Sleep(10 * time.Millisecond)
	p.Stop()
	select {
	case got := <-done:
		if got != "r1" {
			t.Fatalf("got %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("running run was interrupted")
	}
}
```

- [ ] **Step 2：fail**

- [ ] **Step 3：实现**

```go
// service/backup/pool.go
package backup

import (
	"context"
	"sync"
)

type ExecFunc func(ctx context.Context, runID string)

type Pool struct {
	workers   int
	queue     chan string
	exec      ExecFunc
	wg        sync.WaitGroup
	cancel    context.CancelFunc
	stopOnce  sync.Once
}

func NewPool(workers int, exec ExecFunc) *Pool {
	return &Pool{
		workers: workers,
		queue:   make(chan string, workers*4),
		exec:    exec,
	}
}

func (p *Pool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case runID, ok := <-p.queue:
			if !ok {
				return
			}
			p.exec(ctx, runID)
		}
	}
}

// Submit 非阻塞入队。队列满时返回 false。
func (p *Pool) Submit(runID string) bool {
	select {
	case p.queue <- runID:
		return true
	default:
		return false
	}
}

func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		close(p.queue)
		p.wg.Wait()
	})
}

// QueueLen 当前排队中的 run 数（用于 metric）
func (p *Pool) QueueLen() int { return len(p.queue) }
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/pool.go service/backup/pool_test.go
git commit -m "feat(backup): Pool 队列 + worker pool 合并实现"
```

---

### Task 12：Scheduler with Reconcile

**Files:**
- Create: `service/backup/scheduler.go`
- Test: `service/backup/scheduler_test.go`

- [ ] **Step 1：测试 — fake repo + fake cron 验证 reconcile diff 三种动作**

```go
// service/backup/scheduler_test.go
package backup

import (
	"sort"
	"sync"
	"testing"

	"github.com/housepower/ckman/model"
)

type fakeCron struct {
	mu   sync.Mutex
	jobs map[string]string // policy_id -> crontab
}

func newFakeCron() *fakeCron { return &fakeCron{jobs: map[string]string{}} }
func (f *fakeCron) Add(id, spec string, _ func() error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs[id] = spec
}
func (f *fakeCron) Remove(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.jobs, id)
}
func (f *fakeCron) snapshot() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]string{}
	for k, v := range f.jobs {
		out[k] = v
	}
	return out
}

type fakePolicyRepo struct{ items []model.BackupPolicy }

func (r *fakePolicyRepo) Active(_ string) []model.BackupPolicy { return r.items }

func TestScheduler_Reconcile_Add(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 3 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}}
	cr := newFakeCron()
	s := &Scheduler{self: "self", cron: cr, policy: repo, fn: func(model.BackupPolicy) {}}
	s.Reconcile()
	got := cr.snapshot()
	if len(got) != 1 || got["p1"] != "0 3 * * *" {
		t.Fatalf("expected p1=0 3 * * *, got %v", got)
	}
}

func TestScheduler_Reconcile_Remove(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{}}
	cr := newFakeCron()
	cr.jobs["p1"] = "0 3 * * *"
	s := &Scheduler{self: "self", cron: cr, policy: repo, fn: func(model.BackupPolicy) {}}
	s.Reconcile()
	if _, ok := cr.snapshot()["p1"]; ok {
		t.Fatal("p1 should be removed")
	}
}

func TestScheduler_Reconcile_CrontabChange(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 5 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}}
	cr := newFakeCron()
	cr.jobs["p1"] = "0 3 * * *"
	s := &Scheduler{self: "self", cron: cr, policy: repo, fn: func(model.BackupPolicy) {}}
	s.Reconcile()
	got := cr.snapshot()
	if got["p1"] != "0 5 * * *" {
		t.Fatalf("expected updated crontab, got %v", got)
	}
}

func TestScheduler_Reconcile_StableNoOp(t *testing.T) {
	repo := &fakePolicyRepo{items: []model.BackupPolicy{
		{PolicyID: "p1", Crontab: "0 3 * * *", Enabled: true, ScheduleType: model.BACKUP_SCHEDULED, Instance: "self"},
	}}
	cr := newFakeCron()
	cr.jobs["p1"] = "0 3 * * *"
	added := 0
	cr2 := &fakeCronCounting{fakeCron: cr, addCounter: &added}
	s := &Scheduler{self: "self", cron: cr2, policy: repo, fn: func(model.BackupPolicy) {}}
	s.Reconcile()
	if added != 0 {
		t.Fatalf("stable reconcile should not Add: %d", added)
	}
}

type fakeCronCounting struct {
	*fakeCron
	addCounter *int
}

func (f *fakeCronCounting) Add(id, spec string, fn func() error) {
	*f.addCounter++
	f.fakeCron.Add(id, spec, fn)
}

// 排序辅助
func keys(m map[string]string) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 2：fail**

- [ ] **Step 3：实现**

```go
// service/backup/scheduler.go
package backup

import (
	"context"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

const ReconcileInterval = 60 * time.Second

// CronAdapter 允许测试时注入 fake cron
type CronAdapter interface {
	Add(id, spec string, fn func() error)
	Remove(id string)
}

// PolicyRepo 允许测试时注入 fake repo
type PolicyRepo interface {
	Active(instance string) []model.BackupPolicy
}

type Scheduler struct {
	self    string
	cron    CronAdapter
	policy  PolicyRepo
	fn      func(model.BackupPolicy) // 真实业务：触发一次 Submit
	tracked map[string]string         // policy_id -> registered crontab
	cancel  context.CancelFunc
}

func NewScheduler(self string, cron CronAdapter, policy PolicyRepo, fn func(model.BackupPolicy)) *Scheduler {
	return &Scheduler{self: self, cron: cron, policy: policy, fn: fn, tracked: map[string]string{}}
}

func (s *Scheduler) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.Reconcile() // 启动时立即跑一次
	go func() {
		t := time.NewTicker(ReconcileInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.Reconcile()
			}
		}
	}()
}

func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

// Reconcile：拉 active policies → diff → add/remove/replace cron jobs
func (s *Scheduler) Reconcile() {
	policies := s.policy.Active(s.self)
	want := map[string]string{}
	for _, p := range policies {
		want[p.PolicyID] = p.Crontab
	}

	// 删除：在 tracked 中但不在 want 中
	for id := range s.tracked {
		if _, ok := want[id]; !ok {
			s.cron.Remove(id)
			delete(s.tracked, id)
			log.Logger.Infof("[scheduler] reconcile remove %s", id)
		}
	}

	// 增加 / 替换
	for _, p := range policies {
		registered, exists := s.tracked[p.PolicyID]
		if exists && registered == p.Crontab {
			continue // 不变
		}
		if exists {
			s.cron.Remove(p.PolicyID) // crontab 变了
			log.Logger.Infof("[scheduler] reconcile replace %s: %s -> %s", p.PolicyID, registered, p.Crontab)
		} else {
			log.Logger.Infof("[scheduler] reconcile add %s: %s", p.PolicyID, p.Crontab)
		}
		policy := p // 捕获副本
		s.cron.Add(p.PolicyID, p.Crontab, func() error {
			s.fn(policy)
			return nil
		})
		s.tracked[p.PolicyID] = p.Crontab
	}
}
```

- [ ] **Step 4：测试 PASS**

```bash
go test ./service/backup -run "TestScheduler_" -v
```

- [ ] **Step 5：commit**

```bash
git add service/backup/scheduler.go service/backup/scheduler_test.go
git commit -m "$(cat <<'EOF'
feat(backup): Scheduler 周期性 Reconcile

每 60 秒从 repository 拉 active policies，diff 当前 cron 注册表：
- want 缺：Add
- have 缺：Remove
- crontab 变化：Remove + Add

支持运行时编辑 / 故障转移 / 启停 等，无需重启 ckman。
通过 CronAdapter 与 PolicyRepo 接口便于单测；ReconcileInterval = 60s。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 13：BackupService（Submit / SubmitRestore / 查询）

**Files:**
- Create: `service/backup/service.go`
- Test: `service/backup/service_test.go`

- [ ] **Step 1：测试 — overlap / queue full / 跨实例 拒绝 / 正常入队**

```go
// service/backup/service_test.go
package backup

import (
	"errors"
	"testing"
	"time"

	"github.com/housepower/ckman/model"
)

type memRepo struct {
	policies map[string]model.BackupPolicy
	runs     map[string]model.BackupRun
}

func newMemRepo() *memRepo {
	return &memRepo{policies: map[string]model.BackupPolicy{}, runs: map[string]model.BackupRun{}}
}

func (r *memRepo) CreatePolicy(p model.BackupPolicy) error    { r.policies[p.PolicyID] = p; return nil }
func (r *memRepo) GetPolicy(id string) (model.BackupPolicy, error) {
	p, ok := r.policies[id]
	if !ok {
		return model.BackupPolicy{}, errors.New("not found")
	}
	return p, nil
}
func (r *memRepo) CreateRun(rn model.BackupRun) error    { r.runs[rn.RunID] = rn; return nil }
func (r *memRepo) GetRun(id string) (model.BackupRun, error) {
	rn, ok := r.runs[id]
	if !ok {
		return model.BackupRun{}, errors.New("not found")
	}
	return rn, nil
}
func (r *memRepo) InFlightRunsByPolicy(policyID string) []model.BackupRun {
	var out []model.BackupRun
	for _, rn := range r.runs {
		if rn.PolicyID == policyID && (rn.Status == model.BACKUP_STATUS_QUEUED || rn.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, rn)
		}
	}
	return out
}

type fakePool struct {
	full bool
	in   []string
}

func (f *fakePool) Submit(id string) bool {
	if f.full {
		return false
	}
	f.in = append(f.in, id)
	return true
}

func TestService_Submit_NormalCase(t *testing.T) {
	repo := newMemRepo()
	pool := &fakePool{}
	svc := &Service{self: "ckman-01", repo: repo, pool: pool, now: func() time.Time { return time.Unix(1, 0) }}

	policy := model.BackupPolicy{
		PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_IMMEDIATE, Instance: "ckman-01", Enabled: false,
	}
	repo.policies["p1"] = policy
	runID, err := svc.SubmitForPolicy(policy, model.TRIGGER_MANUAL_IMMEDIATE)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if runID == "" {
		t.Fatal("empty runID")
	}
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_QUEUED {
		t.Fatalf("run not queued: %+v", rn)
	}
	if len(pool.in) != 1 || pool.in[0] != runID {
		t.Fatalf("pool not enqueued: %+v", pool.in)
	}
}

func TestService_Submit_OverlapWritesSkipped(t *testing.T) {
	repo := newMemRepo()
	policy := model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1",
		ScheduleType: model.BACKUP_SCHEDULED, Instance: "ckman-01"}
	repo.policies["p1"] = policy
	repo.runs["r-prev"] = model.BackupRun{
		RunID: "r-prev", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING,
	}
	pool := &fakePool{}
	svc := &Service{self: "ckman-01", repo: repo, pool: pool, now: time.Now}

	runID, err := svc.SubmitForPolicy(policy, model.TRIGGER_CRON)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_SKIPPED || rn.StatusReason != model.REASON_OVERLAP {
		t.Fatalf("expected skipped(overlap), got %+v", rn)
	}
	if len(pool.in) != 0 {
		t.Fatal("pool should not receive overlap-skipped run")
	}
}

func TestService_Submit_QueueFullWritesSkipped(t *testing.T) {
	repo := newMemRepo()
	policy := model.BackupPolicy{PolicyID: "p1", Instance: "ckman-01", ScheduleType: model.BACKUP_SCHEDULED}
	repo.policies["p1"] = policy
	pool := &fakePool{full: true}
	svc := &Service{self: "ckman-01", repo: repo, pool: pool, now: time.Now}

	runID, err := svc.SubmitForPolicy(policy, model.TRIGGER_CRON)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_SKIPPED || rn.StatusReason != model.REASON_QUEUE_FULL {
		t.Fatalf("expected skipped(queue_full), got %+v", rn)
	}
}

func TestService_Submit_DisabledPolicyWritesSkipped(t *testing.T) {
	repo := newMemRepo()
	policy := model.BackupPolicy{PolicyID: "p1", Instance: "ckman-01",
		ScheduleType: model.BACKUP_SCHEDULED, Enabled: false}
	repo.policies["p1"] = policy
	pool := &fakePool{}
	svc := &Service{self: "ckman-01", repo: repo, pool: pool, now: time.Now}

	runID, _ := svc.SubmitForPolicy(policy, model.TRIGGER_CRON)
	rn, _ := repo.GetRun(runID)
	if rn.Status != model.BACKUP_STATUS_SKIPPED || rn.StatusReason != model.REASON_DISABLED {
		t.Fatalf("expected skipped(disabled), got %+v", rn)
	}
}
```

- [ ] **Step 2：fail**

- [ ] **Step 3：实现 + 接口抽象**

```go
// service/backup/service.go
package backup

import (
	"errors"
	"time"

	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

type ServiceRepo interface {
	CreatePolicy(p model.BackupPolicy) error
	GetPolicy(id string) (model.BackupPolicy, error)
	CreateRun(r model.BackupRun) error
	GetRun(id string) (model.BackupRun, error)
	InFlightRunsByPolicy(policyID string) []model.BackupRun
}

type ServicePool interface {
	Submit(runID string) bool
}

type Service struct {
	self string
	repo ServiceRepo
	pool ServicePool
	now  func() time.Time
}

func NewService(self string, repo ServiceRepo, pool ServicePool) *Service {
	return &Service{self: self, repo: repo, pool: pool, now: time.Now}
}

// SubmitForPolicy 给已存在的 policy 生成一次 run；trigger ∈ TRIGGER_*
// 返回 runID（即便是 skipped 状态也返回，便于台账定位）
func (s *Service) SubmitForPolicy(p model.BackupPolicy, trigger string) (string, error) {
	if p.PolicyID == "" {
		return "", errors.New("policy id empty")
	}

	now := s.now()
	run := model.BackupRun{
		RunID:       uuid.New(),
		PolicyID:    p.PolicyID,
		ClusterName: p.ClusterName,
		Database:    p.Database,
		Table:       p.Table,
		Operation:   model.OP_BACKUP,
		TriggerType: trigger,
		Instance:    p.Instance,
		Status:      model.BACKUP_STATUS_QUEUED,
		CreateTime:  now,
	}

	// 1. policy.enabled=false 但仍被触发（cron 与 reconcile 间窗口）
	if p.ScheduleType == model.BACKUP_SCHEDULED && !p.Enabled {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_DISABLED
		run.FinishedAt = now
		return run.RunID, s.repo.CreateRun(run)
	}

	// 2. 重叠检测
	inFlight := s.repo.InFlightRunsByPolicy(p.PolicyID)
	if len(inFlight) > 0 {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_OVERLAP
		run.FinishedAt = now
		return run.RunID, s.repo.CreateRun(run)
	}

	// 3. 持久化为 queued
	if err := s.repo.CreateRun(run); err != nil {
		return "", err
	}

	// 4. 入队；满则改为 skipped(queue_full)
	if !s.pool.Submit(run.RunID) {
		run.Status = model.BACKUP_STATUS_SKIPPED
		run.StatusReason = model.REASON_QUEUE_FULL
		run.FinishedAt = s.now()
		log.Logger.Warnf("[backup] queue full, run %s skipped", run.RunID)
		// best effort，update 失败也认了；台账状态会停在 queued，启动时会被重置 interrupted
		if upErr := s.updateRunSafe(run); upErr != nil {
			log.Logger.Errorf("[backup] update queue_full run failed: %v", upErr)
		}
	}

	return run.RunID, nil
}

func (s *Service) updateRunSafe(r model.BackupRun) error {
	// 在仅有 ServiceRepo 时这是 stub；真实使用时通过 PersistentMgr.UpdateBackupRun
	// 后续 task 把 ServiceRepo 扩展为带 UpdateRun 的接口
	return nil
}
```

> 注：测试里 `Service` 字段用了 unexported 的小写名，因为测试在同包（`package backup`）。

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/service.go service/backup/service_test.go
git commit -m "$(cat <<'EOF'
feat(backup): BackupService.SubmitForPolicy 主路径

- 检查 policy.enabled（修闭 disable+cron 触发窗口）
- 重叠检测：调用 InFlightRunsByPolicy 命中即写 skipped(overlap)
- 入队失败写 skipped(queue_full)
- 通过 ServiceRepo / ServicePool 接口支持测试 mock

注：SubmitRestore / Boot interrupted 标记 / 完整 ServiceRepo 在后续 task 补。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 14：Boot — 启动时把残留 in-flight run 标 interrupted

**Files:**
- Modify: `service/backup/service.go`（新增 Boot 方法）
- Test: `service/backup/service_test.go` 追加用例

- [ ] **Step 1：测试**

```go
func TestService_Boot_MarksInFlightAsInterrupted(t *testing.T) {
	repo := newMemRepo()
	repo.runs["r-running"] = model.BackupRun{RunID: "r-running", Status: model.BACKUP_STATUS_RUNNING, Instance: "ckman-01"}
	repo.runs["r-queued"] = model.BackupRun{RunID: "r-queued", Status: model.BACKUP_STATUS_QUEUED, Instance: "ckman-01"}
	repo.runs["r-other"] = model.BackupRun{RunID: "r-other", Status: model.BACKUP_STATUS_RUNNING, Instance: "ckman-02"}

	svc := &Service{self: "ckman-01", repo: repo, pool: &fakePool{}, now: time.Now}
	if err := svc.Boot(); err != nil {
		t.Fatalf("boot: %v", err)
	}
	r1, _ := repo.GetRun("r-running")
	if r1.Status != model.BACKUP_STATUS_INTERRUPTED {
		t.Fatalf("running should be interrupted: %+v", r1)
	}
	r2, _ := repo.GetRun("r-queued")
	if r2.Status != model.BACKUP_STATUS_INTERRUPTED {
		t.Fatalf("queued should be interrupted: %+v", r2)
	}
	r3, _ := repo.GetRun("r-other")
	if r3.Status != model.BACKUP_STATUS_RUNNING {
		t.Fatalf("other instance unaffected: %+v", r3)
	}
}
```

注意：`memRepo` 需要一个新方法 `InFlightRunsByInstance(instance string) []model.BackupRun` 与 `UpdateRun(model.BackupRun) error`，相应在 `ServiceRepo` 接口与 `memRepo` fake 中加进去。

- [ ] **Step 2：fail**

- [ ] **Step 3：实现 Boot + 扩接口**

```go
// service/backup/service.go 内补

type ServiceRepo interface {
	CreatePolicy(p model.BackupPolicy) error
	GetPolicy(id string) (model.BackupPolicy, error)
	CreateRun(r model.BackupRun) error
	UpdateRun(r model.BackupRun) error // 新增
	GetRun(id string) (model.BackupRun, error)
	InFlightRunsByPolicy(policyID string) []model.BackupRun
	InFlightRunsByInstance(instance string) []model.BackupRun // 新增
}

func (s *Service) Boot() error {
	now := s.now()
	for _, r := range s.repo.InFlightRunsByInstance(s.self) {
		r.Status = model.BACKUP_STATUS_INTERRUPTED
		r.StatusReason = model.REASON_RESTART
		r.FinishedAt = now
		if err := s.repo.UpdateRun(r); err != nil {
			log.Logger.Errorf("[backup] boot mark interrupted run=%s: %v", r.RunID, err)
		}
	}
	return nil
}
```

`updateRunSafe` 改用接口方法：

```go
func (s *Service) updateRunSafe(r model.BackupRun) error { return s.repo.UpdateRun(r) }
```

memRepo 的扩展：

```go
func (r *memRepo) UpdateRun(rn model.BackupRun) error                       { r.runs[rn.RunID] = rn; return nil }
func (r *memRepo) InFlightRunsByInstance(ins string) []model.BackupRun {
	var out []model.BackupRun
	for _, rn := range r.runs {
		if rn.Instance == ins && (rn.Status == model.BACKUP_STATUS_QUEUED || rn.Status == model.BACKUP_STATUS_RUNNING) {
			out = append(out, rn)
		}
	}
	return out
}
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/service.go service/backup/service_test.go
git commit -m "feat(backup): Service.Boot 启动时把本实例残留 in-flight run 标 interrupted"
```

---

## Phase E：Executor (Tasks 15-19)

> Phase E 把 `service/clickhouse/data_manage.go` 里的执行逻辑搬到 `service/backup/executor.go`，**搬迁的同时修闭** spec §2 / §8 列的所有 Bug：
>
> - **#3 Check 早 return**：return 移到 for 循环外
> - **#4 Init 丢分区**：`GetBackupByTable` 报错时不丢新枚举的分区
> - **#5 Prepare race**：用 `errgroup`
> - **#7 Close 静默失败**：DROP PARTITION 错误不再吞
> - **#8 SetStatus 错误**：repository 返回错误一律检查
> - **#9 SQL/Shell 注入**：identifier / 路径走 Task 2 校验
>
> Local checksum 已在 Task 10 实现，Executor.Check 直接调 `storage.CheckPartition` 不再分支。

### Task 15：Executor 骨架 + Init 阶段

**Files:**
- Create: `service/backup/executor.go`
- Test: `service/backup/executor_test.go`

- [ ] **Step 1：测试**

```go
// service/backup/executor_test.go
package backup

import (
	"context"
	"errors"
	"testing"

	"github.com/housepower/ckman/model"
)

type fakeExecRepo struct {
	*memRepo
	policy model.BackupPolicy
}

func (r *fakeExecRepo) GetPolicyForRun(policyID string) (model.BackupPolicy, error) {
	return r.policy, nil
}

func TestExecutor_Init_NoShardConnect(t *testing.T) {
	repo := &fakeExecRepo{memRepo: newMemRepo(), policy: model.BackupPolicy{PolicyID: "p1", ClusterName: "missing"}}
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1", Status: model.BACKUP_STATUS_RUNNING}

	e := &Executor{
		repo:        repo,
		connFactory: func(string) ([]*shardConn, error) { return nil, errors.New("cluster not found") },
	}
	err := e.Run(context.Background(), "r1")
	rn, _ := repo.GetRun("r1")
	if err == nil || rn.Status != model.BACKUP_STATUS_FAILED {
		t.Fatalf("expected failed, got status=%s err=%v", rn.Status, err)
	}
}

func TestExecutor_Init_PartitionListErrorDoesNotDropList(t *testing.T) {
	// 模拟现有 GetBackupByTable 报错，但本次新枚举的分区仍被采用（修 #4）
	repo := &fakeExecRepo{memRepo: newMemRepo()}
	repo.policy = model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA", Database: "dba", Table: "t1", DaysBefore: 7}
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
		// fakeExecRepo 的 GetBackupByTable 故意返回错（代表持久层暂时挂掉）
		getLastRunPartitions: func(cluster, db, table string) ([]model.BackupRunPartition, error) {
			called = true
			return nil, errors.New("repo down")
		},
		stages: stagesStub{},
	}
	_ = e.Init(context.Background(), "r1")
	if !called {
		t.Fatal("getLastRunPartitions not called")
	}
	rn, _ := repo.GetRun("r1")
	if len(rn.Partitions) != 2 {
		t.Fatalf("expected 2 partitions despite repo error, got %d (#4 regression!)", len(rn.Partitions))
	}
}
```

`shardConn` / `newFakeShardConn` / `stagesStub` 是后续 task 补的轻量假体；这里先把 Init 关注的两个语义钉住。

- [ ] **Step 2：fail**

- [ ] **Step 3：实现 Init**

```go
// service/backup/executor.go
package backup

import (
	"context"
	"fmt"
	"time"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

type ExecRepo interface {
	GetRun(id string) (model.BackupRun, error)
	UpdateRun(r model.BackupRun) error
	GetPolicyForRun(policyID string) (model.BackupPolicy, error)
}

type shardConn struct {
	host string
	// 真实实现包含 *common.Conn；测试用 fake
}

type Executor struct {
	repo                 ExecRepo
	connFactory          func(cluster string) ([]*shardConn, error)
	listPartitions       func(c *shardConn, db, table, beforeYYYYMMDD string) ([]string, error)
	getLastRunPartitions func(cluster, db, table string) ([]model.BackupRunPartition, error)
	stages               stages // 见 Task 16-19
	now                  func() time.Time
}

// Run 是 worker 调用的入口
func (e *Executor) Run(ctx context.Context, runID string) error {
	if err := e.Init(ctx, runID); err != nil {
		return e.markFailed(runID, "init: "+err.Error())
	}
	r, _ := e.repo.GetRun(runID)
	if r.Operation == model.OP_BACKUP {
		if err := e.stages.Prepare(ctx, e, runID); err != nil {
			return e.markFailed(runID, "prepare: "+err.Error())
		}
		if err := e.stages.Backup(ctx, e, runID); err != nil {
			return e.markFailed(runID, "backup: "+err.Error())
		}
		// Check 仅当 checksum=true 时执行
		policy, _ := e.repo.GetPolicyForRun(r.PolicyID)
		if policy.Checksum {
			if err := e.stages.Check(ctx, e, runID); err != nil {
				return e.markFailed(runID, "check: "+err.Error())
			}
		}
	} else if r.Operation == model.OP_RESTORE {
		if err := e.stages.Restore(ctx, e, runID); err != nil {
			return e.markFailed(runID, "restore: "+err.Error())
		}
	} else {
		return e.markFailed(runID, "unknown operation: "+r.Operation)
	}
	if err := e.stages.Close(ctx, e, runID); err != nil {
		return e.markFailed(runID, "close: "+err.Error())
	}
	return e.markSuccess(runID)
}

func (e *Executor) Init(ctx context.Context, runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	policy, err := e.repo.GetPolicyForRun(r.PolicyID)
	if err != nil {
		return err
	}
	conns, err := e.connFactory(policy.ClusterName)
	if err != nil {
		return fmt.Errorf("connect cluster %s: %w", policy.ClusterName, err)
	}
	if len(conns) == 0 {
		return fmt.Errorf("no shard reachable for cluster %s", policy.ClusterName)
	}
	// 列分区（仅 daily 模式且 daysBefore > 0）
	if policy.BackupType == model.BACKUP_TYPE_DAILY_PARTITION && policy.DaysBefore > 0 {
		toPartition := time.Now().AddDate(0, 0, -policy.DaysBefore).Format("20060102")
		newPartitions, err := e.listPartitions(conns[0], policy.Database, policy.Table, toPartition)
		if err != nil {
			return err
		}
		// 修 #4：GetBackupByTable 报错时仍采用 newPartitions，不丢
		prev, _ := e.getLastRunPartitions(policy.ClusterName, policy.Database, policy.Table)
		merged := mergePartitionLists(prev, newPartitions)
		r.Partitions = merged
	}
	// 写回
	r.StartedAt = time.Now()
	if err := e.repo.UpdateRun(r); err != nil {
		return err
	}
	// 把 conns 暂存到 executor 的 run-scoped 结构（实现细节，省略示意）
	return nil
}

func mergePartitionLists(prev []model.BackupRunPartition, newest []string) []model.BackupRunPartition {
	have := map[string]bool{}
	for _, p := range prev {
		have[p.Partition] = true
	}
	out := append([]model.BackupRunPartition{}, prev...)
	for _, n := range newest {
		if !have[n] {
			out = append(out, model.BackupRunPartition{
				Partition: n, Status: model.BACKUP_PARTITION_STATUS_WAITING,
			})
		}
	}
	return out
}

func (e *Executor) markFailed(runID, reason string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		log.Logger.Errorf("[exec] cannot mark failed, get run %s: %v", runID, err)
		return err
	}
	r.Status = model.BACKUP_STATUS_FAILED
	r.ErrorMsg = reason
	r.FinishedAt = time.Now()
	r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	if upErr := e.repo.UpdateRun(r); upErr != nil {
		log.Logger.Errorf("[exec] update failed run %s: %v", runID, upErr)
	}
	return fmt.Errorf("run %s failed: %s", runID, reason)
}

func (e *Executor) markSuccess(runID string) error {
	r, err := e.repo.GetRun(runID)
	if err != nil {
		return err
	}
	r.Status = model.BACKUP_STATUS_SUCCESS
	r.FinishedAt = time.Now()
	r.Elapsed = int(r.FinishedAt.Sub(r.StartedAt).Seconds())
	return e.repo.UpdateRun(r)
}

// stages 用接口分阶段，便于 Task 16-19 单独 TDD 各阶段
type stages interface {
	Prepare(ctx context.Context, e *Executor, runID string) error
	Backup(ctx context.Context, e *Executor, runID string) error
	Restore(ctx context.Context, e *Executor, runID string) error
	Check(ctx context.Context, e *Executor, runID string) error
	Close(ctx context.Context, e *Executor, runID string) error
}

type stagesStub struct{}

func (stagesStub) Prepare(context.Context, *Executor, string) error { return nil }
func (stagesStub) Backup(context.Context, *Executor, string) error  { return nil }
func (stagesStub) Restore(context.Context, *Executor, string) error { return nil }
func (stagesStub) Check(context.Context, *Executor, string) error   { return nil }
func (stagesStub) Close(context.Context, *Executor, string) error   { return nil }

func newFakeShardConn(host string) *shardConn { return &shardConn{host: host} }
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/executor.go service/backup/executor_test.go
git commit -m "$(cat <<'EOF'
feat(backup): Executor 骨架 + Init 阶段

修 #4：Init 列分区时即便 getLastRunPartitions 报错仍采用本次新枚举的分区，
不再静默丢分区导致 run "成功但啥都没备"。

stages 接口分 Prepare/Backup/Restore/Check/Close 五段，便于后续单独 TDD。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 16：Prepare 阶段（修 #5 race + rows.Close 泄漏）

**Files:**
- Modify: `service/backup/executor.go`（新增 `realStages.Prepare`）
- Test: `service/backup/executor_test.go` 追加用例

**前置：** `realStages` 是 `stages` 接口的真实实现，与 Task 15 的 `stagesStub` 对应。本 task 创建 `realStages` 类型并实现 `Prepare`。

- [ ] **Step 1：写测试**

```go
// service/backup/executor_test.go 追加
func TestRealStages_Prepare_ChecksumErrorAggregated(t *testing.T) {
	// 3 个 host 同时跑 md5sum，其中 host2 失败；errgroup 应聚合错误（修 #5 race）
	// 旧实现里多 goroutine 共享 lastErr，可能丢错或并发写
	closed := map[string]bool{}
	conns := []*shardConn{newFakeShardConn("h1"), newFakeShardConn("h2"), newFakeShardConn("h3")}
	queryFn := func(host string) (queryResult, error) {
		if host == "h2" {
			return nil, errors.New("h2 down")
		}
		return &fakeQueryRows{closeFn: func() { closed[host] = true }}, nil
	}
	e := &Executor{
		conns: conns, queryRows: queryFn,
		repo: newMemRepoWithRun(model.BackupRun{
			RunID: "r1", PolicyID: "p1",
			Partitions: []model.BackupRunPartition{{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING}},
		}),
	}
	rs := realStages{}
	err := rs.Prepare(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "h2 down") {
		t.Fatalf("expected error containing 'h2 down', got %v", err)
	}
	// 即便有错误，已 query 成功的 rows 必须被 close（修 rows.Close 泄漏）
	if !closed["h1"] || !closed["h3"] {
		t.Fatalf("rows.Close should be called on successful conns, got %+v", closed)
	}
}

func TestRealStages_Prepare_CleanFailureFailsRun(t *testing.T) {
	// storage.CleanPartition 失败 → 整体 run failed，不静默
	e := &Executor{
		conns: []*shardConn{newFakeShardConn("h1")},
		queryRows: func(string) (queryResult, error) { return &fakeQueryRows{}, nil },
		storage: &fakeStorage{cleanErr: errors.New("rm permission denied")},
		repo: newMemRepoWithRun(model.BackupRun{
			RunID: "r1", PolicyID: "p1",
			Partitions: []model.BackupRunPartition{{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING}},
		}),
	}
	rs := realStages{}
	err := rs.Prepare(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "rm permission denied") {
		t.Fatalf("clean failure should fail run: %v", err)
	}
}
```

`fakeQueryRows` / `queryResult` / `fakeStorage` 是辅助类型；放在 executor_test.go 顶部：

```go
type queryResult interface{ Close() }
type fakeQueryRows struct{ closeFn func() }
func (f *fakeQueryRows) Close() { if f.closeFn != nil { f.closeFn() } }

type fakeStorage struct {
	cleanErr error
}
func (f *fakeStorage) Init() error                                                              { return nil }
func (f *fakeStorage) BackupSQL(d, t, p, k string) string                                       { return "" }
func (f *fakeStorage) RestoreSQL(d, t, p, k string) string                                      { return "" }
func (f *fakeStorage) CleanPartition(d, t, h, p string) error                                    { return f.cleanErr }
func (f *fakeStorage) CheckPartition(h, d, t, p string, _ map[string]model.PathInfo) error      { return nil }
func (f *fakeStorage) Type() string                                                              { return "fake" }

func newMemRepoWithRun(r model.BackupRun) *fakeExecRepo {
	repo := &fakeExecRepo{memRepo: newMemRepo(), policy: model.BackupPolicy{PolicyID: r.PolicyID}}
	repo.runs[r.RunID] = r
	return repo
}
```

`Executor` 需要新增字段：`conns []*shardConn`、`queryRows func(host string) (queryResult, error)`、`storage storage.Storage`。

- [ ] **Step 2：跑测试，确认 fail**

```bash
go test ./service/backup -run "TestRealStages_Prepare_" -v
# FAIL — realStages 未定义
```

- [ ] **Step 3：实现**

`Executor` 加字段（修改 Task 15 的定义）：

```go
type Executor struct {
	repo                 ExecRepo
	connFactory          func(cluster string) ([]*shardConn, error)
	listPartitions       func(c *shardConn, db, table, beforeYYYYMMDD string) ([]string, error)
	getLastRunPartitions func(cluster, db, table string) ([]model.BackupRunPartition, error)
	stages               stages
	now                  func() time.Time

	// run-scoped（Init 阶段填，后续阶段读）
	conns     []*shardConn
	queryRows func(host string) (queryResult, error)
	storage   Storage
}

type Storage = storageiface.Storage // alias to service/backup/storage.Storage
```

实现 `realStages.Prepare`：

```go
import (
	"golang.org/x/sync/errgroup"
)

type realStages struct{}

func (realStages) Prepare(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil { return err }

	// 查 size/rows（迁自旧代码，省略；按行数/大小用 cluster() 单 conn 查即可）
	if err := e.queryPartitionSizes(ctx, &run); err != nil {
		return err
	}
	if err := e.repo.UpdateRun(run); err != nil {
		return fmt.Errorf("update partition sizes: %w", err)
	}

	// checksum 阶段：errgroup 替代旧 lastErr race
	policy, _ := e.repo.GetPolicyForRun(run.PolicyID)
	if policy.Checksum {
		g, gctx := errgroup.WithContext(ctx)
		for _, c := range e.conns {
			c := c
			g.Go(func() error {
				rows, err := e.queryRows(c.host)
				if err != nil { return fmt.Errorf("[%s] query: %w", c.host, err) }
				defer rows.Close() // 即便后续报错也 close
				_ = gctx           // 留给具体 md5 收集逻辑取消用
				return e.collectChecksumOnHost(c, &run)
			})
		}
		if err := g.Wait(); err != nil { return err }
		if err := e.repo.UpdateRun(run); err != nil {
			return fmt.Errorf("update checksum info: %w", err)
		}
	}

	// 清旧目标端残留：失败立即终止 run（不静默）
	for _, p := range run.Partitions {
		if p.Status != model.BACKUP_PARTITION_STATUS_WAITING { continue }
		for _, c := range e.conns {
			if err := e.storage.CleanPartition(run.Database, run.Table, c.host, p.Partition); err != nil {
				return fmt.Errorf("clean %s on %s: %w", p.Partition, c.host, err)
			}
		}
	}
	return nil
}

// queryPartitionSizes / collectChecksumOnHost 是私有 helper，迁自
// service/clickhouse/data_manage.go 同名逻辑，去掉 *Back 依赖；这里不展开
// 但 collectChecksumOnHost 必须只 append PathInfo，不再写 lastErr
```

- [ ] **Step 4：跑测试 PASS**

```bash
go test ./service/backup -run "TestRealStages_Prepare_" -v
```

- [ ] **Step 5：commit**

```bash
git add service/backup/executor.go service/backup/executor_test.go
git commit -m "$(cat <<'EOF'
feat(backup): Executor.Prepare 用 errgroup 闭合 race + rows.Close 泄漏

修 #5：checksum 阶段多 goroutine 共享 lastErr 的 race 用 errgroup 替换；
所有 query rows 在错误路径下也走 defer rows.Close()。
清旧目录走 storage.CleanPartition（路径白名单已在 storage.Local.Init 校验），
任一失败立即冒泡使 run failed。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 17：BackupData 阶段（partition 隔离 + 状态机串行写）

**Files:**
- Modify: `service/backup/executor.go`（新增 `realStages.Backup`）
- Test: `service/backup/executor_test.go` 追加用例

- [ ] **Step 1：写测试**

```go
func TestRealStages_Backup_PartitionIsolation(t *testing.T) {
	// 3 个 partition，第 2 个跑 BACKUP TABLE 失败；其余继续；run 整体 failed
	conns := []*shardConn{newFakeShardConn("h1")}
	execCalls := []string{}
	execFn := func(host, sql string) error {
		execCalls = append(execCalls, sql)
		if strings.Contains(sql, "20250508") {
			return errors.New("backup engine OOM")
		}
		return nil
	}
	run := model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_WAITING},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{
		repo:    newMemRepoWithRun(run),
		conns:   conns,
		execSQL: execFn,
		storage: &fakeStorage{},
	}
	rs := realStages{}
	err := rs.Backup(context.Background(), e, "r1")
	if err == nil {
		t.Fatal("expected error since one partition failed")
	}
	got, _ := e.repo.GetRun("r1")
	statuses := []string{}
	for _, p := range got.Partitions { statuses = append(statuses, p.Status) }
	want := []string{"success", "failed", "success"}
	for i := range want {
		if statuses[i] != want[i] {
			t.Fatalf("partition[%d] status got=%s want=%s", i, statuses[i], want[i])
		}
	}
	if len(execCalls) != 3 {
		t.Fatalf("expected 3 BACKUP attempts (no traction), got %d", len(execCalls))
	}
}
```

`Executor.execSQL` 字段：`func(host, sql string) error`，注入便于 mock。

- [ ] **Step 2：fail**

- [ ] **Step 3：实现**

```go
func (realStages) Backup(ctx context.Context, e *Executor, runID string) error {
	run, _ := e.repo.GetRun(runID)
	var anyFail bool

	for i := range run.Partitions {
		p := &run.Partitions[i]
		if p.Status != model.BACKUP_PARTITION_STATUS_WAITING { continue }
		p.Status = model.BACKUP_PARTITION_STATUS_RUNNING
		if err := e.repo.UpdateRun(run); err != nil { return fmt.Errorf("mark partition running: %w", err) }
		start := time.Now()

		// 跨 shard 并发执行，但状态汇总主线程串行写
		type result struct{ host string; err error }
		ch := make(chan result, len(e.conns))
		for _, c := range e.conns {
			c := c
			go func() {
				key := fmt.Sprintf("%s/%s.%s/%s", p.Partition, run.Database, run.Table, c.host)
				sql := fmt.Sprintf("BACKUP TABLE `%s`.`%s`%s",
					run.Database, run.Table, e.storage.BackupSQL(run.Database, run.Table, p.Partition, key))
				ch <- result{c.host, e.execSQL(c.host, sql)}
			}()
		}
		var partErrs []string
		for range e.conns {
			r := <-ch
			if r.err != nil {
				partErrs = append(partErrs, fmt.Sprintf("[%s] %v", r.host, r.err))
			}
		}
		p.Elapsed = int(time.Since(start).Seconds())
		if len(partErrs) > 0 {
			p.Status = model.BACKUP_PARTITION_STATUS_FAILED
			p.Msg = strings.Join(partErrs, "; ")
			anyFail = true
		} else {
			p.Status = model.BACKUP_PARTITION_STATUS_SUCCESS
		}
		if err := e.repo.UpdateRun(run); err != nil { return fmt.Errorf("update partition: %w", err) }
	}
	if anyFail {
		return errors.New("one or more partitions failed")
	}
	return nil
}
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/executor.go service/backup/executor_test.go
git commit -m "$(cat <<'EOF'
feat(backup): Executor.Backup partition 隔离 + 状态机串行写

partition 级失败不再传染：跨 shard 并发执行，但 partition 状态汇总在主线程
串行写入，规避 b.b.Partitions[i].Status 的 race。
任一分区失败 → run 整体 failed，但其余分区仍尝试，便于用户事后挑成功子集恢复。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 18：RestoreData 阶段

**Files:**
- Modify: `service/backup/executor.go`、`service/backup/executor_test.go`

与 Task 17 对称：BACKUP TABLE → RESTORE TABLE，partition 子集来自源 run 的成功分区（已由 Plan 2 的 SubmitRestore 在创建本 run 时写入）。

- [ ] **Step 1：测试**

```go
func TestRealStages_Restore_PartitionIsolation(t *testing.T) {
	conns := []*shardConn{newFakeShardConn("h1")}
	calls := []string{}
	execFn := func(_ string, sql string) error {
		calls = append(calls, sql)
		if strings.Contains(sql, "20250508") { return errors.New("restore failed") }
		return nil
	}
	run := model.BackupRun{
		RunID: "r1", Operation: model.OP_RESTORE,
		Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_WAITING},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_WAITING},
		},
	}
	e := &Executor{repo: newMemRepoWithRun(run), conns: conns, execSQL: execFn, storage: &fakeStorage{}}
	err := realStages{}.Restore(context.Background(), e, "r1")
	if err == nil { t.Fatal("expected fail") }
	for _, sql := range calls {
		if !strings.Contains(sql, "RESTORE TABLE") {
			t.Fatalf("expected RESTORE TABLE: %s", sql)
		}
	}
}
```

- [ ] **Step 2：fail**

- [ ] **Step 3：实现**（与 Task 17 几乎一致，仅 SQL 前缀与 storage 调用换为 `RestoreSQL`）

```go
func (realStages) Restore(ctx context.Context, e *Executor, runID string) error {
	run, _ := e.repo.GetRun(runID)
	var anyFail bool
	for i := range run.Partitions {
		p := &run.Partitions[i]
		if p.Status != model.BACKUP_PARTITION_STATUS_WAITING { continue }
		p.Status = model.BACKUP_PARTITION_STATUS_RUNNING
		if err := e.repo.UpdateRun(run); err != nil { return err }
		start := time.Now()
		type result struct{ host string; err error }
		ch := make(chan result, len(e.conns))
		for _, c := range e.conns {
			c := c
			go func() {
				key := fmt.Sprintf("%s/%s.%s/%s", p.Partition, run.Database, run.Table, c.host)
				sql := fmt.Sprintf("RESTORE TABLE `%s`.`%s`%s SETTINGS allow_non_empty_tables=true",
					run.Database, run.Table, e.storage.RestoreSQL(run.Database, run.Table, p.Partition, key))
				ch <- result{c.host, e.execSQL(c.host, sql)}
			}()
		}
		var partErrs []string
		for range e.conns {
			r := <-ch
			if r.err != nil { partErrs = append(partErrs, fmt.Sprintf("[%s] %v", r.host, r.err)) }
		}
		p.Elapsed = int(time.Since(start).Seconds())
		if len(partErrs) > 0 {
			p.Status = model.BACKUP_PARTITION_STATUS_FAILED
			p.Msg = strings.Join(partErrs, "; ")
			anyFail = true
		} else {
			p.Status = model.BACKUP_PARTITION_STATUS_SUCCESS
		}
		if err := e.repo.UpdateRun(run); err != nil { return err }
	}
	if anyFail { return errors.New("one or more partitions failed") }
	return nil
}
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/executor.go service/backup/executor_test.go
git commit -m "feat(backup): Executor.Restore partition 隔离（与 Backup 对称）"
```

---

### Task 19：Check 阶段（修 #3 早 return）

**Files:**
- Modify: `service/backup/executor.go`、`service/backup/executor_test.go`

旧版 `Check` 的 `return lastErr` 写在 for 循环里，多分区时只校验第一个就退出。本 task 钉住"全部分区都校验"。

- [ ] **Step 1：测试**

```go
func TestRealStages_Check_AllPartitionsValidated(t *testing.T) {
	// 3 个 success partition，全都要被校验；旧版只校验第一个就 return
	checkedHosts := map[string]int{}
	storage := &checksumStorage{
		check: func(host, db, table, p string, _ map[string]model.PathInfo) error {
			checkedHosts[p]++
			return nil
		},
	}
	run := model.BackupRun{
		RunID: "r1", Database: "dba", Table: "t1",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	e := &Executor{
		repo: newMemRepoWithRun(run),
		conns: []*shardConn{newFakeShardConn("h1")},
		storage: storage,
	}
	if err := (realStages{}).Check(context.Background(), e, "r1"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(checkedHosts) != 3 {
		t.Fatalf("expected 3 partitions checked, got %d (regression of #3)", len(checkedHosts))
	}
}

func TestRealStages_Check_FirstFailureRetainedButContinues(t *testing.T) {
	// p2 失败，p1 / p3 仍校验
	count := 0
	storage := &checksumStorage{check: func(_, _, _, p string, _ map[string]model.PathInfo) error {
		count++
		if p == "20250508" { return errors.New("md5 mismatch") }
		return nil
	}}
	run := model.BackupRun{
		RunID: "r1", Database: "d", Table: "t",
		Partitions: []model.BackupRunPartition{
			{Partition: "20250507", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
			{Partition: "20250509", Status: model.BACKUP_PARTITION_STATUS_SUCCESS},
		},
	}
	e := &Executor{repo: newMemRepoWithRun(run), conns: []*shardConn{newFakeShardConn("h1")}, storage: storage}
	err := (realStages{}).Check(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "md5 mismatch") {
		t.Fatalf("expected md5 mismatch, got %v", err)
	}
	if count != 3 {
		t.Fatalf("expected all 3 checked despite failure, got %d", count)
	}
}
```

`checksumStorage` 是测试假体：

```go
type checksumStorage struct {
	check func(host, db, table, partition string, pi map[string]model.PathInfo) error
}
func (s *checksumStorage) Init() error                                     { return nil }
func (s *checksumStorage) BackupSQL(_, _, _, _ string) string              { return "" }
func (s *checksumStorage) RestoreSQL(_, _, _, _ string) string             { return "" }
func (s *checksumStorage) CleanPartition(_, _, _, _ string) error          { return nil }
func (s *checksumStorage) CheckPartition(host, db, table, partition string, pi map[string]model.PathInfo) error {
	return s.check(host, db, table, partition, pi)
}
func (s *checksumStorage) Type() string { return "fake" }
```

- [ ] **Step 2：fail**

- [ ] **Step 3：实现**

```go
func (realStages) Check(ctx context.Context, e *Executor, runID string) error {
	run, _ := e.repo.GetRun(runID)
	var firstErr error
	for _, p := range run.Partitions {
		if p.Status != model.BACKUP_PARTITION_STATUS_SUCCESS { continue }
		g, _ := errgroup.WithContext(ctx)
		for _, c := range e.conns {
			c := c
			pp := p
			g.Go(func() error {
				return e.storage.CheckPartition(c.host, run.Database, run.Table, pp.Partition, pp.PathInfo)
			})
		}
		if err := g.Wait(); err != nil && firstErr == nil {
			firstErr = err
			// 不 return；继续校验后续 partition（修 #3）
		}
	}
	return firstErr
}
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/executor.go service/backup/executor_test.go
git commit -m "$(cat <<'EOF'
feat(backup): Executor.Check 校验所有分区不再早 return（修 #3）

旧版 return lastErr 写在 for 循环里，多分区只校验第一个就退出。
新版：firstErr 累计但循环继续，确保全部 success 分区都被校验；返回
首个不匹配以便用户排查。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 20：Close 阶段（修 #7 静默吞错 + #8 SetStatus 错误）

**Files:**
- Modify: `service/backup/executor.go`、`service/backup/executor_test.go`

- [ ] **Step 1：测试**

```go
func TestRealStages_Close_DropPartitionFailureFailsRun(t *testing.T) {
	// Clean=true 时 DROP PARTITION 失败 → 整体 run failed（修 #7）
	repo := &fakeExecRepo{memRepo: newMemRepo(),
		policy: model.BackupPolicy{PolicyID: "p1", Clean: true}}
	repo.runs["r1"] = model.BackupRun{
		RunID: "r1", PolicyID: "p1", Database: "d", Table: "t",
		Partitions: []model.BackupRunPartition{{Partition: "20250508", Status: model.BACKUP_PARTITION_STATUS_SUCCESS}},
	}
	e := &Executor{
		repo: repo, conns: []*shardConn{newFakeShardConn("h1")},
		execSQL: func(_, sql string) error {
			if strings.HasPrefix(sql, "ALTER TABLE") { return errors.New("permission denied") }
			return nil
		},
	}
	err := realStages{}.Close(context.Background(), e, "r1")
	if err == nil || !strings.Contains(err.Error(), "cleanup_failed") {
		t.Fatalf("DROP PARTITION failure must surface, got %v", err)
	}
}

func TestRealStages_Close_NoCleanReturnsNil(t *testing.T) {
	repo := &fakeExecRepo{memRepo: newMemRepo(), policy: model.BackupPolicy{PolicyID: "p1", Clean: false}}
	repo.runs["r1"] = model.BackupRun{RunID: "r1", PolicyID: "p1"}
	e := &Executor{repo: repo, conns: []*shardConn{newFakeShardConn("h1")}, execSQL: func(_, _ string) error { return nil }}
	if err := (realStages{}).Close(context.Background(), e, "r1"); err != nil {
		t.Fatalf("Clean=false should be no-op: %v", err)
	}
}
```

- [ ] **Step 2：fail**

- [ ] **Step 3：实现**

```go
func (realStages) Close(ctx context.Context, e *Executor, runID string) error {
	run, err := e.repo.GetRun(runID)
	if err != nil { return err }
	policy, err := e.repo.GetPolicyForRun(run.PolicyID)
	if err != nil { return err }
	if !policy.Clean { return nil }

	var dropErrs []error
	for _, p := range run.Partitions {
		if p.Status != model.BACKUP_PARTITION_STATUS_SUCCESS { continue }
		// identifier / partition 值校验
		if err := ValidateIdentifier(run.Database); err != nil { return err }
		if err := ValidateIdentifier(run.Table); err != nil { return err }
		for _, c := range e.conns {
			sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PARTITION '%s'",
				run.Database, run.Table, strings.ReplaceAll(p.Partition, "'", "''"))
			if err := e.execSQL(c.host, sql); err != nil {
				dropErrs = append(dropErrs, fmt.Errorf("[%s][%s] %w", c.host, p.Partition, err))
			}
		}
	}
	if len(dropErrs) > 0 {
		return fmt.Errorf("cleanup_failed: %v", errors.Join(dropErrs...)) // 修 #7
	}
	return nil
}
```

- [ ] **Step 4：测试 PASS**

- [ ] **Step 5：commit**

```bash
git add service/backup/executor.go service/backup/executor_test.go
git commit -m "$(cat <<'EOF'
feat(backup): Executor.Close DROP PARTITION 失败不再静默（修 #7 + #8）

Clean=true 时 DROP PARTITION 错误用 errors.Join 聚合后冒泡使 run 标 failed，
status_reason 写明 cleanup_failed；旧版 _ = conn.Exec(query) 静默吞错使
"备份成功 + 数据未清理"看起来无异常，本次修闭。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## 收尾：Task 21

### Task 21：在 main 里启动 service.backup（不接 HTTP）

**Files:**
- Modify: `cmd/ckman/main.go`（或现有启动入口）

让新代码在进程里跑起来，但不动 HTTP；新 scheduler 与老 cron 共存（spec §3.3 已允许过渡期）。

- [ ] **Step 1：找到 ckman main 启动顺序**

```bash
grep -rn "repository.Ps\\.Init\\|router.Init\\|cron.NewCronService" cmd/ /data/root/go/src/github.com/housepower/ckman/main.go 2>/dev/null
```

- [ ] **Step 2：在 repository.Ps.Init() 之后、router.Init() 之前插入：**

```go
// main.go 启动逻辑中
import bk "github.com/housepower/ckman/service/backup"

self := net.JoinHostPort(cfg.Server.Ip, fmt.Sprint(cfg.Server.Port))
repo := bk.NewPersistentRepoAdapter(repository.Ps) // 适配现有 Ps 接口为 ServiceRepo
maxConcurrent := 8
if cfg.Backup.MaxConcurrent > 0 { maxConcurrent = cfg.Backup.MaxConcurrent }

bkSvc := bk.NewService(self, repo, nil) // pool 后注入

executor := bk.NewExecutor(repo, /* ckhouse conn factory */, /* storage 工厂 */)
pool := bk.NewPool(maxConcurrent, executor.Run)
bkSvc.SetPool(pool)

// 启动顺序：interrupted 标记 → pool → scheduler
ctx := context.Background()
if err := bkSvc.Boot(); err != nil { log.Logger.Fatal("backup boot: ", err) }
pool.Start(ctx)

cronAdapter := bk.NewRobfigCronAdapter(cronService)
sched := bk.NewScheduler(self, cronAdapter, bk.NewPolicyRepoAdapter(repository.Ps),
    func(p model.BackupPolicy) { _, _ = bkSvc.SubmitForPolicy(p, model.TRIGGER_CRON) })
sched.Start(ctx)

// router.Init 仍在后面，HTTP 路由本期不动
```

`NewPersistentRepoAdapter` / `NewPolicyRepoAdapter` / `NewRobfigCronAdapter` 是把 `repository.Ps` 与 `cron.CronService` 适配到 Plan 1 中定义的接口，写在 `service/backup/adapter.go` 内（短薄壁，每个方法 1-2 行委托）。

- [ ] **Step 3：编译跑起来**

```bash
go build -o ckman .
./ckman --conf conf/ckman.hjson &  # 旧老 cron + 新 scheduler 并存，不冲突
```

- [ ] **Step 4：手测一下**

```bash
# 看新 scheduler 在 60 秒内 reconcile log
tail -f log/ckman.log | grep "\\[scheduler\\]"
```

启动应能看到：
```
[scheduler] reconcile add p-... 0 3 * * *
```

- [ ] **Step 5：commit**

```bash
git add cmd/ckman/main.go service/backup/adapter.go
git commit -m "$(cat <<'EOF'
feat(backup): 在 main 启动 service.backup（与老 cron 并存过渡）

启动顺序：repository → backup.Boot（interrupted 标记） → pool.Start → scheduler.Start
→ router。HTTP 入口本期不动，老 BackupManage 路径继续承担线上流量。

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## 完成定义

Plan 1 完成时：

- ✅ `go build ./...` 整体编译通过
- ✅ `go test ./...` 整体通过（不依赖真实 ClickHouse / S3 的部分）
- ✅ ckman 能正常启动，老备份功能未受影响
- ✅ 新 scheduler 在日志中能看到周期 reconcile
- ✅ 新数据表已建（MySQL/PG 自动建；local 文件已 dump）
- ✅ Plan 1 涉及的所有 Bug（#3 #4 #5 #7 #8 #9 + Local checksum）有对应的失败测试 + 修复后通过

后续 Plan 2 接管 HTTP cutover + Frontend 重构；Plan 3 接管数据迁移 + 老代码删除 + 集成测试 + 观测指标。
