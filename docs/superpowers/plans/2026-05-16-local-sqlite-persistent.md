# Local 持久化后端 SQLite 化 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `repository/local` 的 JSON 文件持久化替换为 SQLite（纯 Go 驱动 `glebarez/sqlite`），用户配置 `persistent_policy: local` 不变，旧 JSON 数据启动时自动迁移，并提供 `ckmanctl dump-to-json` 离线回写工具与优雅关闭快照支持降级。

**Architecture:** 新增 `repository/sqlite` 包以 `"local"` 名字注册到 `PersistentRegistry`；原 `repository/local` 改名为 `repository/legacyjson` 并去除注册，仅作为一次性迁移读源 + dump 写入目标；启动时 `Init()` 通过共用的 `cmd/migrate.MigrateBetween()` 把旧 JSON 内容搬到 SQLite。

**Tech Stack:** Go 1.24, GORM `v1.23.5`, `github.com/glebarez/sqlite`（纯 Go，底层 `modernc.org/sqlite`，无 CGo），SQLite WAL 模式。

**Spec:** `docs/superpowers/specs/2026-05-16-local-sqlite-persistent-design.md`

---

## 文件清单

**新增：**
- `repository/sqlite/factory.go`、`constant.go`、`config.go`、`model.go`、`sqlite.go`、`migrate.go`、`sqlite_test.go`
- `repository/sqlite/testdata/legacy_clusters.json`、`legacy_clusters.yaml`
- `repository/legacyjson/writer.go`（新写入工具）
- `cmd/dumpjson/dumpjson.go`（ckmanctl 子命令实现）

**重命名/移动：**
- `repository/local/` → `repository/legacyjson/`（文件原样移动，仅改 package 声明 + 移除 init 注册）

**修改：**
- `repository/persistent.go`（接口加两个 `GetAll*`）
- `repository/mysql/mysql.go`、`repository/postgres/postgres.go`、`repository/dm8/dm8.go`（实现新接口方法）
- `cmd/migrate/migrate.go`（拆出 `MigrateBetween(src, dst)`，补 BackupPolicy/Run）
- `cmd/migrate/migrate_test.go`（新增）
- `cmd/ckmanctl/ckmanctl.go`（加 `dump-to-json` 子命令）
- `main.go`（替换 `_ "repository/local"` 为 `_ "repository/sqlite"`、`termHandler` 加快照）
- `go.mod` / `go.sum`（加 `glebarez/sqlite` 依赖）

---

## 任务依赖图

```
Task 1 (interface) ─┬─> Task 2 (local 实现 GetAll*)
                    ├─> Task 3 (mysql/pg/dm8 实现 GetAll*)
                    └─> Task 4 (MigrateBetween 重构 + 含 GetAll*)
                              │
Task 5 (rename local→legacyjson) ──> Task 6 (legacyjson 加 Writer) ──> Task 7 (cmd/migrate 改 import)
                              │
Task 8 (go.mod 加依赖) ──> Task 9 (sqlite skeleton) ──> Task 10 (model.go)
                                          ├─> Task 11 (Init + 事务)
                                          ├─> Task 12 Cluster+Logic
                                          ├─> Task 13 QueryHistory
                                          ├─> Task 14 Task
                                          ├─> Task 15 Backup
                                          ├─> Task 16 BackupPolicy
                                          └─> Task 17 BackupRun
                                                  │
                                          Task 18 (migrate.go 启动迁移)
                                                  │
                                          Task 19 (main.go 换注册 + shutdown snapshot)
                                                  │
                                          Task 20 (ckmanctl dump-to-json)
                                                  │
                                          Task 21 (手工验收)
```

每个 Task 结束都要 `make build && go test ./repository/... ./cmd/...` 全绿，然后提交。

---

## Task 1: 在接口加 `GetAllBackupPolicies` / `GetAllBackupRuns`

**Files:**
- Modify: `repository/persistent.go`

**Rationale:** 现有 `PersistentBackupPolicyService` 只能按 cluster / instance 过滤；迁移和 dump-to-json 需要"全部"读出。这是新增接口方法，对实现侧是加法。

- [ ] **Step 1: 修改 `repository/persistent.go` 接口定义**

在 `PersistentBackupPolicyService` 接口最后一行加：

```go
type PersistentBackupPolicyService interface {
    CreateBackupPolicy(p model.BackupPolicy) error
    UpdateBackupPolicy(p model.BackupPolicy) error
    DeleteBackupPolicy(policyID string) error
    GetBackupPolicy(policyID string) (model.BackupPolicy, error)
    GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error)
    GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error)
    GetAllBackupPolicies() ([]model.BackupPolicy, error) // 新增：用于迁移和 dump
}
```

在 `PersistentBackupRunService` 接口最后一行加：

```go
type PersistentBackupRunService interface {
    CreateBackupRun(r model.BackupRun) error
    UpdateBackupRun(r model.BackupRun) error
    DeleteBackupRun(runID string) error
    GetBackupRun(runID string) (model.BackupRun, error)
    GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error)
    GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error)
    GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error)
    GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error)
    MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error)
    GetAllBackupRuns() ([]model.BackupRun, error) // 新增：用于迁移和 dump
}
```

- [ ] **Step 2: 编译验证报错（红 → 红，预期所有 backend 都没实现）**

```bash
go build ./...
```

Expected: 报错 `*LocalPersistent does not implement repository.PersistentMgr (missing method GetAllBackupPolicies)` 等。**这步是预期失败**，确保下个 Task 真要补实现。

- [ ] **Step 3: 不提交，直接进入 Task 2/3**（保持编译失败状态以保证 Task 2/3 收敛）

---

## Task 2: 在 `repository/local` 实现 `GetAllBackupPolicies` / `GetAllBackupRuns`

> 注意：此时 `repository/local` 还没改名，先在原位补实现。

**Files:**
- Modify: `repository/local/local.go`
- Test: `repository/local/local_v2_test.go`

- [ ] **Step 1: 在 `local_v2_test.go` 末尾加测试**

```go
func TestLocal_GetAllBackupPolicies(t *testing.T) {
    lp := newTestLP(t)
    _ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p1", ClusterName: "ckA"})
    _ = lp.CreateBackupPolicy(model.BackupPolicy{PolicyID: "p2", ClusterName: "ckB"})
    all, err := lp.GetAllBackupPolicies()
    if err != nil {
        t.Fatalf("get all: %v", err)
    }
    if len(all) != 2 {
        t.Fatalf("expected 2 policies, got %d", len(all))
    }
}

func TestLocal_GetAllBackupRuns(t *testing.T) {
    lp := newTestLP(t)
    _ = lp.CreateBackupRun(model.BackupRun{RunID: "r1", PolicyID: "p1"})
    _ = lp.CreateBackupRun(model.BackupRun{RunID: "r2", PolicyID: "p2"})
    all, err := lp.GetAllBackupRuns()
    if err != nil {
        t.Fatalf("get all: %v", err)
    }
    if len(all) != 2 {
        t.Fatalf("expected 2 runs, got %d", len(all))
    }
}
```

- [ ] **Step 2: 验证测试失败**

```bash
go test ./repository/local/ -run "TestLocal_GetAll" -v
```

Expected: FAIL（编译错或 method 不存在）

- [ ] **Step 3: 在 `local.go` `MarkRunRunningIfQueued` 之后加实现**

```go
func (lp *LocalPersistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
    lp.lock.RLock()
    defer lp.lock.RUnlock()
    out := make([]model.BackupPolicy, 0, len(lp.Data.BackupPolicy))
    for _, p := range lp.Data.BackupPolicy {
        out = append(out, p)
    }
    return out, nil
}

func (lp *LocalPersistent) GetAllBackupRuns() ([]model.BackupRun, error) {
    lp.lock.RLock()
    defer lp.lock.RUnlock()
    out := make([]model.BackupRun, 0, len(lp.Data.BackupRun))
    for _, r := range lp.Data.BackupRun {
        out = append(out, r)
    }
    return out, nil
}
```

- [ ] **Step 4: 验证测试通过**

```bash
go test ./repository/local/ -run "TestLocal_GetAll" -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add repository/persistent.go repository/local/local.go repository/local/local_v2_test.go
git commit -m "feat(repository): add GetAllBackupPolicies/Runs to interface (local impl)

For upcoming MigrateBetween refactor and dump-to-json tool. mysql/postgres/dm8
impls follow in next commit (build is intentionally broken between these two)."
```

---

## Task 3: 在 mysql / postgres / dm8 实现 `GetAllBackupPolicies` / `GetAllBackupRuns`

**Files:**
- Modify: `repository/mysql/mysql.go`、`repository/postgres/postgres.go`、`repository/dm8/dm8.go`

实现模式三份后端完全一致，只是模型类型名不同（`TblBackupPolicy` / `TblBackupRun`）。

- [ ] **Step 1: 在 `repository/mysql/mysql.go` 末尾加（紧跟 `MarkRunRunningIfQueued`）**

```go
func (mp *MysqlPersistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
    var tbls []TblBackupPolicy
    tx := mp.Client.Find(&tbls)
    if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
        return nil, errors.Wrap(tx.Error, "")
    }
    out := make([]model.BackupPolicy, 0, len(tbls))
    for _, tbl := range tbls {
        var p model.BackupPolicy
        if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, p)
    }
    return out, nil
}

func (mp *MysqlPersistent) GetAllBackupRuns() ([]model.BackupRun, error) {
    var tbls []TblBackupRun
    tx := mp.Client.Find(&tbls)
    if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
        return nil, errors.Wrap(tx.Error, "")
    }
    out := make([]model.BackupRun, 0, len(tbls))
    for _, tbl := range tbls {
        var r model.BackupRun
        if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, r)
    }
    return out, nil
}
```

- [ ] **Step 2: 在 `repository/postgres/postgres.go` 末尾加同样的两个函数**

把上面代码中的 `MysqlPersistent` 改成 `PostgresPersistent`（参考文件首部已有结构体名）。其余完全一致。

- [ ] **Step 3: 在 `repository/dm8/dm8.go` 末尾加同样的两个函数**

把 `MysqlPersistent` 改成 `Dm8Persistent`。`tbl.Policy` / `tbl.Run` 字段类型在 dm8 是 `dmSchema.Clob`，需要先 `string(tbl.Policy)` 转换 —— 仿照 `dm8.go` 内已有的 `CreateBackupPolicy` 反向用法（`tbl.Policy` 已经是 `dmSchema.Clob` 类型且可以 string 强转）。

具体写法：

```go
func (dp *Dm8Persistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
    var tbls []TblBackupPolicy
    tx := dp.Client.Find(&tbls)
    if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
        return nil, errors.Wrap(tx.Error, "")
    }
    out := make([]model.BackupPolicy, 0, len(tbls))
    for _, tbl := range tbls {
        var p model.BackupPolicy
        if err := json.Unmarshal([]byte(string(tbl.Policy)), &p); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, p)
    }
    return out, nil
}

func (dp *Dm8Persistent) GetAllBackupRuns() ([]model.BackupRun, error) {
    var tbls []TblBackupRun
    tx := dp.Client.Find(&tbls)
    if tx.Error != nil && tx.Error != gorm.ErrRecordNotFound {
        return nil, errors.Wrap(tx.Error, "")
    }
    out := make([]model.BackupRun, 0, len(tbls))
    for _, tbl := range tbls {
        var r model.BackupRun
        if err := json.Unmarshal([]byte(string(tbl.Run)), &r); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, r)
    }
    return out, nil
}
```

- [ ] **Step 4: 编译验证**

```bash
go build ./...
```

Expected: 成功，无 `does not implement` 错误。

- [ ] **Step 5: Commit**

```bash
git add repository/mysql/mysql.go repository/postgres/postgres.go repository/dm8/dm8.go
git commit -m "feat(repository): implement GetAllBackupPolicies/Runs for sql backends"
```

---

## Task 4: 重构 `cmd/migrate.Migrate()` 为 `MigrateBetween(src, dst)`

**Files:**
- Modify: `cmd/migrate/migrate.go`
- Test: `cmd/migrate/migrate_test.go`（新增）

**Rationale:** 现有 `Migrate()` 用包级全局 `psrc/pdst`，且没迁移 `BackupPolicy/BackupRun`。重构为可复用纯函数 + 补全。

- [ ] **Step 1: 写测试 `cmd/migrate/migrate_test.go`**

```go
package migrate

import (
    "os"
    "testing"

    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/model"
    "github.com/housepower/ckman/repository"
    _ "github.com/housepower/ckman/repository/local"
)

func TestMain(m *testing.M) {
    log.InitLoggerConsole()
    os.Exit(m.Run())
}

func newLocalPM(t *testing.T) repository.PersistentMgr {
    t.Helper()
    dir := t.TempDir()
    ps := repository.GetPersistentByName("local")
    if ps == nil {
        t.Fatalf("local backend not registered")
    }
    cfg := ps.UnmarshalConfig(map[string]interface{}{
        "format":      "json",
        "config_dir":  dir,
        "config_file": "test_clusters",
    })
    if err := ps.Init(cfg); err != nil {
        t.Fatalf("init: %v", err)
    }
    return ps
}

func TestMigrateBetween_CoversAllEntities(t *testing.T) {
    src := newLocalPM(t)
    dst := newLocalPM(t)

    if err := src.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1"}); err != nil {
        t.Fatalf("src create cluster: %v", err)
    }
    if err := src.CreateLogicCluster("L1", []string{"ck1"}); err != nil {
        t.Fatalf("src create logic: %v", err)
    }
    if err := src.CreateQueryHistory(model.QueryHistory{CheckSum: "qh1", Cluster: "ck1", QuerySql: "SELECT 1"}); err != nil {
        t.Fatalf("src create qh: %v", err)
    }
    if err := src.CreateTask(model.Task{TaskId: "t1"}); err != nil {
        t.Fatalf("src create task: %v", err)
    }
    if err := src.CreateBackup(model.Backup{BackupId: "b1", ClusterName: "ck1"}); err != nil {
        t.Fatalf("src create backup: %v", err)
    }
    if err := src.CreateBackupPolicy(model.BackupPolicy{PolicyID: "bp1", ClusterName: "ck1"}); err != nil {
        t.Fatalf("src create policy: %v", err)
    }
    if err := src.CreateBackupRun(model.BackupRun{RunID: "br1", PolicyID: "bp1"}); err != nil {
        t.Fatalf("src create run: %v", err)
    }

    if err := MigrateBetween(src, dst); err != nil {
        t.Fatalf("migrate: %v", err)
    }

    if _, err := dst.GetClusterbyName("ck1"); err != nil {
        t.Fatalf("dst missing cluster: %v", err)
    }
    if _, err := dst.GetLogicClusterbyName("L1"); err != nil {
        t.Fatalf("dst missing logic: %v", err)
    }
    if _, err := dst.GetQueryHistoryByCheckSum("qh1"); err != nil {
        t.Fatalf("dst missing query history: %v", err)
    }
    if _, err := dst.GetTaskbyTaskId("t1"); err != nil {
        t.Fatalf("dst missing task: %v", err)
    }
    if _, err := dst.GetBackupById("b1"); err != nil {
        t.Fatalf("dst missing backup: %v", err)
    }
    if _, err := dst.GetBackupPolicy("bp1"); err != nil {
        t.Fatalf("dst missing policy: %v", err)
    }
    if _, err := dst.GetBackupRun("br1"); err != nil {
        t.Fatalf("dst missing run: %v", err)
    }
}
```

- [ ] **Step 2: 验证测试失败（`MigrateBetween` 不存在）**

```bash
go test ./cmd/migrate/ -run TestMigrateBetween -v
```

Expected: FAIL with "undefined: MigrateBetween"

- [ ] **Step 3: 重构 `cmd/migrate/migrate.go`**

完全替换 `Migrate` 函数为 `MigrateBetween`，并补 BackupPolicy/BackupRun：

```go
func MigrateBetween(src, dst repository.PersistentMgr) error {
    clusters, err := src.GetAllClusters()
    if err != nil {
        return errors.Wrap(err, "get clusters")
    }
    if len(clusters) == 0 {
        log.Logger.Warnf("clusters have 0 records, will migrate nothing")
    }

    logics, err := src.GetAllLogicClusters()
    if err != nil {
        return errors.Wrap(err, "get logics")
    }
    historys, err := src.GetAllQueryHistory()
    if err != nil {
        return errors.Wrap(err, "get query history")
    }
    tasks, err := src.GetAllTasks()
    if err != nil {
        return errors.Wrap(err, "get tasks")
    }
    var backups []model.Backup
    for _, conf := range clusters {
        b, err := src.GetAllBackups(conf.Cluster)
        if err != nil {
            return errors.Wrap(err, "get backups")
        }
        backups = append(backups, b...)
    }
    policies, err := src.GetAllBackupPolicies()
    if err != nil {
        return errors.Wrap(err, "get backup policies")
    }
    runs, err := src.GetAllBackupRuns()
    if err != nil {
        return errors.Wrap(err, "get backup runs")
    }

    if err = dst.Begin(); err != nil {
        return errors.Wrap(err, "begin")
    }
    rollback := func(e error) error {
        _ = dst.Rollback()
        return errors.Wrap(e, "")
    }

    for _, cluster := range clusters {
        if err = dst.CreateCluster(cluster); err != nil {
            return rollback(err)
        }
    }
    for logic, physics := range logics {
        if err = dst.CreateLogicCluster(logic, physics); err != nil {
            return rollback(err)
        }
    }
    for _, v := range historys {
        if err = dst.CreateQueryHistory(v); err != nil {
            return rollback(err)
        }
    }
    for _, v := range tasks {
        if err = dst.CreateTask(v); err != nil {
            return rollback(err)
        }
    }
    for _, v := range backups {
        if err = dst.CreateBackup(v); err != nil {
            return rollback(err)
        }
    }
    for _, p := range policies {
        if err = dst.CreateBackupPolicy(p); err != nil {
            return rollback(err)
        }
    }
    for _, r := range runs {
        if err = dst.CreateBackupRun(r); err != nil {
            return rollback(err)
        }
    }

    if err = dst.Commit(); err != nil {
        return errors.Wrap(err, "commit")
    }
    return nil
}
```

把原 `Migrate()` 删除，原 `MigrateHandle` 修改为调用新函数：

```go
func MigrateHandle(conf string) {
    config, err := ParseConfig(conf)
    if err != nil {
        fmt.Printf("parse config file %s failed: %v\n", conf, err)
        return
    }
    src, err := PersistentCheck(config, config.Source)
    if err != nil {
        fmt.Printf("source [%s] err: %v\n", config.Source, err)
        return
    }
    dst, err := PersistentCheck(config, config.Target)
    if err != nil {
        fmt.Printf("target [%s] err: %v\n", config.Target, err)
        return
    }
    if err := MigrateBetween(src, dst); err != nil {
        fmt.Printf("migrate failed: %v\n", err)
        return
    }
    fmt.Printf("From [%s] migrate to [%s] success!\n", config.Source, config.Target)
}
```

包级变量 `psrc / pdst` 可以彻底删除。

- [ ] **Step 4: 验证测试通过**

```bash
go test ./cmd/migrate/ -run TestMigrateBetween -v
```

Expected: PASS

- [ ] **Step 5: 整包编译**

```bash
go build ./...
```

Expected: success

- [ ] **Step 6: Commit**

```bash
git add cmd/migrate/migrate.go cmd/migrate/migrate_test.go
git commit -m "refactor(migrate): extract MigrateBetween(src,dst) and cover policy/run

Previously Migrate() relied on package-level psrc/pdst and silently
skipped BackupPolicy/BackupRun tables. The refactor makes it a pure
function and covers all 7 entities."
```

---

## Task 5: 重命名 `repository/local/` → `repository/legacyjson/`

**Files:**
- Move: `repository/local/*` → `repository/legacyjson/*`（5 个文件）
- Modify: 移动后所有 `.go` 文件的 `package local` → `package legacyjson`
- Modify: `repository/legacyjson/factory.go`（改注册名 + 后续 Task 6 整体重写）

- [ ] **Step 1: 移动目录**

```bash
mkdir -p repository/legacyjson
git mv repository/local/config.go repository/legacyjson/
git mv repository/local/constant.go repository/legacyjson/
git mv repository/local/factory.go repository/legacyjson/
git mv repository/local/local.go repository/legacyjson/
git mv repository/local/local_v2_test.go repository/legacyjson/
git mv repository/local/model.go repository/legacyjson/
rmdir repository/local
```

- [ ] **Step 2: 批量替换 package 声明**

```bash
sed -i 's/^package local$/package legacyjson/' repository/legacyjson/*.go
```

校验：

```bash
grep -l "^package " repository/legacyjson/
head -1 repository/legacyjson/*.go
```

Expected: 全部 `package legacyjson`

- [ ] **Step 3: 不动 import path 引用（下一个 Task 单独修 cmd/migrate 的引用，main.go 留到 Task 19）**

构建会因 `cmd/migrate` 和 `main.go` 还 import `repository/local` 而失败，这是预期 —— Task 6 后接 Task 7 修。

- [ ] **Step 4: 暂不 commit**，与 Task 6 合并为一次提交。

---

## Task 6: 在 `legacyjson` 移除 init 注册、加 NewReader / NewWriter

**Files:**
- Modify: `repository/legacyjson/factory.go`（整体重写）
- Create: `repository/legacyjson/writer.go`

- [ ] **Step 1: 重写 `repository/legacyjson/factory.go`**

```go
package legacyjson

// 不再调用 RegistePersistent —— legacyjson 仅作为迁移读源 / dump 写目标，
// 不能被 persistent_policy 配置选中。
//
// 启动期迁移：repository/sqlite/migrate.go 直接调 NewReader(cfg) 构造实例。
// dump-to-json 工具：cmd/dumpjson 直接调 NewWriter(cfg) 构造实例。

// NewReader 仅打开（或创建空）legacy JSON / YAML 文件，提供完整的 PersistentMgr
// 读路径。写路径同样保留以兼容 cmd/migrate 反向场景（dump-to-json）。
func NewReader(cfg LocalConfig) (*LocalPersistent, error) {
    lp := NewLocalPersistent()
    if err := lp.Init(cfg); err != nil {
        return nil, err
    }
    return lp, nil
}
```

注意：原文件里 `init()` / `Factory{}` / `NewFactory()` 全删除。`LocalPersistent` 结构体和方法保留不动。

- [ ] **Step 2: 创建 `repository/legacyjson/writer.go`**

```go
package legacyjson

// NewWriter 创建一个空的 legacy 后端实例，等待被填入数据并 dump 到指定 JSON/YAML 文件。
// 与 NewReader 的差异只在语义；底层实现完全相同。
func NewWriter(cfg LocalConfig) (*LocalPersistent, error) {
    return NewReader(cfg)
}
```

- [ ] **Step 3: 验证 legacyjson 包能独立构建**

```bash
go build ./repository/legacyjson/
```

Expected: success

- [ ] **Step 4: 验证 legacyjson 的测试还能跑（之前从 local_v2_test.go 移过来的）**

```bash
go test ./repository/legacyjson/ -v
```

Expected: PASS（测试文件里的 `package local` 已经被 Step 5.2 改成了 `package legacyjson`，所以可以直接编译运行）

- [ ] **Step 5: 与 Task 5 一起 commit**

```bash
git add repository/legacyjson/
git commit -m "refactor(repository): rename local→legacyjson, drop registration

repository/local renamed to repository/legacyjson. The package no longer
registers itself to PersistentRegistry; instead it exposes NewReader/NewWriter
constructors for the new sqlite backend's migration and the dump-to-json tool.

Build is intentionally broken at HEAD because cmd/migrate and main.go still
import repository/local; Task 7 and Task 19 fix those."
```

---

## Task 7: 修复 `cmd/migrate` 对 legacyjson 的 import

**Files:**
- Modify: `cmd/migrate/migrate.go`
- Modify: `cmd/migrate/migrate_test.go`

- [ ] **Step 1: 替换 `cmd/migrate/migrate.go` 第 13 行附近**

```go
_ "github.com/housepower/ckman/repository/local"
```

改为：

```go
// legacyjson 不再注册到 PersistentRegistry，故不能 anonymous import 来获取 "local" policy。
// 真正的 "local" policy 将由 repository/sqlite 提供（见 Task 19 main.go）。
// cmd/migrate 在 ckmanctl 上下文里依赖 main 包注册，所以这里不需要 anonymous import。
```

实际上把这一行删掉即可。但是 `cmd/migrate` 还需要别的 backend（mysql/postgres/dm8）注册。继续保留：

```go
_ "github.com/housepower/ckman/repository/dm8"
_ "github.com/housepower/ckman/repository/mysql"
_ "github.com/housepower/ckman/repository/postgres"
```

只删除 `repository/local` 那一行。

- [ ] **Step 2: 修改 `cmd/migrate/migrate_test.go` 同样 import**

```go
_ "github.com/housepower/ckman/repository/local"
```

替换为（暂时跳过测试，等 sqlite 注册后再启用）：

```go
// 测试在 Task 19 之后启用 —— 那时 sqlite 注册为 "local"。
// 当前临时改用直接构造避免 PersistentRegistry 依赖：
```

实际改测试为直接构造 legacyjson（不走 registry）：

```go
package migrate

import (
    "os"
    "testing"

    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/model"
    "github.com/housepower/ckman/repository"
    "github.com/housepower/ckman/repository/legacyjson"
)

func TestMain(m *testing.M) {
    log.InitLoggerConsole()
    os.Exit(m.Run())
}

func newLegacyJSON(t *testing.T) repository.PersistentMgr {
    t.Helper()
    dir := t.TempDir()
    ps, err := legacyjson.NewReader(legacyjson.LocalConfig{
        Format:     "json",
        ConfigDir:  dir,
        ConfigFile: "test_clusters",
    })
    if err != nil {
        t.Fatalf("init legacyjson: %v", err)
    }
    return ps
}

func TestMigrateBetween_CoversAllEntities(t *testing.T) {
    src := newLegacyJSON(t)
    dst := newLegacyJSON(t)
    // 其余代码同 Task 4 Step 1
    // ...（保留之前写好的 CRUD 验证）
}
```

完整测试代码与 Task 4 Step 1 的测试体内容一致，仅替换 `newLocalPM` 为 `newLegacyJSON`。

- [ ] **Step 3: 验证编译 + 测试**

```bash
go build ./cmd/migrate/
go test ./cmd/migrate/ -v
```

Expected: PASS

- [ ] **Step 4: 整包编译（main.go 仍然 broken，预期）**

```bash
go build ./...
```

Expected: FAIL on `main.go` — `repository/local` not found. 预留给 Task 19 修。

- [ ] **Step 5: Commit**

```bash
git add cmd/migrate/migrate.go cmd/migrate/migrate_test.go
git commit -m "refactor(cmd/migrate): switch to legacyjson reader

cmd/migrate no longer anonymous-imports repository/local (which moved).
Use legacyjson.NewReader directly in tests; production migrate command
gets the new local→sqlite backend via main.go's anonymous import (Task 19)."
```

---

## Task 8: 加 `glebarez/sqlite` 依赖

**Files:**
- Modify: `go.mod`、`go.sum`

- [ ] **Step 1: go get**

```bash
go get github.com/glebarez/sqlite@latest
```

Expected: 写入 `go.mod` 一行 `github.com/glebarez/sqlite vX.Y.Z`

- [ ] **Step 2: 验证版本兼容 GORM v1.23.5**

```bash
go mod tidy
go build ./...
```

Expected: 编译报错（`main.go` 还有 broken import），但 glebarez 包不应报版本冲突。
如有 GORM 不兼容，回退用最后一个支持 GORM v1.23 的版本（pin `v1.5.0`）：

```bash
go get github.com/glebarez/sqlite@v1.5.0
go mod tidy
```

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "build: add github.com/glebarez/sqlite for pure-Go SQLite backend"
```

---

## Task 9: 创建 `repository/sqlite` 包骨架（config + constant + 空 factory）

**Files:**
- Create: `repository/sqlite/constant.go`
- Create: `repository/sqlite/config.go`
- Create: `repository/sqlite/factory.go`

- [ ] **Step 1: 创建 `repository/sqlite/constant.go`**

```go
package sqlite

// SQLitePersistentName 是 ckman.hjson 中 persistent_policy 的值。
// 保持 "local" 不变以兼容用户配置。
const SQLitePersistentName = "local"

const (
    SQLITE_TBL_CLUSTER        = "tbl_cluster"
    SQLITE_TBL_LOGIC          = "tbl_logic"
    SQLITE_TBL_QUERY_HISTORY  = "tbl_query_history"
    SQLITE_TBL_TASK           = "tbl_task"
    SQLITE_TBL_BACKUP         = "tbl_backup"
    SQLITE_TBL_BACKUP_POLICY  = "tbl_backup_policy"
    SQLITE_TBL_BACKUP_RUN     = "tbl_backup_run"
    SQLITE_TBL_META           = "tbl_meta"
)

const (
    SQLITE_DEFAULT_DB_FILE = "clusters.db"
    SQLITE_SCHEMA_VERSION  = "1"
)

const (
    METAKEY_MIGRATED_FROM  = "migrated_from"
    METAKEY_SCHEMA_VERSION = "schema_version"

    META_FRESH_INSTALL = "(fresh install)"
)
```

- [ ] **Step 2: 创建 `repository/sqlite/config.go`**

```go
package sqlite

import (
    "fmt"
    "path"

    "github.com/housepower/ckman/common"
    "github.com/housepower/ckman/config"
)

// LocalConfig 字段名与旧 repository/local 完全一致，
// 用户 ckman.hjson 中 persistent_config.local 节点反序列化无需修改。
//
// Format / ConfigDir / ConfigFile 在 SQLite 后端中仅用于：
//   - ConfigDir + ConfigFile.db: SQLite 文件路径
//   - Format + ConfigDir + ConfigFile.<ext>: 启动时定位需迁移的旧 JSON/YAML 文件
type LocalConfig struct {
    Format     string `yaml:"format" json:"format"`
    ConfigDir  string `yaml:"config_dir" json:"config_dir"`
    ConfigFile string `yaml:"config_file" json:"config_file"`
}

// Normalize 填充默认值。
//   - ConfigDir 默认 <work>/conf
//   - ConfigFile 默认 "clusters"（最终落到 clusters.db）
//   - Format 仅迁移时用，默认 "" 表示自动嗅探 .json / .yaml
func (loc *LocalConfig) Normalize() {
    loc.ConfigDir = common.GetStringwithDefault(loc.ConfigDir, path.Join(config.GetWorkDirectory(), "conf"))
    loc.ConfigFile = common.GetStringwithDefault(loc.ConfigFile, "clusters")
}

// DBPath 返回 SQLite 数据库文件绝对路径。
func (loc *LocalConfig) DBPath() string {
    return path.Join(loc.ConfigDir, fmt.Sprintf("%s.db", loc.ConfigFile))
}

// LegacyJSONPath / LegacyYAMLPath：返回旧文件的可能路径（迁移用）。
func (loc *LocalConfig) LegacyJSONPath() string {
    return path.Join(loc.ConfigDir, fmt.Sprintf("%s.json", loc.ConfigFile))
}

func (loc *LocalConfig) LegacyYAMLPath() string {
    return path.Join(loc.ConfigDir, fmt.Sprintf("%s.yaml", loc.ConfigFile))
}
```

- [ ] **Step 3: 创建占位 `repository/sqlite/factory.go`**

```go
package sqlite

import "github.com/housepower/ckman/repository"

func init() {
    repository.RegistePersistent(NewFactory)
}

type Factory struct{}

func (factory *Factory) CreatePersistent() repository.PersistentMgr {
    return NewSQLitePersistent()
}

func (factory *Factory) GetPersistentName() string {
    return SQLitePersistentName
}

func NewFactory() repository.PersistentFactory {
    return &Factory{}
}

// NewSQLitePersistent 在 Task 11 中给出具体定义。这里给一个引用占位
// 避免 factory 单独构建失败。
// Task 11 会创建 sqlite.go 文件，定义 SQLitePersistent 类型并实现 NewSQLitePersistent()。
```

- [ ] **Step 4: 编译验证（预期失败因为 `NewSQLitePersistent` 未定义）**

```bash
go build ./repository/sqlite/
```

Expected: FAIL with `undefined: NewSQLitePersistent`. 留给 Task 11 修。

- [ ] **Step 5: 不 commit**，与 Task 10/11 合并。

---

## Task 10: 创建 `repository/sqlite/model.go`

**Files:**
- Create: `repository/sqlite/model.go`

GORM tag 仿照 `repository/mysql/model.go`，去除 MySQL 专属语法。

- [ ] **Step 1: 创建 `repository/sqlite/model.go`**

```go
package sqlite

import (
    "time"

    "gorm.io/gorm"
)

// 字段定义与 repository/mysql/model.go 对齐；type:JSON 改为 type:TEXT。

type TblCluster struct {
    gorm.Model
    ClusterName string `gorm:"index:idx_name,unique; column:cluster_name"`
    Config      string `gorm:"column:config;type:TEXT"`
}

func (TblCluster) TableName() string { return SQLITE_TBL_CLUSTER }

type TblLogic struct {
    gorm.Model
    LogicCluster   string `gorm:"index:idx1,unique; column:logic_name"`
    PhysicClusters string `gorm:"column:physic_clusters;type:TEXT"`
}

func (TblLogic) TableName() string { return SQLITE_TBL_LOGIC }

type TblQueryHistory struct {
    Cluster    string    `gorm:"index:idx1; column:cluster"`
    CheckSum   string    `gorm:"primaryKey; column:checksum"`
    QuerySql   string    `gorm:"column:query;type:TEXT"`
    CreateTime time.Time `gorm:"column:create_time"`
}

func (TblQueryHistory) TableName() string { return SQLITE_TBL_QUERY_HISTORY }

type TblTask struct {
    TaskId string `gorm:"primaryKey; column:task_id"`
    Status int    `gorm:"column:status"`
    Task   string `gorm:"column:config;type:TEXT"`
}

func (TblTask) TableName() string { return SQLITE_TBL_TASK }

type TblBackup struct {
    BackupId    string `gorm:"column:backup_id"`
    ClusterName string `gorm:"column:cluster_name"`
    UpdateTime  string `gorm:"column:update_time"`
    Backup      string `gorm:"column:backup;type:TEXT"`
}

func (TblBackup) TableName() string { return SQLITE_TBL_BACKUP }

type TblBackupPolicy struct {
    PolicyID     string `gorm:"column:policy_id;primaryKey"`
    ClusterName  string `gorm:"column:cluster_name;index:idx_bp_cluster_db_table"`
    Database     string `gorm:"column:database_name;index:idx_bp_cluster_db_table"`
    Table        string `gorm:"column:table_name;index:idx_bp_cluster_db_table"`
    Instance     string `gorm:"column:instance;index:idx_bp_instance"`
    ScheduleType string `gorm:"column:schedule_type"`
    Enabled      bool   `gorm:"column:enabled"`
    Deleted      bool   `gorm:"column:deleted"`
    Policy       string `gorm:"column:policy;type:TEXT"`
    UpdateTime   string `gorm:"column:update_time"`
}

func (TblBackupPolicy) TableName() string { return SQLITE_TBL_BACKUP_POLICY }

type TblBackupRun struct {
    RunID       string    `gorm:"column:run_id;primaryKey"`
    PolicyID    string    `gorm:"column:policy_id;index:idx_br_policy_started"`
    ClusterName string    `gorm:"column:cluster_name;index:idx_br_table_started"`
    Database    string    `gorm:"column:database_name;index:idx_br_table_started"`
    Table       string    `gorm:"column:table_name;index:idx_br_table_started"`
    Status      string    `gorm:"column:status;index:idx_br_status_instance"`
    Instance    string    `gorm:"column:instance;index:idx_br_status_instance"`
    StartedAt   time.Time `gorm:"column:started_at;index:idx_br_policy_started;index:idx_br_table_started"`
    Run         string    `gorm:"column:run;type:TEXT"`
    CreateTime  time.Time `gorm:"column:create_time"`
}

func (TblBackupRun) TableName() string { return SQLITE_TBL_BACKUP_RUN }

// TblMeta 是单一 KV 表，目前存 migrated_from / schema_version。
type TblMeta struct {
    Key   string `gorm:"primaryKey; column:key"`
    Value string `gorm:"column:value;type:TEXT"`
}

func (TblMeta) TableName() string { return SQLITE_TBL_META }
```

- [ ] **Step 2: 不单独 commit**（与 Task 11 一起）

---

## Task 11: 创建 `repository/sqlite/sqlite.go` —— `Init` / `Begin` / `Commit` / `Rollback` / `wrapError`

**Files:**
- Create: `repository/sqlite/sqlite.go`
- Create: `repository/sqlite/sqlite_test.go`

- [ ] **Step 1: 写测试 `repository/sqlite/sqlite_test.go`**

```go
package sqlite

import (
    "errors"
    "os"
    "path/filepath"
    "testing"

    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/model"
    "github.com/housepower/ckman/repository"
)

func TestMain(m *testing.M) {
    log.InitLoggerConsole()
    os.Exit(m.Run())
}

// newTestSP 在临时目录创建 SQLitePersistent，避开任何 legacy 文件，
// 保证测试 _meta.migrated_from == "(fresh install)"。
func newTestSP(t *testing.T) *SQLitePersistent {
    t.Helper()
    dir := t.TempDir()
    sp := NewSQLitePersistent()
    if err := sp.Init(LocalConfig{ConfigDir: dir, ConfigFile: "testdb"}); err != nil {
        t.Fatalf("init: %v", err)
    }
    return sp
}

func TestSQLite_FreshInit(t *testing.T) {
    sp := newTestSP(t)
    v, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
    if err != nil {
        t.Fatalf("read meta: %v", err)
    }
    if v != META_FRESH_INSTALL {
        t.Fatalf("expected fresh install, got %q", v)
    }
    if _, err := os.Stat(filepath.Join(sp.Config.ConfigDir, "testdb.db")); err != nil {
        t.Fatalf("db file missing: %v", err)
    }
}

func TestSQLite_TxCommit(t *testing.T) {
    sp := newTestSP(t)
    if err := sp.Begin(); err != nil {
        t.Fatalf("begin: %v", err)
    }
    if err := sp.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1"}); err != nil {
        t.Fatalf("create: %v", err)
    }
    if err := sp.Commit(); err != nil {
        t.Fatalf("commit: %v", err)
    }
    if _, err := sp.GetClusterbyName("ck1"); err != nil {
        t.Fatalf("get after commit: %v", err)
    }
}

func TestSQLite_TxRollback(t *testing.T) {
    sp := newTestSP(t)
    if err := sp.Begin(); err != nil {
        t.Fatalf("begin: %v", err)
    }
    if err := sp.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1"}); err != nil {
        t.Fatalf("create: %v", err)
    }
    if err := sp.Rollback(); err != nil {
        t.Fatalf("rollback: %v", err)
    }
    _, err := sp.GetClusterbyName("ck1")
    if !errors.Is(err, repository.ErrRecordNotFound) {
        t.Fatalf("expected ErrRecordNotFound, got %v", err)
    }
}
```

注意这里依赖 `CreateCluster / GetClusterbyName` —— Task 12 才会实现。先把测试写出来，**Task 11 步骤本身只跑 `TestSQLite_FreshInit`**，另外两个跑会 fail，等 Task 12 完成后再补跑。

- [ ] **Step 2: 创建 `repository/sqlite/sqlite.go`**

```go
package sqlite

import (
    "encoding/json"
    "fmt"

    sqlitedriver "github.com/glebarez/sqlite"
    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/repository"
    "github.com/pkg/errors"
    "gorm.io/gorm"
    "moul.io/zapgorm2"
)

type SQLitePersistent struct {
    Config   LocalConfig
    Client   *gorm.DB
    ParentDB *gorm.DB
}

func NewSQLitePersistent() *SQLitePersistent {
    return &SQLitePersistent{}
}

func (sp *SQLitePersistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
    var cfg LocalConfig
    data, err := json.Marshal(configMap)
    if err != nil {
        log.Logger.Errorf("marshal sqlite configMap failed: %v", err)
        return nil
    }
    if err := json.Unmarshal(data, &cfg); err != nil {
        log.Logger.Errorf("unmarshal sqlite config failed: %v", err)
        return nil
    }
    return cfg
}

func (sp *SQLitePersistent) Init(cfgIn interface{}) error {
    if cfgIn == nil {
        cfgIn = LocalConfig{}
    }
    sp.Config = cfgIn.(LocalConfig)
    sp.Config.Normalize()

    dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", sp.Config.DBPath())

    logger := zapgorm2.New(log.ZapLog)
    logger.SetAsDefault()
    db, err := gorm.Open(sqlitedriver.Open(dsn), &gorm.Config{Logger: logger})
    if err != nil {
        return errors.Wrap(err, "")
    }
    sp.Client = db
    sp.ParentDB = db

    if err := db.AutoMigrate(
        &TblCluster{},
        &TblLogic{},
        &TblQueryHistory{},
        &TblTask{},
        &TblBackup{},
        &TblBackupPolicy{},
        &TblBackupRun{},
        &TblMeta{},
    ); err != nil {
        return errors.Wrap(err, "")
    }

    // Task 18 在这里挂迁移逻辑：
    if err := sp.migrateLegacyIfAny(); err != nil {
        return errors.Wrap(err, "")
    }
    return nil
}

func (sp *SQLitePersistent) Begin() error {
    if sp.Client != sp.ParentDB {
        return repository.ErrTransActionBegin
    }
    tx := sp.Client.Begin()
    sp.Client = tx
    return tx.Error
}

func (sp *SQLitePersistent) Commit() error {
    if sp.Client == sp.ParentDB {
        return repository.ErrTransActionEnd
    }
    tx := sp.Client.Commit()
    sp.Client = sp.ParentDB
    return tx.Error
}

func (sp *SQLitePersistent) Rollback() error {
    if sp.Client == sp.ParentDB {
        return repository.ErrTransActionEnd
    }
    tx := sp.Client.Rollback()
    sp.Client = sp.ParentDB
    return tx.Error
}

// wrapError 把 gorm 错误归一化到 repository.ErrRecordNotFound（沿用 mysql 后端模式）。
func wrapError(err error) error {
    if err == nil {
        return nil
    }
    if errors.Is(err, gorm.ErrRecordNotFound) {
        return repository.ErrRecordNotFound
    }
    return errors.Wrap(err, "")
}

// readMeta / writeMeta 由 Task 18 的 migrate.go 共用。先在这里给出。
func readMeta(db *gorm.DB, key string) (string, error) {
    var m TblMeta
    if err := db.Where("key = ?", key).First(&m).Error; err != nil {
        if errors.Is(err, gorm.ErrRecordNotFound) {
            return "", nil
        }
        return "", errors.Wrap(err, "")
    }
    return m.Value, nil
}

func writeMeta(db *gorm.DB, key, value string) error {
    m := TblMeta{Key: key, Value: value}
    // upsert
    if err := db.Save(&m).Error; err != nil {
        return errors.Wrap(err, "")
    }
    return nil
}
```

- [ ] **Step 3: 临时桩 —— 让 `migrateLegacyIfAny` 在 Task 18 之前可编译**

Task 18 才正式实现 `migrateLegacyIfAny`，现在加一个返回 nil 的桩，先用 `_meta.migrated_from` 直接写 fresh install。在 `repository/sqlite/sqlite.go` 末尾追加：

```go
// 临时桩：Task 18 用真实实现替换。
func (sp *SQLitePersistent) migrateLegacyIfAny() error {
    cur, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
    if err != nil {
        return err
    }
    if cur != "" {
        return nil
    }
    return writeMeta(sp.Client, METAKEY_MIGRATED_FROM, META_FRESH_INSTALL)
}
```

- [ ] **Step 4: 编译验证（仍然缺 CRUD 方法，所以 PersistentMgr 接口不满足）**

为了让 factory 能编译，临时让 SQLitePersistent 仅满足 `PersistentBase`，不让它实现 `PersistentMgr`。但 `Factory.CreatePersistent()` 返回 `PersistentMgr` 接口，会失败。

**解决：** 让 factory 的 `CreatePersistent` 临时改为返回 `*SQLitePersistent`（直接类型），等 Task 12-17 全部完成后再恢复接口。

修改 `factory.go`：

```go
// 临时类型断言通过 —— Task 12-17 完成后此处自动满足 PersistentMgr。
func (factory *Factory) CreatePersistent() repository.PersistentMgr {
    sp := NewSQLitePersistent()
    var _ repository.PersistentMgr = sp // Task 12-17 完成前会编译报错；此处先注释掉
    return sp
}
```

实际可行办法：注释 `var _` 那行，留下断言失败的编译错误（让接口缺失方法显式暴露）。

更简洁：注释掉 `factory.go` 里 `init()` 整段直到 Task 18，避免 sqlite 包被无意识引入。在文件顶部加：

```go
// Task 18 完成后取消注释 init —— 在此之前 sqlite 包不参与注册，
// 仍由 main.go 显式 import 触发编译期检查。
// func init() {
//     repository.RegistePersistent(NewFactory)
// }
```

- [ ] **Step 5: 跑 fresh init 测试**

```bash
go test ./repository/sqlite/ -run TestSQLite_FreshInit -v
```

Expected: PASS（仅这一个测试不依赖 CRUD）

- [ ] **Step 6: Commit**

```bash
git add repository/sqlite/
git commit -m "feat(sqlite): add backend skeleton (init, tx, meta, models)

Init opens SQLite via glebarez (pure-Go), enables WAL + busy_timeout,
AutoMigrates all tables. Begin/Commit/Rollback mirror mysql backend.
CRUD methods land in Tasks 12-17; init() registration enabled in Task 18."
```

---

## Task 12: Cluster + Logic CRUD

**Files:**
- Modify: `repository/sqlite/sqlite.go`（追加方法）
- Modify: `repository/sqlite/sqlite_test.go`（追加测试）

**Pattern reference:** `repository/mysql/mysql.go:119-219`（Cluster）和 `:259-345`（Logic）。SQLite 实现完全镜像该模式，仅把 `MysqlPersistent` 替换为 `SQLitePersistent`。

- [ ] **Step 1: 写测试**

在 `sqlite_test.go` 追加：

```go
func TestSQLite_ClusterCRUD(t *testing.T) {
    sp := newTestSP(t)
    cfg := model.CKManClickHouseConfig{Cluster: "ck1", Comment: "test"}
    if err := sp.CreateCluster(cfg); err != nil {
        t.Fatalf("create: %v", err)
    }
    if !sp.ClusterExists("ck1") {
        t.Fatalf("ClusterExists should return true")
    }
    got, err := sp.GetClusterbyName("ck1")
    if err != nil {
        t.Fatalf("get: %v", err)
    }
    if got.Cluster != "ck1" || got.Comment != "test" {
        t.Fatalf("unexpected: %+v", got)
    }

    cfg.Comment = "updated"
    if err := sp.UpdateCluster(cfg); err != nil {
        t.Fatalf("update: %v", err)
    }
    got2, _ := sp.GetClusterbyName("ck1")
    if got2.Comment != "updated" {
        t.Fatalf("update lost: %+v", got2)
    }

    all, _ := sp.GetAllClusters()
    if len(all) != 1 {
        t.Fatalf("GetAll size: %d", len(all))
    }

    if err := sp.DeleteCluster("ck1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
    if sp.ClusterExists("ck1") {
        t.Fatalf("ClusterExists should return false after delete")
    }
}

func TestSQLite_LogicCRUD(t *testing.T) {
    sp := newTestSP(t)
    if err := sp.CreateLogicCluster("L1", []string{"ck1", "ck2"}); err != nil {
        t.Fatalf("create: %v", err)
    }
    physics, err := sp.GetLogicClusterbyName("L1")
    if err != nil {
        t.Fatalf("get: %v", err)
    }
    if len(physics) != 2 {
        t.Fatalf("expected 2 physics, got %d", len(physics))
    }

    if err := sp.UpdateLogicCluster("L1", []string{"ck1", "ck2", "ck3"}); err != nil {
        t.Fatalf("update: %v", err)
    }
    physics2, _ := sp.GetLogicClusterbyName("L1")
    if len(physics2) != 3 {
        t.Fatalf("update lost: %v", physics2)
    }

    if err := sp.DeleteLogicCluster("L1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
}
```

- [ ] **Step 2: 验证测试失败**

```bash
go test ./repository/sqlite/ -run "TestSQLite_ClusterCRUD|TestSQLite_LogicCRUD" -v
```

Expected: FAIL with `sp.CreateCluster undefined` 等

- [ ] **Step 3: 实现 Cluster 方法**

在 `sqlite.go` 末尾追加。完全镜像 `repository/mysql/mysql.go:119-219` 的实现，把 `mp` 改 `sp`、`MysqlPersistent` 改 `SQLitePersistent`，**且 `errors.Is(err, gorm.ErrRecordNotFound)` 改为统一调用 `wrapError(err)`**：

```go
func (sp *SQLitePersistent) ClusterExists(cluster string) bool {
    _, err := sp.GetClusterbyName(cluster)
    return err == nil
}

func (sp *SQLitePersistent) GetClusterbyName(cluster string) (model.CKManClickHouseConfig, error) {
    var tbl TblCluster
    if err := sp.Client.Where("cluster_name = ?", cluster).First(&tbl).Error; err != nil {
        return model.CKManClickHouseConfig{}, wrapError(err)
    }
    var conf model.CKManClickHouseConfig
    if err := json.Unmarshal([]byte(tbl.Config), &conf); err != nil {
        return model.CKManClickHouseConfig{}, errors.Wrap(err, "")
    }
    repository.DecodePasswd(&conf)
    return conf, nil
}

func (sp *SQLitePersistent) GetAllClusters() (map[string]model.CKManClickHouseConfig, error) {
    var tbls []TblCluster
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make(map[string]model.CKManClickHouseConfig, len(tbls))
    for _, tbl := range tbls {
        var conf model.CKManClickHouseConfig
        if err := json.Unmarshal([]byte(tbl.Config), &conf); err != nil {
            return nil, errors.Wrap(err, "")
        }
        repository.DecodePasswd(&conf)
        out[conf.Cluster] = conf
    }
    return out, nil
}

func (sp *SQLitePersistent) CreateCluster(conf model.CKManClickHouseConfig) error {
    repository.EncodePasswd(&conf)
    raw, err := json.Marshal(conf)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tbl := TblCluster{ClusterName: conf.Cluster, Config: string(raw)}
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateCluster(conf model.CKManClickHouseConfig) error {
    repository.EncodePasswd(&conf)
    raw, err := json.Marshal(conf)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tx := sp.Client.Model(&TblCluster{}).Where("cluster_name = ?", conf.Cluster).Updates(map[string]interface{}{
        "config": string(raw),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteCluster(clusterName string) error {
    return wrapError(sp.Client.Where("cluster_name = ?", clusterName).Delete(&TblCluster{}).Error)
}
```

注意 import 在 `sqlite.go` 文件头部加 `"github.com/housepower/ckman/model"`。

- [ ] **Step 4: 实现 Logic 方法（同样镜像 mysql backend）**

```go
func (sp *SQLitePersistent) GetLogicClusterbyName(logic string) ([]string, error) {
    var tbl TblLogic
    if err := sp.Client.Where("logic_name = ?", logic).First(&tbl).Error; err != nil {
        return nil, wrapError(err)
    }
    var physics []string
    if err := json.Unmarshal([]byte(tbl.PhysicClusters), &physics); err != nil {
        return nil, errors.Wrap(err, "")
    }
    return common.ArrayDistinct(physics), nil
}

func (sp *SQLitePersistent) GetAllLogicClusters() (map[string][]string, error) {
    var tbls []TblLogic
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make(map[string][]string, len(tbls))
    for _, tbl := range tbls {
        var physics []string
        if err := json.Unmarshal([]byte(tbl.PhysicClusters), &physics); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out[tbl.LogicCluster] = common.ArrayDistinct(physics)
    }
    return out, nil
}

func (sp *SQLitePersistent) CreateLogicCluster(logic string, physics []string) error {
    raw, err := json.Marshal(common.ArrayDistinct(physics))
    if err != nil {
        return errors.Wrap(err, "")
    }
    tbl := TblLogic{LogicCluster: logic, PhysicClusters: string(raw)}
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateLogicCluster(logic string, physics []string) error {
    raw, err := json.Marshal(common.ArrayDistinct(physics))
    if err != nil {
        return errors.Wrap(err, "")
    }
    tx := sp.Client.Model(&TblLogic{}).Where("logic_name = ?", logic).Updates(map[string]interface{}{
        "physic_clusters": string(raw),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteLogicCluster(clusterName string) error {
    return wrapError(sp.Client.Where("logic_name = ?", clusterName).Delete(&TblLogic{}).Error)
}
```

import 加 `"github.com/housepower/ckman/common"`。

- [ ] **Step 5: 跑测试**

```bash
go test ./repository/sqlite/ -run "TestSQLite_ClusterCRUD|TestSQLite_LogicCRUD|TestSQLite_Tx" -v
```

Expected: PASS（事务测试现在也能跑了，因为 `CreateCluster` 已实现）

- [ ] **Step 6: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/sqlite_test.go
git commit -m "feat(sqlite): implement Cluster and Logic CRUD"
```

---

## Task 13: QueryHistory CRUD

**Files:**
- Modify: `repository/sqlite/sqlite.go`、`repository/sqlite/sqlite_test.go`

**Pattern reference:** `repository/mysql/mysql.go:347-455`

- [ ] **Step 1: 写测试**

```go
func TestSQLite_QueryHistoryCRUD(t *testing.T) {
    sp := newTestSP(t)
    qh := model.QueryHistory{CheckSum: "qh1", Cluster: "ck1", QuerySql: "SELECT 1"}
    if err := sp.CreateQueryHistory(qh); err != nil {
        t.Fatalf("create: %v", err)
    }
    got, err := sp.GetQueryHistoryByCheckSum("qh1")
    if err != nil {
        t.Fatalf("get: %v", err)
    }
    if got.QuerySql != "SELECT 1" {
        t.Fatalf("unexpected: %+v", got)
    }

    qh.QuerySql = "SELECT 2"
    if err := sp.UpdateQueryHistory(qh); err != nil {
        t.Fatalf("update: %v", err)
    }
    got2, _ := sp.GetQueryHistoryByCheckSum("qh1")
    if got2.QuerySql != "SELECT 2" {
        t.Fatalf("update lost: %+v", got2)
    }

    if c := sp.GetQueryHistoryCount("ck1"); c != 1 {
        t.Fatalf("count: %d", c)
    }
    all, _ := sp.GetAllQueryHistory()
    if len(all) != 1 {
        t.Fatalf("all: %d", len(all))
    }
    byCluster, _ := sp.GetQueryHistoryByCluster("ck1")
    if len(byCluster) != 1 {
        t.Fatalf("by cluster: %d", len(byCluster))
    }
    if _, err := sp.GetEarliestQuery("ck1"); err != nil {
        t.Fatalf("earliest: %v", err)
    }

    if err := sp.DeleteQueryHistory("qh1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
}
```

- [ ] **Step 2: 实现方法**

完整代码镜像 `repository/mysql/mysql.go:347-455`，把 `MysqlPersistent` → `SQLitePersistent`，错误处理用 `wrapError`：

```go
func (sp *SQLitePersistent) GetAllQueryHistory() (map[string]model.QueryHistory, error) {
    var tbls []TblQueryHistory
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make(map[string]model.QueryHistory, len(tbls))
    for _, tbl := range tbls {
        out[tbl.CheckSum] = model.QueryHistory{
            CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
            QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
        }
    }
    return out, nil
}

func (sp *SQLitePersistent) GetQueryHistoryByCluster(cluster string) ([]model.QueryHistory, error) {
    var tbls []TblQueryHistory
    if err := sp.Client.Where("cluster = ?", cluster).Order("create_time DESC").Limit(100).Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.QueryHistory, 0, len(tbls))
    for _, tbl := range tbls {
        out = append(out, model.QueryHistory{
            CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
            QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
        })
    }
    return out, nil
}

func (sp *SQLitePersistent) GetQueryHistoryByCheckSum(checksum string) (model.QueryHistory, error) {
    var tbl TblQueryHistory
    if err := sp.Client.Where("checksum = ?", checksum).First(&tbl).Error; err != nil {
        return model.QueryHistory{}, wrapError(err)
    }
    return model.QueryHistory{
        CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
        QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
    }, nil
}

func (sp *SQLitePersistent) CreateQueryHistory(qh model.QueryHistory) error {
    qh.CreateTime = time.Now()
    tbl := TblQueryHistory{
        CheckSum: qh.CheckSum, Cluster: qh.Cluster,
        QuerySql: qh.QuerySql, CreateTime: qh.CreateTime,
    }
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateQueryHistory(qh model.QueryHistory) error {
    qh.CreateTime = time.Now()
    tx := sp.Client.Model(&TblQueryHistory{}).Where("checksum = ?", qh.CheckSum).Updates(map[string]interface{}{
        "cluster":     qh.Cluster,
        "query":       qh.QuerySql,
        "create_time": qh.CreateTime,
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteQueryHistory(checksum string) error {
    tx := sp.Client.Where("checksum = ?", checksum).Delete(&TblQueryHistory{})
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) GetQueryHistoryCount(cluster string) int64 {
    var count int64
    sp.Client.Model(&TblQueryHistory{}).Where("cluster = ?", cluster).Count(&count)
    return count
}

func (sp *SQLitePersistent) GetEarliestQuery(cluster string) (model.QueryHistory, error) {
    var tbl TblQueryHistory
    if err := sp.Client.Where("cluster = ?", cluster).Order("create_time ASC").First(&tbl).Error; err != nil {
        return model.QueryHistory{}, wrapError(err)
    }
    return model.QueryHistory{
        CheckSum: tbl.CheckSum, Cluster: tbl.Cluster,
        QuerySql: tbl.QuerySql, CreateTime: tbl.CreateTime,
    }, nil
}
```

`sqlite.go` import 加 `"time"`。

- [ ] **Step 3: 跑测试**

```bash
go test ./repository/sqlite/ -run TestSQLite_QueryHistory -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/sqlite_test.go
git commit -m "feat(sqlite): implement QueryHistory CRUD"
```

---

## Task 14: Task CRUD

**Files:**
- Modify: `repository/sqlite/sqlite.go`、`repository/sqlite/sqlite_test.go`

**Pattern reference:** `repository/mysql/mysql.go:457-545`

- [ ] **Step 1: 写测试**

```go
func TestSQLite_TaskCRUD(t *testing.T) {
    sp := newTestSP(t)
    task := model.Task{TaskId: "t1", Status: model.TaskStatusWaiting, ServerIp: "1.2.3.4"}
    if err := sp.CreateTask(task); err != nil {
        t.Fatalf("create: %v", err)
    }
    got, err := sp.GetTaskbyTaskId("t1")
    if err != nil {
        t.Fatalf("get: %v", err)
    }
    if got.TaskId != "t1" {
        t.Fatalf("unexpected: %+v", got)
    }

    task.Status = model.TaskStatusRunning
    if err := sp.UpdateTask(task); err != nil {
        t.Fatalf("update: %v", err)
    }

    pending, _ := sp.GetPengdingTasks("1.2.3.4")
    if len(pending) != 0 {
        t.Fatalf("running task should not be pending: %d", len(pending))
    }

    if c := sp.GetEffectiveTaskCount(); c != 1 {
        t.Fatalf("effective count: %d", c)
    }

    if err := sp.DeleteTask("t1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
}
```

- [ ] **Step 2: 实现方法**

```go
func (sp *SQLitePersistent) CreateTask(task model.Task) error {
    task.CreateTime = time.Now()
    task.UpdateTime = task.CreateTime
    raw, err := json.Marshal(task)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tbl := TblTask{TaskId: task.TaskId, Status: task.Status, Task: string(raw)}
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateTask(task model.Task) error {
    task.UpdateTime = time.Now()
    raw, err := json.Marshal(task)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tx := sp.Client.Model(&TblTask{}).Where("task_id = ?", task.TaskId).Updates(map[string]interface{}{
        "status": task.Status,
        "config": string(raw),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteTask(id string) error {
    return wrapError(sp.Client.Where("task_id = ?", id).Delete(&TblTask{}).Error)
}

func (sp *SQLitePersistent) GetAllTasks() ([]model.Task, error) {
    var tbls []TblTask
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.Task, 0, len(tbls))
    for _, tbl := range tbls {
        var t model.Task
        if err := json.Unmarshal([]byte(tbl.Task), &t); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, t)
    }
    return out, nil
}

func (sp *SQLitePersistent) GetEffectiveTaskCount() int64 {
    var count int64
    sp.Client.Model(&TblTask{}).
        Where("status IN (?, ?)", model.TaskStatusRunning, model.TaskStatusWaiting).
        Count(&count)
    return count
}

func (sp *SQLitePersistent) GetPengdingTasks(serverIp string) ([]model.Task, error) {
    var tbls []TblTask
    if err := sp.Client.Where("status = ?", model.TaskStatusWaiting).Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.Task, 0, len(tbls))
    for _, tbl := range tbls {
        var task model.Task
        if err := json.Unmarshal([]byte(tbl.Task), &task); err != nil {
            return nil, errors.Wrap(err, "")
        }
        if task.ServerIp == serverIp {
            out = append(out, task)
        }
    }
    return out, nil
}

func (sp *SQLitePersistent) GetTaskbyTaskId(id string) (model.Task, error) {
    var tbl TblTask
    if err := sp.Client.Where("task_id = ?", id).First(&tbl).Error; err != nil {
        return model.Task{}, wrapError(err)
    }
    var task model.Task
    if err := json.Unmarshal([]byte(tbl.Task), &task); err != nil {
        return model.Task{}, errors.Wrap(err, "")
    }
    return task, nil
}
```

- [ ] **Step 3: 跑测试**

```bash
go test ./repository/sqlite/ -run TestSQLite_TaskCRUD -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/sqlite_test.go
git commit -m "feat(sqlite): implement Task CRUD"
```

---

## Task 15: Backup CRUD

**Files:**
- Modify: `repository/sqlite/sqlite.go`、`repository/sqlite/sqlite_test.go`

**Pattern reference:** `repository/mysql/mysql.go:459-588`

- [ ] **Step 1: 写测试**

```go
func TestSQLite_BackupCRUD(t *testing.T) {
    sp := newTestSP(t)
    b := model.Backup{
        BackupId: "b1", ClusterName: "ck1",
        Database: "db", Table: "t",
        Operation: "BACKUP", ScheduleType: "ONCE",
    }
    if err := sp.CreateBackup(b); err != nil {
        t.Fatalf("create: %v", err)
    }
    got, err := sp.GetBackupById("b1")
    if err != nil || got.BackupId != "b1" {
        t.Fatalf("get by id: %v %+v", err, got)
    }
    byTable, err := sp.GetBackupByTable("ck1", "db", "t")
    if err != nil || byTable.BackupId != "b1" {
        t.Fatalf("get by table: %v %+v", err, byTable)
    }
    all, _ := sp.GetAllBackups("ck1")
    if len(all) != 1 {
        t.Fatalf("all: %d", len(all))
    }
    byOp, _ := sp.GetbackupByOperation("BACKUP")
    if len(byOp) != 1 {
        t.Fatalf("by op: %d", len(byOp))
    }
    bySched, _ := sp.GetBackupByShechuleType("ONCE")
    if len(bySched) != 1 {
        t.Fatalf("by sched: %d", len(bySched))
    }

    b.Operation = "RESTORE"
    if err := sp.UpdateBackup(b); err != nil {
        t.Fatalf("update: %v", err)
    }

    if err := sp.DeleteBackup("b1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
}
```

- [ ] **Step 2: 实现方法**

完整代码镜像 `repository/mysql/mysql.go:459-588`。注意 `GetBackupByTable / GetbackupByOperation / GetBackupByShechuleType` 都是 "全表 scan + 内存过滤" 的实现（因为业务字段在 JSON blob 内）—— 直接照搬即可：

```go
func (sp *SQLitePersistent) CreateBackup(backup model.Backup) error {
    backup.CreateTime = time.Now()
    backup.UpdateTime = backup.CreateTime
    raw, err := json.Marshal(backup)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tbl := TblBackup{
        BackupId: backup.BackupId, ClusterName: backup.ClusterName,
        UpdateTime: backup.UpdateTime.Format("2006-01-02 15:04:05.999"),
        Backup:     string(raw),
    }
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateBackup(backup model.Backup) error {
    backup.UpdateTime = time.Now()
    raw, err := json.Marshal(backup)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tx := sp.Client.Model(&TblBackup{}).Where("backup_id = ?", backup.BackupId).Updates(map[string]interface{}{
        "cluster_name": backup.ClusterName,
        "update_time":  backup.UpdateTime.Format("2006-01-02 15:04:05.999"),
        "backup":       string(raw),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteBackup(id string) error {
    return wrapError(sp.Client.Where("backup_id = ?", id).Delete(&TblBackup{}).Error)
}

func (sp *SQLitePersistent) GetAllBackups(cluster string) ([]model.Backup, error) {
    var tbls []TblBackup
    if err := sp.Client.Where("cluster_name = ?", cluster).Order("update_time DESC").Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.Backup, 0, len(tbls))
    for _, tbl := range tbls {
        var b model.Backup
        if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, b)
    }
    return out, nil
}

func (sp *SQLitePersistent) GetBackupById(id string) (model.Backup, error) {
    var tbl TblBackup
    if err := sp.Client.Where("backup_id = ?", id).First(&tbl).Error; err != nil {
        return model.Backup{}, wrapError(err)
    }
    var b model.Backup
    if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
        return model.Backup{}, errors.Wrap(err, "")
    }
    return b, nil
}

func (sp *SQLitePersistent) GetBackupByTable(cluster, database, table string) (model.Backup, error) {
    var tbls []TblBackup
    if err := sp.Client.Where("cluster_name = ?", cluster).Find(&tbls).Error; err != nil {
        return model.Backup{}, wrapError(err)
    }
    for _, tbl := range tbls {
        var b model.Backup
        if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
            return model.Backup{}, errors.Wrap(err, "")
        }
        if b.Database == database && b.Table == table {
            return b, nil
        }
    }
    return model.Backup{}, repository.ErrRecordNotFound
}

func (sp *SQLitePersistent) GetbackupByOperation(operation string) ([]model.Backup, error) {
    var tbls []TblBackup
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.Backup, 0)
    for _, tbl := range tbls {
        var b model.Backup
        if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
            return nil, errors.Wrap(err, "")
        }
        if b.Operation == operation {
            out = append(out, b)
        }
    }
    return out, nil
}

func (sp *SQLitePersistent) GetBackupByShechuleType(scheduleType string) ([]model.Backup, error) {
    var tbls []TblBackup
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.Backup, 0)
    for _, tbl := range tbls {
        var b model.Backup
        if err := json.Unmarshal([]byte(tbl.Backup), &b); err != nil {
            return nil, errors.Wrap(err, "")
        }
        if b.ScheduleType == scheduleType {
            out = append(out, b)
        }
    }
    return out, nil
}
```

- [ ] **Step 3: 跑测试**

```bash
go test ./repository/sqlite/ -run TestSQLite_BackupCRUD -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/sqlite_test.go
git commit -m "feat(sqlite): implement Backup CRUD"
```

---

## Task 16: BackupPolicy CRUD

**Files:**
- Modify: `repository/sqlite/sqlite.go`、`repository/sqlite/sqlite_test.go`

**Pattern reference:** `repository/mysql/mysql.go:590-688` 以及 Task 3 中的 `GetAllBackupPolicies`

- [ ] **Step 1: 写测试**

```go
func TestSQLite_BackupPolicyCRUD(t *testing.T) {
    sp := newTestSP(t)
    p := model.BackupPolicy{
        PolicyID: "p1", ClusterName: "ck1",
        Database: "db", Table: "t",
        Instance:     "1.2.3.4", ScheduleType: model.BACKUP_SCHEDULED,
        Enabled:      true,
    }
    if err := sp.CreateBackupPolicy(p); err != nil {
        t.Fatalf("create: %v", err)
    }
    got, err := sp.GetBackupPolicy("p1")
    if err != nil || got.PolicyID != "p1" {
        t.Fatalf("get: %v %+v", err, got)
    }

    got.Crontab = "0 5 * * *"
    if err := sp.UpdateBackupPolicy(got); err != nil {
        t.Fatalf("update: %v", err)
    }
    got2, _ := sp.GetBackupPolicy("p1")
    if got2.Crontab != "0 5 * * *" {
        t.Fatalf("update lost: %+v", got2)
    }

    byCluster, _ := sp.GetBackupPoliciesByCluster("ck1")
    if len(byCluster) != 1 {
        t.Fatalf("by cluster: %d", len(byCluster))
    }

    active, _ := sp.GetActiveScheduledPolicies("1.2.3.4")
    if len(active) != 1 {
        t.Fatalf("active: %d", len(active))
    }

    all, _ := sp.GetAllBackupPolicies()
    if len(all) != 1 {
        t.Fatalf("all: %d", len(all))
    }

    if err := sp.DeleteBackupPolicy("p1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
    got3, _ := sp.GetBackupPolicy("p1")
    if !got3.Deleted {
        t.Fatalf("delete should soft-delete: %+v", got3)
    }
}
```

- [ ] **Step 2: 实现方法**

完整代码镜像 `repository/mysql/mysql.go:590-688`，把 `MysqlPersistent` 改为 `SQLitePersistent`，错误处理统一走 `wrapError`。注意把 `"scheduled"` 字面量替换为 `model.BACKUP_SCHEDULED`：

```go
func (sp *SQLitePersistent) CreateBackupPolicy(p model.BackupPolicy) error {
    if p.CreateTime.IsZero() {
        p.CreateTime = time.Now()
    }
    p.UpdateTime = time.Now()
    raw, err := json.Marshal(p)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tbl := TblBackupPolicy{
        PolicyID: p.PolicyID, ClusterName: p.ClusterName,
        Database: p.Database, Table: p.Table, Instance: p.Instance,
        ScheduleType: p.ScheduleType,
        Enabled:      p.Enabled, Deleted: false,
        Policy:     string(raw),
        UpdateTime: p.UpdateTime.Format("2006-01-02 15:04:05.999"),
    }
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateBackupPolicy(p model.BackupPolicy) error {
    p.UpdateTime = time.Now()
    raw, err := json.Marshal(p)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tx := sp.Client.Model(&TblBackupPolicy{}).Where("policy_id = ?", p.PolicyID).Updates(map[string]interface{}{
        "cluster_name":  p.ClusterName,
        "database_name": p.Database,
        "table_name":    p.Table,
        "instance":      p.Instance,
        "schedule_type": p.ScheduleType,
        "enabled":       p.Enabled,
        "deleted":       p.Deleted,
        "policy":        string(raw),
        "update_time":   p.UpdateTime.Format("2006-01-02 15:04:05.999"),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteBackupPolicy(policyID string) error {
    tx := sp.Client.Model(&TblBackupPolicy{}).Where("policy_id = ?", policyID).Updates(map[string]interface{}{
        "deleted":     true,
        "enabled":     false,
        "update_time": time.Now().Format("2006-01-02 15:04:05.999"),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) GetBackupPolicy(policyID string) (model.BackupPolicy, error) {
    var tbl TblBackupPolicy
    if err := sp.Client.Where("policy_id = ?", policyID).First(&tbl).Error; err != nil {
        return model.BackupPolicy{}, wrapError(err)
    }
    var p model.BackupPolicy
    if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
        return model.BackupPolicy{}, errors.Wrap(err, "")
    }
    p.Deleted = tbl.Deleted
    p.Enabled = tbl.Enabled
    return p, nil
}

func (sp *SQLitePersistent) GetBackupPoliciesByCluster(cluster string) ([]model.BackupPolicy, error) {
    var tbls []TblBackupPolicy
    if err := sp.Client.Where("cluster_name = ? AND deleted = ?", cluster, false).Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.BackupPolicy, 0, len(tbls))
    for _, tbl := range tbls {
        var p model.BackupPolicy
        if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, p)
    }
    return out, nil
}

func (sp *SQLitePersistent) GetActiveScheduledPolicies(instance string) ([]model.BackupPolicy, error) {
    var tbls []TblBackupPolicy
    if err := sp.Client.Where("instance = ? AND enabled = ? AND schedule_type = ? AND deleted = ?",
        instance, true, model.BACKUP_SCHEDULED, false).Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.BackupPolicy, 0, len(tbls))
    for _, tbl := range tbls {
        var p model.BackupPolicy
        if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, p)
    }
    return out, nil
}

func (sp *SQLitePersistent) GetAllBackupPolicies() ([]model.BackupPolicy, error) {
    var tbls []TblBackupPolicy
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.BackupPolicy, 0, len(tbls))
    for _, tbl := range tbls {
        var p model.BackupPolicy
        if err := json.Unmarshal([]byte(tbl.Policy), &p); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, p)
    }
    return out, nil
}
```

- [ ] **Step 3: 跑测试**

```bash
go test ./repository/sqlite/ -run TestSQLite_BackupPolicyCRUD -v
```

Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/sqlite_test.go
git commit -m "feat(sqlite): implement BackupPolicy CRUD"
```

---

## Task 17: BackupRun CRUD

**Files:**
- Modify: `repository/sqlite/sqlite.go`、`repository/sqlite/sqlite_test.go`

**Pattern reference:** `repository/mysql/mysql.go:690-790` + Task 3 中的 `GetAllBackupRuns`

- [ ] **Step 1: 写测试**

```go
func TestSQLite_BackupRunCRUD(t *testing.T) {
    sp := newTestSP(t)
    r := model.BackupRun{
        RunID: "r1", PolicyID: "p1", ClusterName: "ck1",
        Database: "db", Table: "t",
        Status:    model.BACKUP_STATUS_QUEUED,
        StartedAt: time.Now(),
    }
    if err := sp.CreateBackupRun(r); err != nil {
        t.Fatalf("create: %v", err)
    }
    got, err := sp.GetBackupRun("r1")
    if err != nil || got.RunID != "r1" {
        t.Fatalf("get: %v %+v", err, got)
    }

    inflight, _ := sp.GetRunsInFlightByPolicy("p1")
    if len(inflight) != 1 {
        t.Fatalf("inflight by policy: %d", len(inflight))
    }

    ok, err := sp.MarkRunRunningIfQueued("r1", "1.2.3.4", time.Now())
    if err != nil || !ok {
        t.Fatalf("mark running: ok=%v err=%v", ok, err)
    }

    got2, _ := sp.GetBackupRun("r1")
    if got2.Status != model.BACKUP_STATUS_RUNNING {
        t.Fatalf("status not updated: %+v", got2)
    }

    inflight2, _ := sp.GetRunsInFlightByInstance("1.2.3.4")
    if len(inflight2) != 1 {
        t.Fatalf("inflight by instance: %d", len(inflight2))
    }

    byPolicy, _ := sp.GetRunsByPolicy("p1", 10, time.Time{})
    if len(byPolicy) != 1 {
        t.Fatalf("by policy: %d", len(byPolicy))
    }

    byTable, _ := sp.GetRunsByTable("ck1", "db", "t", 7)
    if len(byTable) != 1 {
        t.Fatalf("by table: %d", len(byTable))
    }

    all, _ := sp.GetAllBackupRuns()
    if len(all) != 1 {
        t.Fatalf("all: %d", len(all))
    }

    if err := sp.DeleteBackupRun("r1"); err != nil {
        t.Fatalf("delete: %v", err)
    }
}
```

- [ ] **Step 2: 实现方法**

```go
func (sp *SQLitePersistent) CreateBackupRun(r model.BackupRun) error {
    if r.CreateTime.IsZero() {
        r.CreateTime = time.Now()
    }
    raw, err := json.Marshal(r)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tbl := TblBackupRun{
        RunID: r.RunID, PolicyID: r.PolicyID,
        ClusterName: r.ClusterName, Database: r.Database, Table: r.Table,
        Status: r.Status, Instance: r.Instance,
        StartedAt:  r.StartedAt,
        Run:        string(raw),
        CreateTime: r.CreateTime,
    }
    return wrapError(sp.Client.Create(&tbl).Error)
}

func (sp *SQLitePersistent) UpdateBackupRun(r model.BackupRun) error {
    raw, err := json.Marshal(r)
    if err != nil {
        return errors.Wrap(err, "")
    }
    tx := sp.Client.Model(&TblBackupRun{}).Where("run_id = ?", r.RunID).Updates(map[string]interface{}{
        "status":     r.Status,
        "instance":   r.Instance,
        "started_at": r.StartedAt,
        "run":        string(raw),
    })
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) DeleteBackupRun(runID string) error {
    tx := sp.Client.Where("run_id = ?", runID).Delete(&TblBackupRun{})
    if tx.Error != nil {
        return wrapError(tx.Error)
    }
    if tx.RowsAffected == 0 {
        return repository.ErrRecordNotFound
    }
    return nil
}

func (sp *SQLitePersistent) GetBackupRun(runID string) (model.BackupRun, error) {
    var tbl TblBackupRun
    if err := sp.Client.Where("run_id = ?", runID).First(&tbl).Error; err != nil {
        return model.BackupRun{}, wrapError(err)
    }
    var r model.BackupRun
    if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
        return model.BackupRun{}, errors.Wrap(err, "")
    }
    return r, nil
}

func (sp *SQLitePersistent) GetRunsByPolicy(policyID string, limit int, before time.Time) ([]model.BackupRun, error) {
    q := sp.Client.Where("policy_id = ?", policyID)
    if !before.IsZero() {
        q = q.Where("started_at < ?", before)
    }
    q = q.Order("started_at DESC")
    if limit > 0 {
        q = q.Limit(limit)
    }
    var tbls []TblBackupRun
    if err := q.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.BackupRun, 0, len(tbls))
    for _, tbl := range tbls {
        var r model.BackupRun
        if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, r)
    }
    return out, nil
}

func (sp *SQLitePersistent) GetRunsByTable(cluster, database, table string, sinceDays int) ([]model.BackupRun, error) {
    cutoff := time.Now().AddDate(0, 0, -sinceDays)
    var tbls []TblBackupRun
    if err := sp.Client.Where(
        "cluster_name = ? AND database_name = ? AND table_name = ? AND started_at > ?",
        cluster, database, table, cutoff,
    ).Order("started_at DESC").Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    out := make([]model.BackupRun, 0, len(tbls))
    for _, tbl := range tbls {
        var r model.BackupRun
        if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, r)
    }
    return out, nil
}

func (sp *SQLitePersistent) GetRunsInFlightByPolicy(policyID string) ([]model.BackupRun, error) {
    var tbls []TblBackupRun
    if err := sp.Client.Where("policy_id = ? AND status IN (?, ?)",
        policyID, model.BACKUP_STATUS_QUEUED, model.BACKUP_STATUS_RUNNING).Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    return decodeBackupRuns(tbls)
}

func (sp *SQLitePersistent) GetRunsInFlightByInstance(instance string) ([]model.BackupRun, error) {
    var tbls []TblBackupRun
    if err := sp.Client.Where("instance = ? AND status IN (?, ?)",
        instance, model.BACKUP_STATUS_QUEUED, model.BACKUP_STATUS_RUNNING).Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    return decodeBackupRuns(tbls)
}

func (sp *SQLitePersistent) MarkRunRunningIfQueued(runID, instance string, startedAt time.Time) (bool, error) {
    tx := sp.Client.Model(&TblBackupRun{}).
        Where("run_id = ? AND status = ?", runID, model.BACKUP_STATUS_QUEUED).
        Updates(map[string]interface{}{
            "status":     model.BACKUP_STATUS_RUNNING,
            "instance":   instance,
            "started_at": startedAt,
        })
    if tx.Error != nil {
        return false, wrapError(tx.Error)
    }
    return tx.RowsAffected == 1, nil
}

func (sp *SQLitePersistent) GetAllBackupRuns() ([]model.BackupRun, error) {
    var tbls []TblBackupRun
    if err := sp.Client.Find(&tbls).Error; err != nil {
        return nil, wrapError(err)
    }
    return decodeBackupRuns(tbls)
}

func decodeBackupRuns(tbls []TblBackupRun) ([]model.BackupRun, error) {
    out := make([]model.BackupRun, 0, len(tbls))
    for _, tbl := range tbls {
        var r model.BackupRun
        if err := json.Unmarshal([]byte(tbl.Run), &r); err != nil {
            return nil, errors.Wrap(err, "")
        }
        out = append(out, r)
    }
    return out, nil
}
```

- [ ] **Step 3: 跑测试**

```bash
go test ./repository/sqlite/ -run TestSQLite_BackupRunCRUD -v
```

Expected: PASS

- [ ] **Step 4: 跑全部 sqlite 测试**

```bash
go test ./repository/sqlite/ -v
```

Expected: ALL PASS

- [ ] **Step 5: 整包编译**（main.go 仍 broken，但 repository/sqlite 满足 PersistentMgr 了）

```bash
go build ./repository/...
```

Expected: success

- [ ] **Step 6: 启用 factory.go 的 init()**

去掉 `repository/sqlite/factory.go` 中 `init()` 的注释。

- [ ] **Step 7: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/sqlite_test.go repository/sqlite/factory.go
git commit -m "feat(sqlite): implement BackupRun CRUD; enable factory registration

repository/sqlite now fully implements PersistentMgr and registers as
\"local\" policy. main.go still imports repository/local; Task 19 swaps."
```

---

## Task 18: 启动期 legacy 文件迁移

**Files:**
- Create: `repository/sqlite/migrate.go`
- Modify: `repository/sqlite/sqlite.go`（删除临时桩 `migrateLegacyIfAny`，下挪到 migrate.go）
- Modify: `repository/sqlite/sqlite_test.go`
- Create: `repository/sqlite/testdata/legacy_clusters.json`

- [ ] **Step 1: 写测试 testdata**

`repository/sqlite/testdata/legacy_clusters.json`：

```json
{
  "Clusters": {
    "ck1": {
      "Cluster": "ck1",
      "Comment": "from-legacy",
      "Hosts": ["10.0.0.1"]
    }
  },
  "Logics": {
    "L1": ["ck1"]
  },
  "QueryHistory": {
    "qh1": {
      "CheckSum": "qh1",
      "Cluster": "ck1",
      "QuerySql": "SELECT 1",
      "CreateTime": "2025-01-01T00:00:00Z"
    }
  },
  "Task": {},
  "Backup": {},
  "BackupPolicy": {
    "p1": {
      "PolicyID": "p1",
      "ClusterName": "ck1",
      "Database": "db",
      "Table": "t",
      "ScheduleType": "scheduled",
      "Enabled": true,
      "Deleted": false
    }
  },
  "BackupRun": {
    "br1": {
      "RunID": "br1",
      "PolicyID": "p1",
      "ClusterName": "ck1",
      "Database": "db",
      "Table": "t",
      "Status": "queued"
    }
  }
}
```

- [ ] **Step 2: 写测试 `repository/sqlite/sqlite_test.go` 追加**

```go
func TestMigrateFromJSON(t *testing.T) {
    dir := t.TempDir()
    // 把 testdata 复制到临时目录作为 conf/clusters.json
    src, err := os.ReadFile("testdata/legacy_clusters.json")
    if err != nil {
        t.Fatalf("read testdata: %v", err)
    }
    legacyPath := filepath.Join(dir, "clusters.json")
    if err := os.WriteFile(legacyPath, src, 0644); err != nil {
        t.Fatalf("write legacy: %v", err)
    }

    sp := NewSQLitePersistent()
    if err := sp.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
        t.Fatalf("init: %v", err)
    }

    if _, err := sp.GetClusterbyName("ck1"); err != nil {
        t.Fatalf("cluster ck1 missing after migrate: %v", err)
    }
    if _, err := sp.GetLogicClusterbyName("L1"); err != nil {
        t.Fatalf("logic L1 missing: %v", err)
    }
    if _, err := sp.GetBackupPolicy("p1"); err != nil {
        t.Fatalf("policy p1 missing: %v", err)
    }
    if _, err := sp.GetBackupRun("br1"); err != nil {
        t.Fatalf("run br1 missing: %v", err)
    }

    // 旧文件被改名
    if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
        t.Fatalf("legacy file should have been renamed away")
    }
    matches, _ := filepath.Glob(legacyPath + ".migrated.*")
    if len(matches) != 1 {
        t.Fatalf("expected one .migrated.* file, got %d", len(matches))
    }

    // meta 被写入
    v, _ := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
    if v == "" || v == META_FRESH_INSTALL {
        t.Fatalf("migrated_from not set correctly: %q", v)
    }
}

func TestMigrateIdempotent(t *testing.T) {
    dir := t.TempDir()
    src, _ := os.ReadFile("testdata/legacy_clusters.json")
    _ = os.WriteFile(filepath.Join(dir, "clusters.json"), src, 0644)

    sp1 := NewSQLitePersistent()
    if err := sp1.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
        t.Fatalf("first init: %v", err)
    }
    v1, _ := readMeta(sp1.Client, METAKEY_MIGRATED_FROM)

    // 重新打开 —— 不应该再次迁移
    sp2 := NewSQLitePersistent()
    if err := sp2.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
        t.Fatalf("second init: %v", err)
    }
    v2, _ := readMeta(sp2.Client, METAKEY_MIGRATED_FROM)
    if v1 != v2 {
        t.Fatalf("meta changed on idempotent re-init: %q -> %q", v1, v2)
    }
}

func TestMigrateLegacyCorrupt(t *testing.T) {
    dir := t.TempDir()
    _ = os.WriteFile(filepath.Join(dir, "clusters.json"), []byte("{not valid json"), 0644)

    sp := NewSQLitePersistent()
    err := sp.Init(LocalConfig{Format: "json", ConfigDir: dir, ConfigFile: "clusters"})
    if err == nil {
        t.Fatalf("expected init to fail on corrupt legacy file")
    }
}
```

- [ ] **Step 3: 删除 `sqlite.go` 末尾的临时桩 `migrateLegacyIfAny`**

- [ ] **Step 4: 创建 `repository/sqlite/migrate.go`**

```go
package sqlite

import (
    "fmt"
    "os"
    "path/filepath"
    "time"

    "github.com/housepower/ckman/cmd/migrate"
    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/repository/legacyjson"
    "github.com/pkg/errors"
)

// migrateLegacyIfAny 是 Init() 调用入口。
// 算法见 docs/superpowers/specs/2026-05-16-local-sqlite-persistent-design.md §7.
func (sp *SQLitePersistent) migrateLegacyIfAny() error {
    cur, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
    if err != nil {
        return err
    }
    if cur != "" {
        return nil // 已迁移过 / 已记录 fresh install
    }

    legacyPath := sp.detectLegacyPath()
    if legacyPath == "" {
        return writeMeta(sp.Client, METAKEY_MIGRATED_FROM, META_FRESH_INSTALL)
    }

    // 旧文件存在 —— 走标准迁移路径。
    return sp.runLegacyMigration(legacyPath)
}

// detectLegacyPath 按 Format 字段定位旧文件；空则先试 json 再试 yaml。
func (sp *SQLitePersistent) detectLegacyPath() string {
    candidates := []string{}
    switch sp.Config.Format {
    case "json":
        candidates = []string{sp.Config.LegacyJSONPath()}
    case "yaml":
        candidates = []string{sp.Config.LegacyYAMLPath()}
    default:
        candidates = []string{sp.Config.LegacyJSONPath(), sp.Config.LegacyYAMLPath()}
    }
    for _, c := range candidates {
        if _, err := os.Stat(c); err == nil {
            return c
        }
    }
    return ""
}

func (sp *SQLitePersistent) runLegacyMigration(legacyPath string) error {
    legacyCfg := legacyjson.LocalConfig{
        Format:     formatFromPath(legacyPath),
        ConfigDir:  filepath.Dir(legacyPath),
        ConfigFile: trimExt(filepath.Base(legacyPath)),
    }
    src, err := legacyjson.NewReader(legacyCfg)
    if err != nil {
        return errors.Wrapf(err, "open legacy file %s", legacyPath)
    }

    if err := sp.Begin(); err != nil {
        return errors.Wrap(err, "begin migration tx")
    }
    if err := migrate.MigrateBetween(src, sp); err != nil {
        _ = sp.Rollback()
        return errors.Wrap(err, "migrate")
    }
    stamp := fmt.Sprintf("%s@%s", filepath.Base(legacyPath), time.Now().UTC().Format(time.RFC3339))
    if err := writeMeta(sp.Client, METAKEY_MIGRATED_FROM, stamp); err != nil {
        _ = sp.Rollback()
        return errors.Wrap(err, "write meta")
    }
    if err := writeMeta(sp.Client, METAKEY_SCHEMA_VERSION, SQLITE_SCHEMA_VERSION); err != nil {
        _ = sp.Rollback()
        return errors.Wrap(err, "write schema version")
    }
    if err := sp.Commit(); err != nil {
        return errors.Wrap(err, "commit migration")
    }

    if err := renameLegacyWithTimestamp(legacyPath); err != nil {
        log.Logger.Warnf("migrated successfully but failed to rename legacy file %s: %v", legacyPath, err)
    }
    log.Logger.Infof("local persistent: migrated from %s -> %s", legacyPath, sp.Config.DBPath())
    return nil
}

func formatFromPath(p string) string {
    switch filepath.Ext(p) {
    case ".yaml", ".yml":
        return "yaml"
    default:
        return "json"
    }
}

func trimExt(name string) string {
    return name[:len(name)-len(filepath.Ext(name))]
}

func renameLegacyWithTimestamp(legacyPath string) error {
    target := fmt.Sprintf("%s.migrated.%s", legacyPath, time.Now().UTC().Format("20060102-150405"))
    if err := os.Rename(legacyPath, target); err != nil {
        return err
    }
    // .last 备份文件如果存在，一并改名（不阻断主流程）
    lastFile := legacyPath + ".last"
    if _, err := os.Stat(lastFile); err == nil {
        _ = os.Rename(lastFile, fmt.Sprintf("%s.migrated.%s", lastFile, time.Now().UTC().Format("20060102-150405")))
    }
    return nil
}
```

- [ ] **Step 5: 跑测试**

```bash
go test ./repository/sqlite/ -v
```

Expected: ALL PASS（fresh init、CRUD、tx、migration 都过）

- [ ] **Step 6: Commit**

```bash
git add repository/sqlite/sqlite.go repository/sqlite/migrate.go repository/sqlite/sqlite_test.go repository/sqlite/testdata/
git commit -m "feat(sqlite): one-shot legacy JSON migration on Init

migrateLegacyIfAny detects clusters.json/yaml, runs MigrateBetween into
SQLite within a single tx, writes _meta.migrated_from, then renames
the legacy file with .migrated.<ts> suffix. Failures bubble up and
leave state untouched so retry is safe."
```

---

## Task 19: 切换 main.go 注册 + termHandler 加快照

**Files:**
- Modify: `main.go`

- [ ] **Step 1: 替换 `main.go:23` import**

```go
_ "github.com/housepower/ckman/repository/local"
```

替换为：

```go
_ "github.com/housepower/ckman/repository/sqlite"
```

- [ ] **Step 2: 在 `main.go` 末尾或合适位置加 shutdown 快照函数**

修改 `termHandler` 函数（约 main.go:170），在原有 ClickHouse 连接关闭逻辑后加一行调用快照：

```go
func termHandler() error {
    var hosts []string
    common.ConnectPool.Range(func(k, v interface{}) bool {
        hosts = append(hosts, k.(string))
        return true
    })
    common.CloseConns(hosts)

    if err := writeShutdownSnapshot(); err != nil {
        // 仅记 Warn，不影响关停
        log.Logger.Warnf("shutdown snapshot failed: %v", err)
    }
    return nil
}

func writeShutdownSnapshot() error {
    // 仅当当前 backend 是 sqlite 时才有意义。
    if config.GlobalConfig.Server.PersistentPolicy != "local" {
        return nil
    }
    cfg, ok := config.GlobalConfig.PersistentConfig["local"]
    if !ok {
        cfg = map[string]interface{}{}
    }

    locCfg := sqlite.LocalConfig{}
    if format, ok := cfg["format"].(string); ok {
        locCfg.Format = format
    }
    if dir, ok := cfg["config_dir"].(string); ok {
        locCfg.ConfigDir = dir
    }
    if name, ok := cfg["config_file"].(string); ok {
        locCfg.ConfigFile = name
    }
    locCfg.Normalize()

    ts := time.Now().UTC().Format("20060102-150405")
    outPath := filepath.Join(locCfg.ConfigDir, fmt.Sprintf("%s.json.shutdown_snapshot.%s", locCfg.ConfigFile, ts))

    return dumpjson.DumpFromDB(locCfg.DBPath(), outPath)
}
```

`main.go` import 加：

```go
"path/filepath"
"github.com/housepower/ckman/cmd/dumpjson"
"github.com/housepower/ckman/repository/sqlite"
```

注意：`dumpjson.DumpFromDB` 是 Task 20 的产出。如果 Task 20 在前先做，这里直接调用；如果按本计划顺序（Task 19 → 20），暂时把 `writeShutdownSnapshot` 实现为返回 nil 的占位，等 Task 20 完成后回填。

**保守做法：** 把 Task 19 拆成 19a / 19b，19a 只换 import，19b 加 snapshot。先按 19a 提交，启动验证后再做 Task 20，再回头做 19b。

- [ ] **Step 3: 19a 验证整包能编译运行**

```bash
go build ./...
```

Expected: success（所有原本 broken 的 import 都修好了）

- [ ] **Step 4: 在干净目录手工冒烟（重要）**

```bash
mkdir -p /tmp/ckman-smoke/conf
cp conf/ckman.hjson.sample /tmp/ckman-smoke/conf/ckman.hjson  # 或类似存在的 sample
cd /tmp/ckman-smoke && /path/to/ckman -c conf/ckman.hjson &
sleep 3
ls -la conf/  # 看到 clusters.db 应该已创建
sqlite3 conf/clusters.db ".schema" | head -20  # 八张表 + tbl_meta
kill %1
```

Expected: ckman 正常启动，`clusters.db` 自动创建，schema 含八张表。

- [ ] **Step 5: 19a Commit**

```bash
git add main.go
git commit -m "feat(main): switch local backend registration to repository/sqlite

The local backend now stores data in a SQLite file (clusters.db) instead
of JSON. Existing JSON files are auto-migrated on first startup; see
docs/superpowers/specs/2026-05-16-local-sqlite-persistent-design.md."
```

---

## Task 20: `ckmanctl dump-to-json` 子命令

**Files:**
- Create: `cmd/dumpjson/dumpjson.go`
- Modify: `cmd/ckmanctl/ckmanctl.go`
- Test: `cmd/dumpjson/dumpjson_test.go`

- [ ] **Step 1: 写测试 `cmd/dumpjson/dumpjson_test.go`**

```go
package dumpjson

import (
    "encoding/json"
    "os"
    "path/filepath"
    "testing"

    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/model"
    "github.com/housepower/ckman/repository/sqlite"
)

func TestMain(m *testing.M) {
    log.InitLoggerConsole()
    os.Exit(m.Run())
}

func TestDumpFromDB_RoundTrip(t *testing.T) {
    dir := t.TempDir()
    sp := sqlite.NewSQLitePersistent()
    if err := sp.Init(sqlite.LocalConfig{ConfigDir: dir, ConfigFile: "clusters"}); err != nil {
        t.Fatalf("init: %v", err)
    }
    if err := sp.CreateCluster(model.CKManClickHouseConfig{Cluster: "ck1", Comment: "x"}); err != nil {
        t.Fatalf("create: %v", err)
    }

    dbPath := filepath.Join(dir, "clusters.db")
    outPath := filepath.Join(dir, "dump.json")
    if err := DumpFromDB(dbPath, outPath); err != nil {
        t.Fatalf("dump: %v", err)
    }

    raw, err := os.ReadFile(outPath)
    if err != nil {
        t.Fatalf("read dump: %v", err)
    }
    var data map[string]interface{}
    if err := json.Unmarshal(raw, &data); err != nil {
        t.Fatalf("parse dump: %v", err)
    }
    clusters, ok := data["Clusters"].(map[string]interface{})
    if !ok || clusters["ck1"] == nil {
        t.Fatalf("dump missing ck1: %+v", data)
    }
}

func TestDumpFromDB_OfflineMode(t *testing.T) {
    // ckman 进程没启动也能跑 —— 跟 RoundTrip 一样直接打开文件即可。
    // 这里仅验证不通过 PersistentRegistry 注入也能完成 dump。
    TestDumpFromDB_RoundTrip(t)
}
```

- [ ] **Step 2: 创建 `cmd/dumpjson/dumpjson.go`**

```go
package dumpjson

import (
    "fmt"
    "path/filepath"

    sqlitedriver "github.com/glebarez/sqlite"
    "github.com/housepower/ckman/cmd/migrate"
    "github.com/housepower/ckman/log"
    "github.com/housepower/ckman/repository/legacyjson"
    "github.com/housepower/ckman/repository/sqlite"
    "github.com/pkg/errors"
    "gorm.io/gorm"
)

// DumpFromDB 把 dbPath 指向的 SQLite 数据库内容导出为 legacy JSON 格式写到 outPath。
// 不依赖 ckman 服务运行；ckman 进程在跑时也能调用（只读 + DEFERRED tx 拿一致快照）。
func DumpFromDB(dbPath, outPath string) error {
    // 1) 只读打开 SQLite
    dsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", dbPath)
    db, err := gorm.Open(sqlitedriver.Open(dsn), &gorm.Config{})
    if err != nil {
        return errors.Wrap(err, "open db ro")
    }

    // 2) 把 SQLitePersistent 实例的内部句柄替换为这个只读 db
    sp := sqlite.NewSQLitePersistent()
    sp.AttachReadOnly(db) // 见下面 helper

    // 3) 一致快照
    if err := sp.Begin(); err != nil {
        return errors.Wrap(err, "begin ro tx")
    }
    defer func() { _ = sp.Rollback() }()

    // 4) 构造一个空 legacyjson 写目标
    dst, err := legacyjson.NewWriter(legacyjson.LocalConfig{
        Format:     "json",
        ConfigDir:  filepath.Dir(outPath),
        ConfigFile: trimExtBase(outPath),
    })
    if err != nil {
        return errors.Wrap(err, "open legacy writer")
    }

    // 5) 复用 MigrateBetween
    if err := migrate.MigrateBetween(sp, dst); err != nil {
        return errors.Wrap(err, "dump")
    }

    log.Logger.Infof("dumped %s -> %s", dbPath, outPath)
    return nil
}

func trimExtBase(p string) string {
    base := filepath.Base(p)
    ext := filepath.Ext(base)
    return base[:len(base)-len(ext)]
}
```

注意：`SQLitePersistent.AttachReadOnly(db)` 是新增方法，方便 dump 工具用现成 CRUD 方法。在 `repository/sqlite/sqlite.go` 加：

```go
// AttachReadOnly 把外部传入的只读 gorm.DB 绑定到该实例。
// 仅用于 dump-to-json 工具，不应被业务路径调用。
func (sp *SQLitePersistent) AttachReadOnly(db *gorm.DB) {
    sp.Client = db
    sp.ParentDB = db
}
```

- [ ] **Step 3: 跑测试**

```bash
go test ./cmd/dumpjson/ -v
```

Expected: PASS

- [ ] **Step 4: 在 `cmd/ckmanctl/ckmanctl.go` 加子命令**

在已有 `migrateCmd` 等定义附近，加：

```go
dumpCmd  = kingpin.Command("dump-to-json", "export local SQLite db as legacy clusters.json")
d_conf   = dumpCmd.Flag("conf", "ckman config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()
d_output = dumpCmd.Flag("output", "output JSON file").Short('o').Default("conf/clusters.json").String()
```

在 `main()` 的 `switch firstCmd` 加 case：

```go
case "dump-to-json":
    runDumpToJSON(*d_conf, *d_output)
```

并在文件末尾加：

```go
func runDumpToJSON(confPath, outPath string) {
    if err := config.ParseConfigFile(confPath, ""); err != nil {
        fmt.Printf("parse config %s failed: %v\n", confPath, err)
        return
    }
    locCfg := sqlite.LocalConfig{}
    if c, ok := config.GlobalConfig.PersistentConfig["local"]; ok {
        if fmtv, ok := c["format"].(string); ok {
            locCfg.Format = fmtv
        }
        if dir, ok := c["config_dir"].(string); ok {
            locCfg.ConfigDir = dir
        }
        if name, ok := c["config_file"].(string); ok {
            locCfg.ConfigFile = name
        }
    }
    locCfg.Normalize()

    if err := dumpjson.DumpFromDB(locCfg.DBPath(), outPath); err != nil {
        fmt.Printf("dump failed: %v\n", err)
        return
    }
    fmt.Printf("Dumped %s -> %s\n", locCfg.DBPath(), outPath)
}
```

import 加：

```go
"github.com/housepower/ckman/cmd/dumpjson"
"github.com/housepower/ckman/config"
"github.com/housepower/ckman/repository/sqlite"
```

- [ ] **Step 5: 编译验证**

```bash
go build ./...
```

Expected: success

- [ ] **Step 6: 端到端冒烟**

```bash
# 在 Task 19 冒烟的 /tmp/ckman-smoke 目录基础上：
/path/to/ckmanctl dump-to-json -c conf/ckman.hjson -o /tmp/dump.json
cat /tmp/dump.json | head
```

Expected: 输出 legacy JSON 格式，包含 Clusters / Logics / Task 等字段。

- [ ] **Step 7: Commit**

```bash
git add cmd/dumpjson/ cmd/ckmanctl/ckmanctl.go repository/sqlite/sqlite.go
git commit -m "feat(ckmanctl): add dump-to-json subcommand for downgrade

Reads clusters.db read-only with DEFERRED transaction (consistent snapshot,
no impact on running ckman), routes through MigrateBetween into legacyjson
writer. Output is byte-compatible with the old clusters.json format."
```

---

## Task 19b: 启用 shutdown 快照

**Files:**
- Modify: `main.go`（回填 `writeShutdownSnapshot`，已在 Task 19 Step 2 给出代码）

- [ ] **Step 1: 把 Task 19 Step 2 中给出的 `writeShutdownSnapshot` 完整实现取消注释 / 填回 main.go**

完整代码已经在 Task 19 Step 2 给出，此处直接落地。

- [ ] **Step 2: 编译**

```bash
go build ./...
```

Expected: success

- [ ] **Step 3: 冒烟**

```bash
cd /tmp/ckman-smoke
/path/to/ckman -c conf/ckman.hjson &
sleep 3
kill %1
sleep 2
ls conf/clusters.json.shutdown_snapshot.*
```

Expected: 看到一个时间戳后缀的快照文件。

- [ ] **Step 4: Commit**

```bash
git add main.go
git commit -m "feat(main): write shutdown snapshot on graceful termination

On SIGTERM/SIGINT/SIGHUP, after closing CK connections, dump SQLite
content to <ConfigDir>/<ConfigFile>.json.shutdown_snapshot.<ts>.
Failures are logged but don't block shutdown."
```

---

## Task 21: 端到端验收（手工）

**Reference:** spec §10.4

- [ ] **Step 1: `make build && make test` 全绿**

```bash
make build
make test
```

Expected: 全部通过

- [ ] **Step 2: 真实 JSON → SQLite 迁移**

准备一份脱敏的现网 `clusters.json` 放进测试目录的 `conf/` 下，启动 ckman。

```bash
mkdir -p /tmp/ckman-real/conf
cp ~/clusters.json.sanitized /tmp/ckman-real/conf/clusters.json
cp conf/ckman.hjson.sample /tmp/ckman-real/conf/ckman.hjson
/path/to/ckman -c /tmp/ckman-real/conf/ckman.hjson 2>&1 | tee /tmp/ckman-real.log
```

Expected: log 含一行 `local persistent: migrated from .../clusters.json -> .../clusters.db`

- [ ] **Step 3: UI 验证**

打开浏览器登录 ckman → 检查集群列表、备份策略、任务历史、备份执行记录是否齐全。

- [ ] **Step 4: 幂等性**

```bash
kill ckman pid
/path/to/ckman -c /tmp/ckman-real/conf/ckman.hjson 2>&1 | grep -c "migrated from"
```

Expected: 0（第二次启动不再迁移）

- [ ] **Step 5: schema 检查**

```bash
sqlite3 /tmp/ckman-real/conf/clusters.db ".schema" | grep -E "CREATE TABLE"
```

Expected: 八张 `CREATE TABLE` 行（tbl_cluster, tbl_logic, tbl_query_history, tbl_task, tbl_backup, tbl_backup_policy, tbl_backup_run, tbl_meta）

- [ ] **Step 6: 跨后端迁移仍然工作**

准备一份 mysql 配置的 migrate.hjson：

```hjson
source: local
target: mysql
persistent_config: {
  local: { config_dir: "/tmp/ckman-real/conf", config_file: "clusters" }
  mysql: { host: 127.0.0.1, port: 3306, user: root, password: "...", database: ckman_test }
}
```

```bash
/path/to/ckmanctl migrate -f /tmp/migrate.hjson
mysql -e "USE ckman_test; SELECT COUNT(*) FROM tbl_backup_policy;"
mysql -e "USE ckman_test; SELECT COUNT(*) FROM tbl_backup_run;"
```

Expected: 计数 > 0（验证 MigrateBetween 修复了 BackupPolicy/Run 漏迁）

- [ ] **Step 7: 降级路径**

```bash
/path/to/ckmanctl dump-to-json -c /tmp/ckman-real/conf/ckman.hjson -o /tmp/dump.json
mv /tmp/ckman-real/conf/clusters.db /tmp/ckman-real/conf/clusters.db.bak
cp /tmp/dump.json /tmp/ckman-real/conf/clusters.json
/path/to/ckman-OLD-binary -c /tmp/ckman-real/conf/ckman.hjson &
# 登录 UI 验证数据
```

Expected: 旧版二进制能起来，UI 数据齐全。

- [ ] **Step 8: Shutdown snapshot**

```bash
systemctl stop ckman   # 或 kill PID
ls /tmp/ckman-real/conf/clusters.json.shutdown_snapshot.*
```

Expected: 看到一个时间戳后缀的快照文件。

- [ ] **Step 9: 全部通过后打 tag 或合 PR**

视项目流程；本计划完成。

---

## Self-Review Checklist

(由 writing-plans skill 强制要求；落盘前自查)

**Spec coverage:**
- §2 范围内全部条目 → Task 1-3（接口扩展）/ 4（MigrateBetween）/ 5-7（rename）/ 8-17（sqlite 实现）/ 18（迁移）/ 19+19b（main 切换 + 快照）/ 20（dump-to-json）✓
- §10 测试用例 → 散落在 Task 12-18 的测试步骤 + Task 21 手工验收 ✓
- §14 降级路径 → Task 20（dump-to-json）+ Task 19b（shutdown snapshot）+ Task 21 Step 7（端到端验证） ✓
- §15 并发与一致性 → DSN pragma 在 Task 11 写入；ro mode 在 Task 20 写入 ✓

**Placeholder scan:** 已逐节扫过，所有 step 都有具体代码或具体命令；唯一引用外部代码的地方都明确指出对应 `repository/mysql/mysql.go` 行号区间。

**Type consistency:**
- 结构体名 `SQLitePersistent` 在 Task 9-17 全程一致
- 配置类型 `LocalConfig` 与 `repository/legacyjson.LocalConfig` 同名但不同包（明确分别 import）
- `wrapError` 在 Task 11 定义、Task 12-17 共用 ✓
- `readMeta/writeMeta` 在 Task 11 定义、Task 18 复用 ✓
- `MigrateBetween(src, dst)` 签名 Task 4 定义、Task 18/20 调用 ✓

**Execution Handoff:**

Plan complete and saved to `docs/superpowers/plans/2026-05-16-local-sqlite-persistent.md`. Two execution options:

1. **Subagent-Driven (recommended)** — 每个 Task 派发新 subagent + checkpoint review，迭代快。
2. **Inline Execution** — 当前 session 顺序跑，批量执行 + checkpoint。

Which approach?
