# ckmanctl sql CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `ckmanctl sql` —— 一个根据 ckman.hjson 的 `persistent_policy` 自动连接对应后端（SQLite/MySQL/Postgres/DM8）、提供 SQL REPL + 单次执行模式、支持上下方向键翻历史、ASCII 表格/JSON/CSV 多种输出的诊断终端。

**Architecture:** 新增 `cmd/sqlcli` 包（REPL + 单次执行 + 三种 meta 翻译 + 四种输出渲染）。四个后端包各 export `BuildDialector(cfgMap) (gorm.Dialector, error)` 把 DSN/驱动构造抽出来复用。`chzyer/readline` 从间接依赖提升为直接依赖以获得方向键和历史持久化。

**Tech Stack:** Go 1.24, GORM v1.23.8, `github.com/chzyer/readline`, `text/tabwriter`(stdlib), `encoding/json`(stdlib), `encoding/csv`(stdlib)，glebarez/sqlite + gorm 各驱动（已在依赖中）。

**Spec:** `docs/superpowers/specs/2026-05-16-ckmanctl-sql-cli-design.md`

---

## 文件清单

**新增：**
- `cmd/sqlcli/sqlcli.go` — `Run(opts)` 入口；REPL 循环 + 单次执行 + 分发 read/write
- `cmd/sqlcli/connect.go` — 按 `persistent_policy` 选 Dialector 并 `gorm.Open`
- `cmd/sqlcli/translate.go` — `SHOW DATABASES` / `SHOW TABLES` / `DESC <tbl>` 三条翻译
- `cmd/sqlcli/render.go` — table / vertical / json / csv 四种 Renderer
- `cmd/sqlcli/sqlcli_test.go` — translate / render / dispatch 单测 + 端到端 SQLite 测试

**修改（DSN 抽取，小重构）：**
- `repository/sqlite/config.go` — 加 `BuildDialector(cfgMap) (gorm.Dialector, error)`
- `repository/sqlite/sqlite.go` — `Init()` 用 `BuildDialector` 替代内联 DSN
- `repository/mysql/config.go` — 同上
- `repository/mysql/mysql.go` — 同上
- `repository/postgres/config.go` — 同上
- `repository/postgres/postgres.go` — 同上
- `repository/dm8/config.go` — 同上
- `repository/dm8/dm8.go` — 同上
- `cmd/ckmanctl/ckmanctl.go` — 注册 `sql` 子命令
- `go.mod` / `go.sum` — `chzyer/readline` 提升为直接依赖

---

## 任务依赖图

```
Task 1 (chzyer/readline 直接依赖) ──┐
                                    │
Task 2 (4 backend BuildDialector) ──┴── Task 3 (sqlcli/connect.go) ──┐
                                                                      │
Task 4 (sqlcli/translate.go + tests) ─────────────────────────────────┤
                                                                      │
Task 5 (sqlcli/render.go + tests) ────────────────────────────────────┤
                                                                      │
                                                  Task 6 (sqlcli/sqlcli.go: Run + dispatch + REPL)
                                                                      │
                                                  Task 7 (ckmanctl 注册子命令)
                                                                      │
                                                  Task 8 (人工验收)
```

每个 Task 结束应当 `go build ./...` + 涉及的包 `go test` 全绿，然后提交。

---

## Task 1: 添加 `chzyer/readline` 直接依赖

**Files:** `go.mod`、`go.sum`

- [ ] **Step 1: go get**

```bash
cd /data/root/go/src/github.com/housepower/ckman
go get github.com/chzyer/readline@latest
go mod tidy
```

- [ ] **Step 2: 验证编译**

```bash
go build ./...
```

Expected: 全包编译通过（这一步还没引入用法，只是加 dep）。

- [ ] **Step 3: 确认 chzyer/readline 出现在 go.mod 的 require block 顶部（非 indirect）**

```bash
grep -n "chzyer/readline" go.mod
```

Expected: 一行像 `github.com/chzyer/readline v1.5.1`（version 可能不同，不带 `// indirect`）。

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "$(cat <<'EOF'
build: promote chzyer/readline to direct dependency

For the upcoming ckmanctl sql REPL: provides arrow-key history
navigation, Ctrl-R reverse search, and a persisted history file.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: 四个后端 export `BuildDialector`

**Files:**
- Modify: `repository/sqlite/config.go`、`repository/sqlite/sqlite.go`
- Modify: `repository/mysql/config.go`、`repository/mysql/mysql.go`
- Modify: `repository/postgres/config.go`、`repository/postgres/postgres.go`
- Modify: `repository/dm8/config.go`、`repository/dm8/dm8.go`

**Rationale:** 把 DSN/驱动构造从各 backend 的 `Init()` 抽到 config.go 的公开函数。`Init()` 改为调用它。sqlcli 之后会直接调用 `BuildDialector` 而无需触发 `Init()` 里的 AutoMigrate / migrateLegacyIfAny。

### Step 1: 修改 `repository/sqlite/config.go`

在文件末尾追加：

```go
// BuildDialector 把 ckman.hjson 中 persistent_config.local 节点转换为 GORM Dialector。
// 用法：sqlcli 等工具想拿原始 *gorm.DB 而不触发 Init() 的迁移路径时，调此函数 + gorm.Open。
// 入参 cfgMap 取自 config.GlobalConfig.PersistentConfig["local"]。
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
    cfg := LocalConfig{}
    data, err := json.Marshal(cfgMap)
    if err != nil {
        return nil, errors.Wrap(err, "marshal cfgMap")
    }
    if err := json.Unmarshal(data, &cfg); err != nil {
        return nil, errors.Wrap(err, "unmarshal cfgMap")
    }
    cfg.Normalize()
    dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", cfg.DBPath())
    return sqlitedriver.Open(dsn), nil
}
```

文件顶部 import 区追加：

```go
import (
    "encoding/json"
    // ...existing imports...
    sqlitedriver "github.com/glebarez/sqlite"
    "github.com/pkg/errors"
    "gorm.io/gorm"
)
```

注意：如果该文件已有部分 import，把上述三个补进现有 import 块；用 `goimports` 思路保持排序。

### Step 2: 修改 `repository/sqlite/sqlite.go` 的 `Init()`

当前 `Init()` 直接拼 DSN。改为调 `BuildDialector`。原代码（约 sqlite.go:50-58）：

```go
dsn := fmt.Sprintf("file:%s?...", sp.Config.DBPath())
logger := zapgorm2.New(log.ZapLog)
logger.SetAsDefault()
db, err := gorm.Open(sqlitedriver.Open(dsn), &gorm.Config{Logger: logger})
```

替换为：

```go
// Re-use BuildDialector so the DSN/pragma set is defined in exactly one place.
cfgMap := map[string]interface{}{
    "format":      sp.Config.Format,
    "config_dir":  sp.Config.ConfigDir,
    "config_file": sp.Config.ConfigFile,
}
dialector, err := BuildDialector(cfgMap)
if err != nil {
    return errors.Wrap(err, "")
}
logger := zapgorm2.New(log.ZapLog)
logger.SetAsDefault()
db, err := gorm.Open(dialector, &gorm.Config{Logger: logger})
```

注意保留原 logger 设置和 AutoMigrate 等后续逻辑。

### Step 3: 验证 sqlite 测试还过

```bash
go test ./repository/sqlite/ -v
```

Expected: 全过（13 个测试，包括 FreshInit、CRUD、迁移、恢复等）。

### Step 4: 修改 `repository/mysql/config.go`，在文件末尾追加

```go
// BuildDialector 把 ckman.hjson 中 persistent_config.mysql 节点转换为 GORM Dialector。
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
    cfg := MysqlConfig{}
    data, err := json.Marshal(cfgMap)
    if err != nil {
        return nil, errors.Wrap(err, "marshal cfgMap")
    }
    if err := json.Unmarshal(data, &cfg); err != nil {
        return nil, errors.Wrap(err, "unmarshal cfgMap")
    }
    cfg.Normalize()
    dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
        cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DataBase)
    return driver.Open(dsn), nil
}
```

文件顶部 import 区追加：

```go
"encoding/json"
"fmt"
driver "gorm.io/driver/mysql"
"github.com/pkg/errors"
"gorm.io/gorm"
```

### Step 5: 修改 `repository/mysql/mysql.go` 的 `Init()`

原代码（mysql.go:31-43）：

```go
dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
    mp.Config.User, mp.Config.Password, mp.Config.Host, mp.Config.Port, mp.Config.DataBase)
log.Logger.Debugf("mysql dsn:%s", dsn)
logger := zapgorm2.New(log.ZapLog)
logger.SetAsDefault()
db, err := gorm.Open(driver.Open(dsn), &gorm.Config{Logger: logger})
```

替换为：

```go
cfgMap := map[string]interface{}{
    "host":     mp.Config.Host,
    "port":     mp.Config.Port,
    "user":     mp.Config.User,
    "password": mp.Config.Password,
    "database": mp.Config.DataBase,
}
dialector, err := BuildDialector(cfgMap)
if err != nil {
    return errors.Wrap(err, "")
}
logger := zapgorm2.New(log.ZapLog)
logger.SetAsDefault()
db, err := gorm.Open(dialector, &gorm.Config{Logger: logger})
```

### Step 6: 修改 `repository/postgres/config.go` 类似 mysql

在文件末尾追加：

```go
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
    cfg := PostgresConfig{}
    data, err := json.Marshal(cfgMap)
    if err != nil {
        return nil, errors.Wrap(err, "marshal cfgMap")
    }
    if err := json.Unmarshal(data, &cfg); err != nil {
        return nil, errors.Wrap(err, "unmarshal cfgMap")
    }
    cfg.Normalize()
    dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
        cfg.Host, cfg.Port, cfg.User, cfg.DataBase, cfg.Password)
    return driver.Open(dsn), nil
}
```

import 追加：

```go
"encoding/json"
"fmt"
driver "gorm.io/driver/postgres"
"github.com/pkg/errors"
"gorm.io/gorm"
```

### Step 7: 修改 `repository/postgres/postgres.go` 的 `Init()`

原代码（postgres.go:31 起）：

```go
dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
    pp.Config.Host, pp.Config.Port, pp.Config.User, pp.Config.DataBase, pp.Config.Password)
```

类似 mysql 的替换：构造 cfgMap，调 BuildDialector，gorm.Open。

```go
cfgMap := map[string]interface{}{
    "host":     pp.Config.Host,
    "port":     pp.Config.Port,
    "user":     pp.Config.User,
    "password": pp.Config.Password,
    "database": pp.Config.DataBase,
}
dialector, err := BuildDialector(cfgMap)
if err != nil {
    return errors.Wrap(err, "")
}
logger := zapgorm2.New(log.ZapLog)
logger.SetAsDefault()
db, err := gorm.Open(dialector, &gorm.Config{Logger: logger})
```

### Step 8: 修改 `repository/dm8/config.go`

```go
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
    cfg := DM8Config{}
    data, err := json.Marshal(cfgMap)
    if err != nil {
        return nil, errors.Wrap(err, "marshal cfgMap")
    }
    if err := json.Unmarshal(data, &cfg); err != nil {
        return nil, errors.Wrap(err, "unmarshal cfgMap")
    }
    cfg.Normalize()
    dsn := fmt.Sprintf("dm://%s:%s@%s:%d?autoCommit=true",
        cfg.User, cfg.Password, cfg.Host, cfg.Port)
    return driver.Open(dsn), nil
}
```

import：

```go
"encoding/json"
"fmt"
driver "github.com/wanlay/gorm-dm8"
"github.com/pkg/errors"
"gorm.io/gorm"
```

注意：dm8 的 config 结构体可能叫 `DM8Config` 或 `Dm8Config` —— 先读 `repository/dm8/config.go` 看实际名字再写。

### Step 9: 修改 `repository/dm8/dm8.go` 的 `Init()`

原代码（dm8.go:32 起）：

```go
dsn := fmt.Sprintf("dm://%s:%s@%s:%d?autoCommit=true",
    dp.Config.User, dp.Config.Password, dp.Config.Host, dp.Config.Port)
```

替换为调 BuildDialector，参考 mysql 替换的样式：

```go
cfgMap := map[string]interface{}{
    "host":     dp.Config.Host,
    "port":     dp.Config.Port,
    "user":     dp.Config.User,
    "password": dp.Config.Password,
}
dialector, err := BuildDialector(cfgMap)
if err != nil {
    return errors.Wrap(err, "")
}
logger := zapgorm2.New(log.ZapLog)
logger.SetAsDefault()
db, err := gorm.Open(dialector, &gorm.Config{Logger: logger})
```

### Step 10: 编译 + 测试

```bash
go build ./...
go test ./repository/...
```

Expected: 全过（mysql / postgres / dm8 没有单元测试，但 sqlite 和 legacyjson 应该过）。

### Step 11: Commit

```bash
git add repository/sqlite/config.go repository/sqlite/sqlite.go \
    repository/mysql/config.go repository/mysql/mysql.go \
    repository/postgres/config.go repository/postgres/postgres.go \
    repository/dm8/config.go repository/dm8/dm8.go
git commit -m "$(cat <<'EOF'
refactor(repository): expose BuildDialector for each persistent backend

Each backend's Init() previously inlined the DSN construction. Extract
it into a public BuildDialector(cfgMap) so external tools (e.g. the
upcoming ckmanctl sql) can obtain a *gorm.DB without triggering Init's
AutoMigrate / migration side effects.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: `cmd/sqlcli/connect.go` —— 后端 dispatch + 连接

**Files:**
- Create: `cmd/sqlcli/connect.go`
- Test: `cmd/sqlcli/sqlcli_test.go`（先建空文件 + TestMain）

### Step 1: 创建 `cmd/sqlcli/sqlcli_test.go`（空骨架）

```go
package sqlcli

import (
    "os"
    "testing"

    "github.com/housepower/ckman/log"
)

func TestMain(m *testing.M) {
    log.InitLoggerConsole()
    os.Exit(m.Run())
}
```

### Step 2: 写测试 `TestOpenDB_SQLite`（追加到 sqlcli_test.go）

```go
func TestOpenDB_SQLite(t *testing.T) {
    dir := t.TempDir()
    cfgMap := map[string]interface{}{
        "config_dir":  dir,
        "config_file": "testdb",
    }
    db, backend, err := OpenDB("local", cfgMap)
    if err != nil {
        t.Fatalf("open: %v", err)
    }
    if backend != BackendSQLite {
        t.Fatalf("unexpected backend: %v", backend)
    }
    if db == nil {
        t.Fatalf("nil db")
    }
    sqlDB, _ := db.DB()
    _ = sqlDB.Close()
}

func TestOpenDB_UnsupportedPolicy(t *testing.T) {
    _, _, err := OpenDB("oracle", map[string]interface{}{})
    if err == nil {
        t.Fatalf("expected error for unsupported policy")
    }
}
```

### Step 3: 跑测试看红

```bash
go test ./cmd/sqlcli/ -run TestOpenDB -v
```

Expected: 编译失败（`OpenDB` 未定义）。

### Step 4: 创建 `cmd/sqlcli/connect.go`

```go
package sqlcli

import (
    "github.com/housepower/ckman/repository/dm8"
    "github.com/housepower/ckman/repository/mysql"
    "github.com/housepower/ckman/repository/postgres"
    "github.com/housepower/ckman/repository/sqlite"
    "github.com/pkg/errors"
    "gorm.io/gorm"
)

// Backend 标识当前 sqlcli 连接到了哪个后端。
// 用于决定 SHOW DATABASES / SHOW TABLES / DESC 翻译到哪种原生 SQL。
type Backend int

const (
    BackendUnknown Backend = iota
    BackendSQLite
    BackendMySQL
    BackendPostgres
    BackendDM8
)

func (b Backend) String() string {
    switch b {
    case BackendSQLite:
        return "sqlite"
    case BackendMySQL:
        return "mysql"
    case BackendPostgres:
        return "postgres"
    case BackendDM8:
        return "dm8"
    default:
        return "unknown"
    }
}

// OpenDB 按 ckman 的 persistent_policy 选驱动 + 打开数据库。
// cfgMap 取自 config.GlobalConfig.PersistentConfig[policy]。
// 调用方负责关闭 sqlDB（通过 db.DB() + Close）。
func OpenDB(policy string, cfgMap map[string]interface{}) (*gorm.DB, Backend, error) {
    var (
        dialector gorm.Dialector
        backend   Backend
        err       error
    )
    switch policy {
    case "local":
        dialector, err = sqlite.BuildDialector(cfgMap)
        backend = BackendSQLite
    case "mysql":
        dialector, err = mysql.BuildDialector(cfgMap)
        backend = BackendMySQL
    case "postgres":
        dialector, err = postgres.BuildDialector(cfgMap)
        backend = BackendPostgres
    case "dm8":
        dialector, err = dm8.BuildDialector(cfgMap)
        backend = BackendDM8
    default:
        return nil, BackendUnknown, errors.Errorf("unsupported persistent_policy: %s", policy)
    }
    if err != nil {
        return nil, BackendUnknown, errors.Wrap(err, "build dialector")
    }
    db, err := gorm.Open(dialector, &gorm.Config{})
    if err != nil {
        return nil, BackendUnknown, errors.Wrap(err, "open db")
    }
    return db, backend, nil
}
```

### Step 5: 跑测试看绿

```bash
go test ./cmd/sqlcli/ -run TestOpenDB -v
```

Expected: 两个测试 PASS。

### Step 6: Commit

```bash
git add cmd/sqlcli/connect.go cmd/sqlcli/sqlcli_test.go
git commit -m "$(cat <<'EOF'
feat(sqlcli): add backend dispatch + connection helper

OpenDB resolves persistent_policy → Dialector → *gorm.DB, returning
also a Backend enum that downstream code uses to pick the right
SHOW DATABASES / SHOW TABLES / DESC translation.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: `cmd/sqlcli/translate.go` —— meta 翻译

**Files:**
- Create: `cmd/sqlcli/translate.go`
- Modify: `cmd/sqlcli/sqlcli_test.go`（追加测试）

### Step 1: 写测试

追加到 sqlcli_test.go：

```go
func TestTranslate_ShowDatabases(t *testing.T) {
    cases := []struct {
        backend Backend
        want    string
    }{
        {BackendSQLite, "PRAGMA database_list"},
        {BackendMySQL, "SHOW DATABASES"},
        {BackendDM8, "SHOW DATABASES"},
        {BackendPostgres, "SELECT datname FROM pg_database WHERE datistemplate = false"},
    }
    for _, c := range cases {
        got := Translate("SHOW DATABASES", c.backend)
        if got != c.want {
            t.Errorf("%v: got %q, want %q", c.backend, got, c.want)
        }
    }
}

func TestTranslate_ShowTables(t *testing.T) {
    cases := []struct {
        backend Backend
        want    string
    }{
        {BackendSQLite, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"},
        {BackendMySQL, "SHOW TABLES"},
        {BackendDM8, "SHOW TABLES"},
        {BackendPostgres, "SELECT tablename FROM pg_tables WHERE schemaname='public'"},
    }
    for _, c := range cases {
        got := Translate("show tables", c.backend) // 大小写无关
        if got != c.want {
            t.Errorf("%v: got %q, want %q", c.backend, got, c.want)
        }
    }
}

func TestTranslate_Desc(t *testing.T) {
    cases := []struct {
        input   string
        backend Backend
        want    string
    }{
        {"DESC tbl_cluster", BackendSQLite, "PRAGMA table_info(tbl_cluster)"},
        {"DESCRIBE tbl_cluster", BackendSQLite, "PRAGMA table_info(tbl_cluster)"},
        {"DESC tbl_cluster", BackendMySQL, "DESC tbl_cluster"},
        {"DESC tbl_cluster", BackendPostgres,
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'tbl_cluster'"},
    }
    for _, c := range cases {
        got := Translate(c.input, c.backend)
        if got != c.want {
            t.Errorf("%s on %v: got %q, want %q", c.input, c.backend, got, c.want)
        }
    }
}

func TestTranslate_PassThrough(t *testing.T) {
    in := "SELECT * FROM tbl_cluster WHERE cluster_name = 'ck1'"
    if got := Translate(in, BackendSQLite); got != in {
        t.Errorf("expected pass-through, got %q", got)
    }
}
```

### Step 2: 跑测试看红

```bash
go test ./cmd/sqlcli/ -run TestTranslate -v
```

Expected: `Translate` 未定义。

### Step 3: 创建 `cmd/sqlcli/translate.go`

```go
package sqlcli

import (
    "strings"
)

// Translate 检查输入是否匹配 SHOW DATABASES / SHOW TABLES / DESC <tbl> 三条 meta；
// 命中则替换为对应后端的原生 SQL，否则原样返回。
//
// 输入约定：调用方已经 trim 过空白和末尾分号。
func Translate(sql string, backend Backend) string {
    upper := strings.ToUpper(strings.TrimSpace(sql))
    switch {
    case upper == "SHOW DATABASES":
        return translateShowDatabases(backend)
    case upper == "SHOW TABLES":
        return translateShowTables(backend)
    case strings.HasPrefix(upper, "DESC "), strings.HasPrefix(upper, "DESCRIBE "):
        tbl := extractDescTarget(sql)
        if tbl == "" {
            return sql
        }
        return translateDesc(backend, tbl)
    }
    return sql
}

func translateShowDatabases(backend Backend) string {
    switch backend {
    case BackendSQLite:
        return "PRAGMA database_list"
    case BackendPostgres:
        return "SELECT datname FROM pg_database WHERE datistemplate = false"
    default: // MySQL / DM8 / Unknown
        return "SHOW DATABASES"
    }
}

func translateShowTables(backend Backend) string {
    switch backend {
    case BackendSQLite:
        return "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    case BackendPostgres:
        return "SELECT tablename FROM pg_tables WHERE schemaname='public'"
    default:
        return "SHOW TABLES"
    }
}

func translateDesc(backend Backend, tbl string) string {
    switch backend {
    case BackendSQLite:
        return "PRAGMA table_info(" + tbl + ")"
    case BackendPostgres:
        return "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '" + tbl + "'"
    default:
        return "DESC " + tbl
    }
}

// extractDescTarget 从 "DESC <tbl>" / "DESCRIBE <tbl>" 中提取表名。
// 简单实现：取关键字之后第一个 token，去掉两侧引号 / 反引号。
func extractDescTarget(sql string) string {
    sql = strings.TrimSpace(sql)
    fields := strings.Fields(sql)
    if len(fields) < 2 {
        return ""
    }
    tbl := fields[1]
    tbl = strings.Trim(tbl, "`\"'")
    return tbl
}
```

### Step 4: 跑测试看绿

```bash
go test ./cmd/sqlcli/ -run TestTranslate -v
```

Expected: 4 个测试 PASS。

### Step 5: Commit

```bash
git add cmd/sqlcli/translate.go cmd/sqlcli/sqlcli_test.go
git commit -m "$(cat <<'EOF'
feat(sqlcli): add meta translation for SHOW DATABASES/TABLES and DESC

Three meta rules give uniform behaviour across SQLite, MySQL, PostgreSQL,
and DM8 backends. Anything not matching a meta rule passes through raw.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: `cmd/sqlcli/render.go` —— 四种输出格式

**Files:**
- Create: `cmd/sqlcli/render.go`
- Modify: `cmd/sqlcli/sqlcli_test.go`（追加测试）

### Step 1: 写测试

追加到 sqlcli_test.go：

```go
import (
    "bytes"
    "strings"
    "time"
)

func sampleRows() ([]string, [][]interface{}) {
    cols := []string{"id", "name", "comment", "created_at"}
    rows := [][]interface{}{
        {int64(1), "ck1", "prod", time.Date(2026, 5, 16, 10, 30, 45, 0, time.UTC)},
        {int64(2), "ck2", nil, time.Date(2026, 5, 16, 11, 2, 11, 0, time.UTC)},
    }
    return cols, rows
}

func TestRender_Table_Basic(t *testing.T) {
    cols, rows := sampleRows()
    var buf bytes.Buffer
    RenderTable(&buf, cols, rows, RenderOptions{})
    out := buf.String()
    for _, want := range []string{"id", "name", "comment", "ck1", "ck2", "(NULL)"} {
        if !strings.Contains(out, want) {
            t.Errorf("table missing %q in:\n%s", want, out)
        }
    }
}

func TestRender_Table_Truncate(t *testing.T) {
    cols := []string{"v"}
    longStr := strings.Repeat("x", 100)
    rows := [][]interface{}{{longStr}}
    var buf bytes.Buffer
    RenderTable(&buf, cols, rows, RenderOptions{TruncateAt: 60})
    out := buf.String()
    if strings.Contains(out, longStr) {
        t.Errorf("expected truncation, got full string in:\n%s", out)
    }
    if !strings.Contains(out, "...") {
        t.Errorf("expected ellipsis marker")
    }
}

func TestRender_Vertical_Basic(t *testing.T) {
    cols, rows := sampleRows()
    var buf bytes.Buffer
    RenderVertical(&buf, cols, rows, RenderOptions{})
    out := buf.String()
    if !strings.Contains(out, "1. row") || !strings.Contains(out, "2. row") {
        t.Errorf("vertical missing row markers:\n%s", out)
    }
    if !strings.Contains(out, "id:") || !strings.Contains(out, "name:") {
        t.Errorf("vertical missing field labels")
    }
}

func TestRender_JSON_NullAndTime(t *testing.T) {
    cols, rows := sampleRows()
    var buf bytes.Buffer
    RenderJSON(&buf, cols, rows)
    out := buf.String()
    // NDJSON: 每行一个对象
    lines := strings.Split(strings.TrimSpace(out), "\n")
    if len(lines) != 2 {
        t.Fatalf("expected 2 NDJSON lines, got %d", len(lines))
    }
    if !strings.Contains(lines[1], `"comment":null`) {
        t.Errorf("NULL not rendered as JSON null: %s", lines[1])
    }
    if !strings.Contains(lines[0], `"created_at":"2026-05-16T10:30:45Z"`) {
        t.Errorf("time not RFC3339 in: %s", lines[0])
    }
}

func TestRender_CSV_Escaping(t *testing.T) {
    cols := []string{"a", "b"}
    rows := [][]interface{}{
        {"hello, world", `quote "inside"`},
        {"line1\nline2", nil},
    }
    var buf bytes.Buffer
    RenderCSV(&buf, cols, rows)
    out := buf.String()
    if !strings.Contains(out, `"hello, world"`) {
        t.Errorf("comma not quoted: %s", out)
    }
    if !strings.Contains(out, `"quote ""inside"""`) {
        t.Errorf("inner quote not escaped: %s", out)
    }
}
```

### Step 2: 跑测试看红

```bash
go test ./cmd/sqlcli/ -run TestRender -v
```

Expected: `RenderTable` 等未定义。

### Step 3: 创建 `cmd/sqlcli/render.go`

```go
package sqlcli

import (
    "encoding/csv"
    "encoding/json"
    "fmt"
    "io"
    "strings"
    "time"
)

// RenderOptions 控制 table / vertical 渲染的截断和列宽。
type RenderOptions struct {
    TruncateAt int // 0 表示不截断；> 0 表示超过 N 个字符的字符串截断为 N-3+...
}

const DefaultTruncate = 60

// RenderTable 按 mysql 客户端的 ASCII table 样式渲染。
func RenderTable(w io.Writer, cols []string, rows [][]interface{}, opts RenderOptions) {
    if len(rows) == 0 {
        return
    }
    // 计算每列最大宽度
    truncate := opts.TruncateAt
    widths := make([]int, len(cols))
    for i, c := range cols {
        widths[i] = len(c)
    }
    formatted := make([][]string, len(rows))
    for ri, row := range rows {
        formatted[ri] = make([]string, len(cols))
        for ci, v := range row {
            s := formatCell(v, truncate)
            formatted[ri][ci] = s
            if len(s) > widths[ci] {
                widths[ci] = len(s)
            }
        }
    }
    // 顶/中/底边线
    sep := buildSep(widths)
    fmt.Fprintln(w, sep)
    fmt.Fprintln(w, buildRow(cols, widths))
    fmt.Fprintln(w, sep)
    for _, row := range formatted {
        fmt.Fprintln(w, buildRow(row, widths))
    }
    fmt.Fprintln(w, sep)
}

func buildSep(widths []int) string {
    var b strings.Builder
    b.WriteString("+")
    for _, w := range widths {
        b.WriteString(strings.Repeat("-", w+2))
        b.WriteString("+")
    }
    return b.String()
}

func buildRow(cells []string, widths []int) string {
    var b strings.Builder
    b.WriteString("|")
    for i, c := range cells {
        b.WriteString(" ")
        b.WriteString(c)
        b.WriteString(strings.Repeat(" ", widths[i]-len(c)))
        b.WriteString(" |")
    }
    return b.String()
}

// RenderVertical 按 mysql `\G` 样式：一行一行展示。
func RenderVertical(w io.Writer, cols []string, rows [][]interface{}, opts RenderOptions) {
    truncate := opts.TruncateAt
    // 找最长字段名做对齐
    maxLabel := 0
    for _, c := range cols {
        if len(c) > maxLabel {
            maxLabel = len(c)
        }
    }
    for ri, row := range rows {
        fmt.Fprintf(w, "*************************** %d. row ***************************\n", ri+1)
        for ci, v := range row {
            fmt.Fprintf(w, "%*s: %s\n", maxLabel, cols[ci], formatCell(v, truncate))
        }
    }
}

// RenderJSON 输出 NDJSON：每行一个 JSON 对象。
// NULL → null；time.Time → RFC3339；[]byte → base64（json.Marshal 默认行为）。
func RenderJSON(w io.Writer, cols []string, rows [][]interface{}) {
    enc := json.NewEncoder(w)
    for _, row := range rows {
        obj := make(map[string]interface{}, len(cols))
        for ci, v := range row {
            if t, ok := v.(time.Time); ok {
                obj[cols[ci]] = t.UTC().Format(time.RFC3339)
            } else {
                obj[cols[ci]] = v
            }
        }
        _ = enc.Encode(obj)
    }
}

// RenderCSV 输出 RFC4180 CSV，第一行为表头。
// NULL → 空字符串；time.Time → RFC3339；其他 → fmt.Sprintf("%v")。
func RenderCSV(w io.Writer, cols []string, rows [][]interface{}) {
    cw := csv.NewWriter(w)
    _ = cw.Write(cols)
    for _, row := range rows {
        record := make([]string, len(row))
        for i, v := range row {
            switch x := v.(type) {
            case nil:
                record[i] = ""
            case time.Time:
                record[i] = x.UTC().Format(time.RFC3339)
            case []byte:
                record[i] = string(x)
            case string:
                record[i] = x
            default:
                record[i] = fmt.Sprintf("%v", x)
            }
        }
        _ = cw.Write(record)
    }
    cw.Flush()
}

// formatCell 把任意类型转字符串，含 NULL 渲染、时间格式、可选截断。
func formatCell(v interface{}, truncateAt int) string {
    var s string
    switch x := v.(type) {
    case nil:
        return "(NULL)"
    case time.Time:
        s = x.Format("2006-01-02 15:04:05")
    case []byte:
        s = string(x)
    case string:
        s = x
    default:
        s = fmt.Sprintf("%v", x)
    }
    if truncateAt > 0 && len(s) > truncateAt {
        return s[:truncateAt-3] + "..."
    }
    return s
}
```

### Step 4: 跑测试看绿

```bash
go test ./cmd/sqlcli/ -run TestRender -v
```

Expected: 5 个测试 PASS。

### Step 5: Commit

```bash
git add cmd/sqlcli/render.go cmd/sqlcli/sqlcli_test.go
git commit -m "$(cat <<'EOF'
feat(sqlcli): add table/vertical/json/csv renderers

Four renderers covering common operator needs:
- table: mysql-style ASCII for interactive REPL (default)
- vertical: mysql \G style for wide rows
- json: NDJSON for jq pipelines
- csv: RFC4180 for Excel/awk

formatCell handles NULL → (NULL), time.Time → "2006-01-02 15:04:05",
[]byte → UTF-8 string, optional truncation at N chars.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: `cmd/sqlcli/sqlcli.go` —— Run 入口 + REPL + 分发

**Files:**
- Create: `cmd/sqlcli/sqlcli.go`
- Modify: `cmd/sqlcli/sqlcli_test.go`（追加 dispatch / RunSingleShot 测试）

### Step 1: 写测试 dispatch 关键字识别

追加到 sqlcli_test.go：

```go
func TestIsReadStatement(t *testing.T) {
    cases := []struct {
        in   string
        want bool
    }{
        {"SELECT * FROM t", true},
        {"select 1", true},
        {"WITH cte AS (SELECT 1) SELECT * FROM cte", true},
        {"PRAGMA table_info(x)", true},
        {"EXPLAIN SELECT 1", true},
        {"SHOW TABLES", true},
        {"DESC tbl", true},
        {"DESCRIBE tbl", true},
        {"INSERT INTO t VALUES (1)", false},
        {"UPDATE t SET a=1", false},
        {"DELETE FROM t", false},
        {"CREATE TABLE t (a int)", false},
        {"DROP TABLE t", false},
        {"BEGIN", false},
        {"COMMIT", false},
    }
    for _, c := range cases {
        if got := isReadStatement(c.in); got != c.want {
            t.Errorf("isReadStatement(%q) = %v, want %v", c.in, got, c.want)
        }
    }
}
```

### Step 2: 写测试 single-shot 端到端（SQLite）

```go
func TestRunSingleShot_SQLite(t *testing.T) {
    dir := t.TempDir()
    // 在临时 db 里建一张表插点数据
    db, _, err := OpenDB("local", map[string]interface{}{
        "config_dir":  dir,
        "config_file": "testdb",
    })
    if err != nil {
        t.Fatalf("open: %v", err)
    }
    if err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").Error; err != nil {
        t.Fatalf("create: %v", err)
    }
    if err := db.Exec("INSERT INTO t VALUES (1, 'alice')").Error; err != nil {
        t.Fatalf("insert: %v", err)
    }
    sqlDB, _ := db.DB()
    _ = sqlDB.Close()

    // 跑 RunSingleShot
    var buf bytes.Buffer
    opts := Options{
        ConfMap: map[string]interface{}{
            "config_dir":  dir,
            "config_file": "testdb",
        },
        Policy: "local",
        Query:  "SELECT * FROM t",
        Format: "json",
        Out:    &buf,
        ErrOut: &buf,
    }
    if err := RunSingleShot(opts); err != nil {
        t.Fatalf("run: %v", err)
    }
    out := buf.String()
    if !strings.Contains(out, `"name":"alice"`) {
        t.Errorf("expected alice in output, got:\n%s", out)
    }
}
```

### Step 3: 跑测试看红

```bash
go test ./cmd/sqlcli/ -run "TestIsReadStatement|TestRunSingleShot" -v
```

Expected: 编译失败 `isReadStatement` / `RunSingleShot` 未定义。

### Step 4: 创建 `cmd/sqlcli/sqlcli.go`

```go
package sqlcli

import (
    "database/sql"
    "fmt"
    "io"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/chzyer/readline"
    "github.com/housepower/ckman/log"
    "github.com/pkg/errors"
    "gorm.io/gorm"
)

// Options 是 ckmanctl sql 子命令的所有运行时参数。
type Options struct {
    Policy      string                 // persistent_policy（local / mysql / postgres / dm8）
    ConfMap     map[string]interface{} // persistent_config[policy] 反序列化结果
    Query       string                 // 非空 ⇒ 单次模式
    Format      string                 // "table" / "json" / "csv"，默认 table
    Vertical    bool                   // -E
    NoTruncate  bool                   // -N
    Out         io.Writer              // 默认 os.Stdout
    ErrOut      io.Writer              // 默认 os.Stderr
    HistoryFile string                 // 默认 $HOME/.ckman_sql_history
}

func (o *Options) normalize() {
    if o.Out == nil {
        o.Out = os.Stdout
    }
    if o.ErrOut == nil {
        o.ErrOut = os.Stderr
    }
    if o.Format == "" {
        o.Format = "table"
    }
    if o.HistoryFile == "" {
        if home := os.Getenv("HOME"); home != "" {
            o.HistoryFile = filepath.Join(home, ".ckman_sql_history")
        }
    }
}

// Run 是 ckmanctl sql 子命令的统一入口。
// Query 非空时进入单次模式后退出；否则进 REPL。
func Run(opts Options) error {
    opts.normalize()
    if opts.Query != "" {
        return RunSingleShot(opts)
    }
    return RunREPL(opts)
}

// RunSingleShot 执行一条 SQL，输出后退出。错误返回 non-nil（ckmanctl main 转 exit code 1）。
func RunSingleShot(opts Options) error {
    opts.normalize()
    db, backend, err := OpenDB(opts.Policy, opts.ConfMap)
    if err != nil {
        return errors.Wrap(err, "open db")
    }
    defer closeGorm(db)
    return executeOne(opts, db, backend, opts.Query)
}

// RunREPL 启动交互式 prompt，每行一条 SQL。EOF / exit / quit 退出。
// 单条 SQL 错误不退出，让用户改后重试。
func RunREPL(opts Options) error {
    opts.normalize()
    db, backend, err := OpenDB(opts.Policy, opts.ConfMap)
    if err != nil {
        return errors.Wrap(err, "open db")
    }
    defer closeGorm(db)

    rl, err := readline.NewEx(&readline.Config{
        Prompt:          "ckman> ",
        HistoryFile:     opts.HistoryFile,
        InterruptPrompt: "^C",
        EOFPrompt:       "exit",
        HistoryLimit:    1000,
    })
    if err != nil {
        return errors.Wrap(err, "init readline")
    }
    defer rl.Close()

    fmt.Fprintf(opts.Out, "Connected to %s backend. Type 'exit' or Ctrl-D to quit.\n", backend)
    for {
        line, rerr := rl.Readline()
        if rerr == readline.ErrInterrupt {
            if line == "" {
                return nil // 第二次 Ctrl-C 退出（readline 默认行为）
            }
            continue
        }
        if rerr == io.EOF {
            return nil
        }
        if rerr != nil {
            return errors.Wrap(rerr, "readline")
        }
        line = strings.TrimSpace(line)
        line = strings.TrimSuffix(line, ";")
        line = strings.TrimSpace(line)
        if line == "" {
            continue
        }
        lower := strings.ToLower(line)
        if lower == "exit" || lower == "quit" {
            return nil
        }
        if err := executeOne(opts, db, backend, line); err != nil {
            fmt.Fprintf(opts.ErrOut, "ERROR: %v\n", err)
            // 不退出 REPL
        }
    }
}

func executeOne(opts Options, db *gorm.DB, backend Backend, raw string) error {
    sqlText := Translate(raw, backend)
    start := time.Now()
    if isReadStatement(sqlText) {
        return runReadQuery(opts, db, sqlText, start)
    }
    return runWriteQuery(opts, db, sqlText, start)
}

func runReadQuery(opts Options, db *gorm.DB, sqlText string, start time.Time) error {
    rows, err := db.Raw(sqlText).Rows()
    if err != nil {
        return errors.Wrap(err, "query")
    }
    defer rows.Close()
    cols, err := rows.Columns()
    if err != nil {
        return errors.Wrap(err, "columns")
    }
    var result [][]interface{}
    for rows.Next() {
        scanTargets := make([]interface{}, len(cols))
        scanValues := make([]sql.RawBytes, len(cols))
        for i := range scanTargets {
            scanTargets[i] = &scanValues[i]
        }
        if err := rows.Scan(scanTargets...); err != nil {
            return errors.Wrap(err, "scan")
        }
        row := make([]interface{}, len(cols))
        for i, raw := range scanValues {
            if raw == nil {
                row[i] = nil
            } else {
                row[i] = string(raw)
            }
        }
        result = append(result, row)
    }
    dur := time.Since(start)
    renderRows(opts, cols, result)
    if len(result) == 0 {
        fmt.Fprintf(opts.Out, "Empty set (%s)\n", formatDuration(dur))
    } else {
        fmt.Fprintf(opts.Out, "%d row%s in set (%s)\n", len(result), plural(len(result)), formatDuration(dur))
    }
    return nil
}

func runWriteQuery(opts Options, db *gorm.DB, sqlText string, start time.Time) error {
    res := db.Exec(sqlText)
    if res.Error != nil {
        return errors.Wrap(res.Error, "exec")
    }
    dur := time.Since(start)
    fmt.Fprintf(opts.Out, "%d row%s affected (%s)\n", res.RowsAffected, plural(int(res.RowsAffected)), formatDuration(dur))
    return nil
}

func renderRows(opts Options, cols []string, rows [][]interface{}) {
    if len(rows) == 0 && opts.Format == "table" {
        return // RenderTable 在空行时也会跳过；这里显式 short-circuit
    }
    truncate := DefaultTruncate
    if opts.NoTruncate {
        truncate = 0
    }
    switch opts.Format {
    case "json":
        RenderJSON(opts.Out, cols, rows)
    case "csv":
        RenderCSV(opts.Out, cols, rows)
    default:
        if opts.Vertical {
            RenderVertical(opts.Out, cols, rows, RenderOptions{TruncateAt: truncate})
        } else {
            RenderTable(opts.Out, cols, rows, RenderOptions{TruncateAt: truncate})
        }
    }
}

// isReadStatement 按首关键字判定 SQL 是读还是写。
// 用于选择 db.Raw().Rows() 还是 db.Exec()。
func isReadStatement(sqlText string) bool {
    fields := strings.Fields(strings.TrimSpace(sqlText))
    if len(fields) == 0 {
        return false
    }
    head := strings.ToUpper(fields[0])
    switch head {
    case "SELECT", "PRAGMA", "EXPLAIN", "WITH", "SHOW", "DESC", "DESCRIBE":
        return true
    }
    return false
}

func plural(n int) string {
    if n == 1 {
        return ""
    }
    return "s"
}

func formatDuration(d time.Duration) string {
    if d < time.Millisecond {
        return fmt.Sprintf("%.3fs", d.Seconds())
    }
    return fmt.Sprintf("%.3fs", d.Seconds())
}

func closeGorm(db *gorm.DB) {
    sqlDB, err := db.DB()
    if err != nil {
        log.Logger.Warnf("get underlying sql.DB: %v", err)
        return
    }
    if err := sqlDB.Close(); err != nil {
        log.Logger.Warnf("close db: %v", err)
    }
}
```

### Step 5: 跑测试看绿

```bash
go test ./cmd/sqlcli/ -v
```

Expected: 全部 PASS（共 ~12 个测试，包括之前几个 task 的 + 新增的两个）。

### Step 6: 编译

```bash
go build ./...
```

Expected: 通过。

### Step 7: Commit

```bash
git add cmd/sqlcli/sqlcli.go cmd/sqlcli/sqlcli_test.go
git commit -m "$(cat <<'EOF'
feat(sqlcli): add Run/RunREPL/RunSingleShot entry points

REPL via chzyer/readline (arrow keys + Ctrl-R + history file).
Single-shot mode reads one query, runs it, returns. SQL is
dispatched read-vs-write by first keyword; reads scan into
[]sql.RawBytes (NULL-safe) and render via the chosen format;
writes go through gorm.Exec and report RowsAffected.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: 注册 `ckmanctl sql` 子命令

**Files:**
- Modify: `cmd/ckmanctl/ckmanctl.go`

### Step 1: 在 `cmd/ckmanctl/ckmanctl.go` 顶部 var block 中新增 sql 命令声明

在已有 `dumpCmd` 等附近追加：

```go
sqlCmd       = kingpin.Command("sql", "interactive SQL shell against ckman's persistent backend")
sql_conf     = sqlCmd.Flag("conf", "ckman config file path").Short('c').Default("/etc/ckman/conf/ckman.hjson").String()
sql_query    = sqlCmd.Flag("query", "single-shot SQL; non-empty means run-and-exit").Short('q').String()
sql_format   = sqlCmd.Flag("format", "output format: table | json | csv").Default("table").String()
sql_vertical = sqlCmd.Flag("vertical", "render rows vertically (mysql \\G)").Short('E').Bool()
sql_notrunc  = sqlCmd.Flag("no-truncate", "do not truncate long cell values").Short('N').Bool()
```

### Step 2: 在 main() 的 switch 中加 case

```go
case "sql":
    runSQLCmd(*sql_conf, *sql_query, *sql_format, *sql_vertical, *sql_notrunc)
```

### Step 3: 在文件底部加 runSQLCmd 函数

```go
func runSQLCmd(confPath, query, format string, vertical, noTrunc bool) {
    if err := config.ParseConfigFile(confPath, ""); err != nil {
        fmt.Printf("parse config %s failed: %v\n", confPath, err)
        os.Exit(1)
    }
    policy := config.GlobalConfig.Server.PersistentPolicy
    cfgMap, _ := config.GlobalConfig.PersistentConfig[policy]
    opts := sqlcli.Options{
        Policy:     policy,
        ConfMap:    cfgMap,
        Query:      query,
        Format:     format,
        Vertical:   vertical,
        NoTruncate: noTrunc,
    }
    if err := sqlcli.Run(opts); err != nil {
        fmt.Fprintf(os.Stderr, "%v\n", err)
        os.Exit(1)
    }
}
```

### Step 4: import 调整

`cmd/ckmanctl/ckmanctl.go` 顶部 import 区追加：

```go
"os"
"github.com/housepower/ckman/cmd/sqlcli"
"github.com/housepower/ckman/config"
```

（`fmt` 应已存在；`config` 可能已 import；按需补充。）

### Step 5: 编译

```bash
go build ./...
```

Expected: 通过。

### Step 6: 烟测 1 —— REPL

```bash
TMPDIR=$(mktemp -d)
mkdir -p "$TMPDIR/conf"
cat > "$TMPDIR/conf/ckman.hjson" <<'EOF'
{
  "server": {"ip": "127.0.0.1", "port": 18820, "session_timeout": 60, "persistent_policy": "local", "public_key": ""},
  "log": {"level": "info", "max_count": 5, "max_size": 10, "max_age": 30},
  "persistent_config": {"local": {"config_dir": "TMPDIR/conf", "config_file": "clusters"}},
  "cron": {},
  "nacos": {"enabled": false}
}
EOF
sed -i "s|TMPDIR|$TMPDIR|g" "$TMPDIR/conf/ckman.hjson"

go build -o "$TMPDIR/ckmanctl" ./cmd/ckmanctl/

# 启动一次 ckman 让它建 db
go build -o "$TMPDIR/ckman" .
"$TMPDIR/ckman" -c "$TMPDIR/conf/ckman.hjson" -l "$TMPDIR/ckman.log" -p "$TMPDIR/ckman.pid" &
sleep 3
kill -TERM %1 2>/dev/null; wait 2>/dev/null

# 单次模式：SHOW TABLES
"$TMPDIR/ckmanctl" sql -c "$TMPDIR/conf/ckman.hjson" -q "SHOW TABLES"
```

Expected: 输出包含 `tbl_cluster` / `tbl_meta` 等 8 张表（mysql 风格 ASCII 表格）。

### Step 7: 烟测 2 —— JSON 格式

```bash
"$TMPDIR/ckmanctl" sql -c "$TMPDIR/conf/ckman.hjson" -q "SELECT key,value FROM tbl_meta" --format=json
```

Expected: NDJSON 输出，含 `migrated_from` 等 key。

### Step 8: 烟测 3 —— DESC

```bash
"$TMPDIR/ckmanctl" sql -c "$TMPDIR/conf/ckman.hjson" -q "DESC tbl_cluster"
```

Expected: 列出 tbl_cluster 的字段（id, created_at, updated_at, deleted_at, cluster_name, config）。

### Step 9: 烟测 4 —— REPL（人工）

```bash
"$TMPDIR/ckmanctl" sql -c "$TMPDIR/conf/ckman.hjson"
# 提示符 ckman> 出现
# 输入 "SHOW TABLES" 回车 → 表格输出
# 按上箭头 → 出现 "SHOW TABLES"
# 输入 "exit" → 退出
# 再启动 → 上箭头能翻到 SHOW TABLES（验 history 持久化）
```

清理：

```bash
rm -rf "$TMPDIR"
```

### Step 10: Commit

```bash
git add cmd/ckmanctl/ckmanctl.go
git commit -m "$(cat <<'EOF'
feat(ckmanctl): register sql subcommand wiring to cmd/sqlcli.Run

Reads ckman.hjson, resolves persistent_policy → persistent_config[policy]
map, hands off to sqlcli.Run with flags from kingpin. -q triggers
single-shot mode and exits with code 1 on error.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: 人工验收

**Reference:** spec §10.3

- [ ] **Step 1: `make build && make test` 全绿**

```bash
make build
make test
```

- [ ] **Step 2: REPL 端到端（local 后端）**

```bash
# 用 Task 7 的 smoke setup，但不清理。手工操作：
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson
ckman> SHOW TABLES
ckman> SELECT * FROM tbl_meta
ckman> UPDATE tbl_meta SET value='manual_test' WHERE key='migrated_from'
ckman> SELECT * FROM tbl_meta
ckman> exit
```

Expected：
- SHOW TABLES 列出 8 张表
- SELECT 输出表格
- UPDATE 输出 `1 row affected (Xs)`
- 第二次 SELECT 看到 `manual_test`
- exit 干净退出，无报错

- [ ] **Step 3: history 持久化**

```bash
ls -la ~/.ckman_sql_history
cat ~/.ckman_sql_history
```

Expected：文件存在，内容含刚刚输入的命令。

- [ ] **Step 4: 单次模式 + 管道**

```bash
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson -q "SELECT key,value FROM tbl_meta" --format=json | jq .
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson -q "SELECT * FROM tbl_cluster" --format=csv > /tmp/clusters.csv
```

Expected：
- json 输出能被 jq 解析
- csv 文件可用 less / Excel 打开

- [ ] **Step 5: 长值截断 + `-N` + `-E`**

```bash
# 先插入一个含长 JSON 的 cluster
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson -q "INSERT INTO tbl_cluster (cluster_name, config) VALUES ('test', '$(python3 -c "import json; print(json.dumps({\"hosts\":[\"10.0.0.\"+str(i) for i in range(50)]}))")')"

# 默认截断
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson -q "SELECT cluster_name, config FROM tbl_cluster"
# 看到 config 列出现 "..."

# 不截断 + 竖排
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson -q "SELECT cluster_name, config FROM tbl_cluster" -E -N
# 竖排展开看到完整 JSON
```

- [ ] **Step 6: 错误处理**

```bash
$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson -q "SELECT * FROM no_such_table"
echo "exit=$?"
# 应该输出 ERROR + exit code 1

$TMPDIR/ckmanctl sql -c $TMPDIR/conf/ckman.hjson
ckman> SELECT * FROM no_such_table
# 应该输出 ERROR 行，但不退出 REPL
ckman> SELECT 1
# 应该正常工作
ckman> exit
```

- [ ] **Step 7: 多后端切换**（如有 MySQL 测试实例）

修改 ckman.hjson 把 `persistent_policy` 改成 `mysql` 并填入 MySQL 连接信息，重复 Step 2-6 的关键验证。

---

## Self-Review Checklist

(writing-plans 强制要求；落盘前自查)

**Spec coverage:**
- §4 命令接口 → Task 7（kingpin 注册 + flag）✓
- §5 后端 dispatch → Task 2（4 个 BuildDialector）+ Task 3（OpenDB）✓
- §6.1 readline → Task 1（直接依赖）+ Task 6（RunREPL）✓
- §6.2 单行 SQL trim → Task 6 RunREPL 中 trim 逻辑 ✓
- §6.3 exit/quit/Ctrl-D → Task 6 ✓
- §6.4 meta 翻译 → Task 4 ✓
- §6.5 read/write 分发 → Task 6 isReadStatement ✓
- §7 四种渲染 → Task 5 ✓
- §8 错误处理 → Task 6 RunREPL/RunSingleShot ✓
- §10.1 单测 → Task 4/5/6 散布 ✓
- §10.2 集成测 (TestRunSingleShot_SQLite) → Task 6 ✓
- §10.3 人工验收 → Task 8 ✓

**Placeholder scan:** 没有 TBD / TODO / "如前所述" 之类。所有代码块完整。dm8 config 结构体名做了"先读文件确认"的提示而非硬编码占位。

**Type consistency:**
- `Options` 字段在 Task 6 定义、Task 7 使用 → ConfMap / Policy / Query / Format / Vertical / NoTruncate ✓
- `Backend` 枚举在 Task 3 定义、Task 4 使用、Task 6 使用 → 一致 ✓
- `RenderOptions` 在 Task 5 定义、Task 6 中以 `RenderOptions{TruncateAt: truncate}` 构造 ✓
- `BuildDialector` 签名 `(cfgMap map[string]interface{}) (gorm.Dialector, error)` 在 Task 2 四个 backend 一致 ✓

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-16-ckmanctl-sql-cli.md`.**

Two execution options:

1. **Subagent-Driven (recommended)** — 每个 Task 派发新 subagent + checkpoint review，迭代快、上下文干净
2. **Inline Execution** — 当前 session 顺序跑，批量执行 + checkpoint

Which approach?
