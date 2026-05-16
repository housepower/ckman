# 用 SQLite 替换 `local` 持久化后端的 JSON 文件存储

**日期：** 2026-05-16
**作者：** brainstorming 会话
**状态：** Draft（待用户审阅）

## 1. 背景与动机

当前 `repository/local` 后端把所有持久化数据（cluster、logic cluster、query history、task、backup、backup policy、backup run）整体序列化为单个 JSON / YAML 文件（默认 `conf/clusters.json`）。该实现有以下问题：

- **每次写都全量重写文件**：任意一条记录变更都会 `os.Rename` + `O_TRUNC` 重写整文件。当 query history、backup run 等表条数增长后写放大严重。
- **无原生事务**：通过 `sync.RWMutex` + `gob` 深拷贝快照模拟事务，与 mysql/postgres/dm8 后端的 GORM 事务语义不统一。
- **无并发读写**：全局 RWMutex 串行化所有读写。
- **无二级查询能力**：所有过滤条件都在 Go 侧 for-loop 扫描，与 mysql/postgres/dm8 后端的查询能力差异大。

目标：把 `local` 后端底层换成 SQLite，对外接口（`PersistentMgr`）零变化、对用户配置（`persistent_policy: local`）零变化、对已有 JSON 数据自动一次性迁移。

## 2. 范围

**本次包含：**
- 新增 `repository/sqlite` 包，作为 `persistent_policy = "local"` 的实现
- 把现有 `repository/local` 重命名为 `repository/legacyjson`，移除注册，仅作为迁移读源 / dump 工具的写入目标
- 启动时自动检测旧 JSON / YAML 文件并迁移到 SQLite，旧文件加时间戳后缀保留
- 在 SQLite 内建 `_meta` 表记录迁移信息，保证幂等
- 重构 `cmd/migrate.Migrate()` 为可复用函数 `MigrateBetween(src, dst)`，顺手补全缺失的 BackupPolicy / BackupRun 迁移
- 在 `repository.PersistentMgr` 接口上新增 `GetAllBackupPolicies()` / `GetAllBackupRuns()`，各后端补实现
- 新增 `ckmanctl dump-to-json` 子命令，将 SQLite 数据离线导出为 legacy JSON，用于降级场景
- ckman 优雅关闭时自动写一份 `clusters.json.shutdown_snapshot.<ts>.json` 兜底快照

**本次不包含（YAGNI）：**
- SQLite → JSON 反向降级路径（仅以"手动改名"作为已知操作记录）
- schema 版本管理框架（仅保留 `_meta.schema_version` 字段占位，本次固定写 1）
- 删除 `legacyjson` 包（保留至少一个发布周期，待用户都升级过再下次删）
- 多实例共享 SQLite 文件（约束沿用旧 JSON 行为，不构成回归）

## 3. 关键设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 新旧后端关系 | 替换式 | 用户配置 `persistent_policy: local` 完全不变，真正"无感" |
| SQLite 驱动 | `github.com/glebarez/sqlite` | 纯 Go（底层 `modernc.org/sqlite`），与 GORM 兼容，保持 ckman 的 `CGO_ENABLED=0` 编译方式 |
| 包结构 | 实现包名 `sqlite`，注册名仍是 `"local"` | 代码语义清晰：包名反映存储介质，配置名反映用户视角 |
| 迁移触发 | 启动时自动 + `_meta` 表幂等记录 + 旧文件改名 | 三层兜底：标记表、改名、显式失败 |
| 迁移失败策略 | `Init()` 返回 error，ckman 启动失败 | 显式失败优于静默丢数据；事务回滚保证库无半截数据 |
| 配置字段兼容 | `LocalConfig.Format` 降级为"迁移源定位器" | 老配置文件不需要修改 |

## 4. 架构与目录结构

```
repository/
├── sqlite/                       # 新包，package sqlite
│   ├── factory.go                # GetPersistentName() 返回 "local"
│   ├── constant.go               # 表名等常量
│   ├── config.go                 # LocalConfig（保留字段名以兼容反序列化）
│   ├── model.go                  # GORM 模型 Tbl*
│   ├── sqlite.go                 # 主实现 SQLitePersistent
│   ├── migrate.go                # 调用 legacyjson + cmd/migrate.MigrateBetween
│   └── sqlite_test.go
├── legacyjson/                   # 由原 repository/local/ 重命名而来
│   ├── reader.go                 # NewReader(cfg) PersistentMgr —— 用于启动期迁移读源
│   ├── writer.go                 # NewWriter(cfg) PersistentMgr —— 用于 dump-to-json / shutdown snapshot
│   ├── model.go                  # 原 PersistentData 结构
│   ├── helper.go                 # 排序、辅助方法
│   └── （删除 init() 中的 RegistePersistent）
└── persistent.go                 # 接口新增 GetAllBackupPolicies / GetAllBackupRuns
```

**外部 import 更新：** 仅 `cmd/migrate/migrate.go` 一处 `_ "github.com/housepower/ckman/repository/local"` 改为 `_ "github.com/housepower/ckman/repository/sqlite"`。其他业务层均通过 `repository.Ps` 接口访问，不受影响。

## 5. 数据模型

`repository/sqlite/model.go` 定义七张表 + `_meta`，字段与 `repository/mysql/model.go` 对齐：

| 表 | 主键 / 唯一索引 | 大字段（TEXT，存 JSON） |
|---|---|---|
| `tbl_cluster` | `cluster_name` unique | `config` |
| `tbl_logic` | `logic_name` unique | `physic_clusters` |
| `tbl_query_history` | `checksum` PK | `query` |
| `tbl_task` | `task_id` PK | `config` |
| `tbl_backup` | `backup_id` | `backup` |
| `tbl_backup_policy` | `policy_id` PK + `(cluster, db, table)` + `instance` | `policy` |
| `tbl_backup_run` | `run_id` PK + 三个复合索引 | `run` |
| `_meta` | `key` PK | `value` |

`_meta` 至少写入以下两条：
- `migrated_from`：`"<legacy filename>@<ISO8601>"` 或 `"(fresh install)"`
- `schema_version`：当前固定 `"1"`

**与 mysql backend 的差异：**
- 所有 `type:JSON` 改为 `type:TEXT`（SQLite 弱类型，显式 TEXT 避免歧义）
- 时间字段 `time.Time` 用 GORM 默认 `DATETIME`（SQLite 存为 ISO8601 文本）

**SQLite Pragma（DSN 一次性设置）：**
```
file:/path/to/clusters.db?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)
```
`glebarez/sqlite` 原生支持 `_pragma=` 形式。

## 6. 事务与并发

**与 `MysqlPersistent` 完全对齐**，丢弃旧 local 的 `sync.RWMutex + gob 深拷贝`：

```go
type SQLitePersistent struct {
    Config   LocalConfig
    Client   *gorm.DB  // 当前活跃句柄（事务中为 tx）
    ParentDB *gorm.DB  // 根连接
}

func (lp *SQLitePersistent) Begin() error {
    if lp.Client != lp.ParentDB { return repository.ErrTransActionBegin }
    tx := lp.Client.Begin()
    lp.Client = tx
    return tx.Error
}
// Commit / Rollback 对称，结束后 lp.Client = lp.ParentDB
```

所有读写方法使用 `lp.Client.Xxx(...)`，未开事务时即根连接。

**并发模型：**
- **写串行**：SQLite 单写锁 + WAL 模式；`busy_timeout=5000` 兜底偶发并发写。
- **读并发**：WAL 模式下多 goroutine 读不互斥。
- **跨实例**：单文件 SQLite 不支持 ckman 多实例共享，约束沿用旧 JSON 行为，不构成回归。多实例部署仍需走 MySQL / Postgres backend。

## 7. 迁移流程

### 7.1 入口

由 `SQLitePersistent.Init()` 在 `gorm.Open` + `AutoMigrate` 之后调用：

```go
func (lp *SQLitePersistent) migrateLegacyIfAny() error {
    if v, _ := readMeta(lp.Client, "migrated_from"); v != "" {
        return nil  // 已迁移过
    }

    legacyFile := lp.Config.legacyFilePath()  // 按 Format 拼 .json / .yaml；空则两个都试
    if legacyFile == "" {
        return writeMeta(lp.Client, "migrated_from", "(fresh install)")
    }

    src, err := legacyjson.NewReader(legacyjson.LegacyConfig{
        File:   legacyFile,
        Format: lp.Config.Format,
    })
    if err != nil { return err }

    if err := lp.Begin(); err != nil { return err }
    if err := migrate.MigrateBetween(src, lp); err != nil {
        _ = lp.Rollback()
        return err
    }
    stamp := fmt.Sprintf("%s@%s", filepath.Base(legacyFile), time.Now().UTC().Format(time.RFC3339))
    if err := writeMeta(lp.Client, "migrated_from", stamp); err != nil {
        _ = lp.Rollback()
        return err
    }
    if err := lp.Commit(); err != nil { return err }

    // 数据已落库，旧文件改名仅为人工兜底；改名失败只打 Warn，不阻断启动
    return renameLegacyWithTimestamp(legacyFile)
}
```

### 7.2 `MigrateBetween` 重构

`cmd/migrate/migrate.go` 现有 `Migrate()` 依赖包级全局 `psrc/pdst`，且**缺 BackupPolicy / BackupRun 的迁移**。重构为：

```go
func MigrateBetween(src, dst repository.PersistentMgr) error {
    // 顺序：clusters → logics → query history → tasks → backups
    //      → backup policies (新增) → backup runs (新增)
    // 调用方负责 dst.Begin() / Commit() / Rollback()
}

func MigrateHandle(conf string) {
    // ... 沿用 ParseConfig / PersistentCheck，最后调 MigrateBetween(psrc, pdst)
}
```

### 7.3 接口扩展

`repository/persistent.go` 新增：

```go
type PersistentBackupPolicyService interface {
    // 已有方法 ...
    GetAllBackupPolicies() ([]model.BackupPolicy, error)  // 新增（迁移用）
}

type PersistentBackupRunService interface {
    // 已有方法 ...
    GetAllBackupRuns() ([]model.BackupRun, error)         // 新增（迁移用）
}
```

各 backend（`legacyjson`、`sqlite`、`mysql`、`postgres`、`dm8`）补实现。

### 7.4 失败语义

| 场景 | 处理 |
|---|---|
| 旧文件不存在 | 写 `_meta.migrated_from = "(fresh install)"`，正常启动 |
| 旧文件存在但 parse 失败 | 返回 error，事务回滚（库空），旧文件未改名，`Init()` 冒泡，ckman 启动失败 |
| `MigrateBetween` 中途失败 | 事务回滚（库空），旧文件未改名，`Init()` 冒泡，ckman 启动失败 |
| 事务 commit 成功但旧文件改名失败 | 数据已落库，仅打 Warn；下次启动因 `_meta.migrated_from` 已写入而跳过迁移 |
| 已迁移过（`_meta.migrated_from` 非空） | 直接返回 nil，不重复迁移 |

### 7.5 密码字段

- 上层 `CreateCluster / UpdateCluster` 调用前已经 `EncodePasswd`，`GetCluster*` 返回前 `DecodePasswd`
- `sqlite` backend 在这些方法里保持与 mysql backend 同样的调用次序
- `MigrateBetween` 走 `src.GetAllClusters()`（解码）→ `dst.CreateCluster(c)`（重编码）的对称路径，密码字节最终保持原值

## 8. 向后兼容

### 8.1 配置文件兼容

老配置三种形态新代码都正常吃进：

| 老配置 | 新行为 |
|---|---|
| `local: {}` 或缺省 | `Format=""`，DB 路径 `conf/clusters.db`；迁移阶段先后试 `clusters.json` / `clusters.yaml` |
| `local: {format: json, config_file: clusters}` | DB 路径 `conf/clusters.db`；迁移源 `conf/clusters.json` |
| `local: {format: yaml, config_file: clusters}` | DB 路径 `conf/clusters.db`；迁移源 `conf/clusters.yaml` |

**`LocalConfig.Normalize()` 关键调整：** 不再用 `Format` 拼后缀，统一拼 `.db`。基名复用 `ConfigFile`，扩展名强制 `.db`。

### 8.2 降级路径

详见 §14。简言之：推荐用 `ckmanctl dump-to-json` 离线导出 JSON 后再换二进制；紧急情况可手动改回 `.migrated.<ts>` 文件，代价是丢失升级后所有变更。

### 8.3 错误映射

所有查询方法在返回前转换：
```go
if errors.Is(err, gorm.ErrRecordNotFound) {
    return ..., repository.ErrRecordNotFound
}
```
与 mysql backend 一致。

## 9. 日志

启动时打 `Info` 一行，包含旧文件名、各表迁移记录数、新 DB 路径，例：

```
INFO local persistent: migrated from conf/clusters.json -> conf/clusters.db
     (clusters=3, logics=1, query_history=42, tasks=120, backups=8, backup_policies=2, backup_runs=15)
```

用户能从日志看出"无感"背后发生了什么。

## 10. 测试计划

### 10.1 单测（`repository/sqlite/sqlite_test.go`）

| 用例 | 验证点 |
|---|---|
| `TestSQLiteFresh` | 空目录启动 → 创建 db、AutoMigrate 七张表 + `_meta`、`_meta.migrated_from = "(fresh install)"` |
| `TestSQLiteCRUD` | 每张表 Create / Get / Update / Delete，错误映射到 `repository.ErrRecordNotFound` / `ErrRecordExists` |
| `TestSQLiteTx` | Begin → 多写 → Rollback：库无残留；Begin → 多写 → Commit：可见 |
| `TestMigrateFromJSON` | 复制 testdata/legacy_clusters.json → 启动 → 数据齐全（含 BackupPolicy / BackupRun），旧文件被改名 `.migrated.<ts>` |
| `TestMigrateFromYAML` | 同上，源为 yaml |
| `TestMigrateIdempotent` | 第一次完成后再启动 → 不重复迁移、不报错（通过 `_meta.migrated_from` 判定） |
| `TestMigrateLegacyMissing` | `format=json` 但文件不存在 → `_meta.migrated_from = "(fresh install)"`，正常启动 |
| `TestMigrateLegacyCorrupt` | 坏 JSON → `Init()` 返回 error，db 无任何数据落地，旧文件未改名 |
| `TestPasswordRoundTrip` | 迁移后 GetCluster 解码出的密码 == 原 JSON 中明文密码 |

### 10.2 `cmd/migrate` 测试

补 `TestMigrateBetween_localToSQLite`，验证 BackupPolicy / BackupRun 也搬运（锁住 `MigrateBetween` 修复）。

### 10.3 `ckmanctl dump-to-json` 测试

| 用例 | 验证点 |
|---|---|
| `TestDumpToJSON_RoundTrip` | 准备 SQLite db（含全部七张表数据），跑 `dump-to-json`，再用 `legacyjson.NewReader` 读出，逐张表 deep-equal |
| `TestDumpToJSON_PasswordObfuscation` | 集群密码字段在导出 JSON 中保持加密形态（与升级前 JSON 字节兼容） |
| `TestDumpToJSON_OfflineMode` | ckman 服务未启动也能正常执行（直接打开 db 文件） |

### 10.4 人工验收清单

1. `make build && make test` 全绿
2. 拿一份脱敏现网 `clusters.json` 放进 `conf/`，启动 ckman → 看日志一行 "Migrated ..."
3. 登录 UI，验证集群列表、备份策略、任务历史齐全
4. 重启一次，日志中**不再**出现 "Migrated" 字样
5. `sqlite3 conf/clusters.db ".schema"` 看七张表 + `_meta` 表存在
6. `ckmanctl migrate -f migrate.hjson` 把 sqlite 数据搬到 mysql，验证 BackupPolicy / Run 也搬过去了
7. `ckmanctl dump-to-json -c conf/ckman.hjson -o /tmp/dump.json` → 用旧版 ckman 二进制把 `/tmp/dump.json` 作为 `clusters.json` 启动，UI 数据齐全（降级路径端到端验证）
8. `systemctl stop ckman` 后检查 `conf/` 目录出现 `clusters.json.shutdown_snapshot.<ts>.json` 文件

## 11. 依赖变更

`go.mod` 新增 `github.com/glebarez/sqlite`（具体版本号在实施期取与 `gorm.io/gorm v1.23.5` 兼容的最新稳定版）。间接引入 `modernc.org/sqlite`（纯 Go，无 CGo 依赖）。

## 12. 风险与限制

| 风险 | 缓解 |
|---|---|
| 大表（query_history、backup_run）迁移耗时 | 单事务批量 INSERT；启动日志打记录数让用户感知 |
| 用户手动改回 `.migrated.<ts>` → `.json` 触发重迁 | `_meta.migrated_from` 是首要防御：一旦写入，后续启动直接跳过迁移逻辑，旧文件被忽略。运维要"强制重迁"必须显式删除 `clusters.db`（见 §13）。 |
| 多实例共享 SQLite 文件 | 不支持，与旧 JSON 行为一致；多实例部署引导用 MySQL/Postgres |
| `glebarez/sqlite` 与 `gorm.io/gorm v1.23.5` 版本兼容性 | 实施时验证；若不兼容，备选 `crawshaw.io/sqlite` 自行封装（成本高，仅作 fallback） |

## 13. 已知操作记录（运维）

- **降级**：见 §14
- **强制重迁**：删除 `conf/clusters.db` 和 `_meta` 记录，把 `.migrated.<ts>` 文件改回 `.json`，启动 ckman
- **查看 SQLite 数据**：`sqlite3 conf/clusters.db` 标准命令行，或任何 SQLite GUI 工具

## 14. 降级路径

降级 = 用户因为新版 bug、性能回退等原因，需要回到旧版 ckman 二进制。
旧二进制只识别 JSON，不识别 SQLite。设计三层兜底，按数据损失从小到大：

### 14.1 推荐路径：`ckmanctl dump-to-json`（零损失）

新增子命令：
```
ckmanctl dump-to-json -c /etc/ckman/conf/ckman.hjson -o conf/clusters.json
```

**实现：**
- 不依赖 ckman 服务运行，直接读 `clusters.db` 文件
- 复用 `MigrateBetween(sqlite, legacyjson)` 反向调用
- 输出文件采用 legacy JSON 格式（旧二进制原生兼容）

**降级标准流程（推荐先 stop）：**
```
systemctl stop ckman
ckmanctl dump-to-json -c conf/ckman.hjson -o conf/clusters.json
# 备份并移除 clusters.db 防止新二进制重启时再次迁移覆盖 JSON
mv conf/clusters.db conf/clusters.db.bak
# 换回旧二进制
systemctl start ckman
```

**ckman 仍在运行也能跑** —— 工具以 `?mode=ro` + `BEGIN DEFERRED` 打开 db，拿事务开始那一刻的一致快照。后续 ckman 写入不影响输出，但也不会出现在结果里（详见 §15）。这种用法适合"先粗略导一份，再 stop 服务后 dump 一次拿增量"的灰度回退。

**适用：** 计划性降级；新旧版本数据 schema 兼容。

### 14.2 优雅关闭快照（最多丢上一次启动后的写）

ckman 收到 SIGTERM 走优雅关闭路径时，触发一次 `dump-to-json`，输出到 `conf/clusters.json.shutdown_snapshot.<ts>.json`。

**收益：** 即使运维忘记跑 `ckmanctl dump-to-json`，只要服务是 `systemctl stop` 这种正常停止，最近一次的快照永远在。降级时把这个文件 `mv` 成 `clusters.json` 即可。

**不适用场景：** `kill -9` / 进程崩溃 / 断电 —— 拿不到关闭钩子。

### 14.3 最后兜底：手动改回 `.migrated.<ts>`（丢升级后所有变更）

`.migrated.<ts>` 文件是升级时刻的 JSON 快照，永远留在磁盘。极端情况下（SQLite 文件损坏、`dump-to-json` 也跑不通）：

```
mv conf/clusters.json.migrated.<ts> conf/clusters.json
mv conf/clusters.db conf/clusters.db.broken
# 启动旧二进制
```

**代价：** 丢失升级以来的所有数据变更（新增集群、任务历史、备份执行记录等）。

### 14.4 三层组合的覆盖矩阵

| 场景 | 14.1 | 14.2 | 14.3 |
|---|---|---|---|
| 计划性降级 | ✅ 推荐 | 兜底 | 兜底 |
| 紧急降级，ckman 还能 graceful stop | ✅ 推荐 | ✅ 自动可用 | 兜底 |
| 紧急降级，ckman 已被 kill -9 | ✅（只要 db 文件没坏） | ❌ | ✅ |
| `clusters.db` 文件损坏 | ❌ | ✅（若上次有过 graceful stop） | ✅ |

## 15. 并发访问与一致性

### 15.1 多进程访问 `clusters.db`

SQLite 通过文件级 fcntl 锁支持多进程访问：

| 访问组合 | WAL 模式（本设计） | 备注 |
|---|---|---|
| 多个进程并发读 | ✅ 不互斥 | 各进程独立打开只读句柄即可 |
| 一个写（ckman） + 多个读（ckmanctl） | ✅ 不互斥 | 读不阻塞写，写不阻塞读 |
| 多个进程同时写 | ❌ 串行 | 后者拿写锁失败 → `busy_timeout=5000` 自动重试 5 秒，超时报错 |

**文件系统约束：** 必须支持 POSIX advisory locking。
- 本地 ext4 / xfs / btrfs ✅
- NFS ❌（已知 SQLite 在 NFS 上行为不可靠）
- ckman 默认安装路径 `/etc/ckman/conf` 通常在本地盘 —— 设计文档显式声明"`clusters.db` 必须放在本地文件系统"

### 15.2 ckmanctl 工具的快照语义

`ckmanctl dump-to-json` 和 `ckmanctl migrate` 在 ckman 运行期间使用时：

**实现要点：**
- DSN 加 `?mode=ro`，确保只读打开，绝不参与写锁竞争
- 工具开始时执行 `BEGIN DEFERRED`，整个导出过程在同一个读事务里完成
- 输出代表"事务开始那一刻"的一致快照

**用户视角：**
- 工具不会因为 ckman 在写而失败或读到脏数据
- ckman 在工具运行**期间**写入的数据**不会**出现在导出结果中（事务隔离）
- 工具退出时间和事务开始时间可能差几秒，差异可忽略

**stdout 提示：** 工具运行后打印一行
```
Snapshot captured at 2026-05-16T10:30:45Z (ckman pid=12345 still running; writes after this point are not included)
```
让运维显式知晓快照边界。

### 15.3 多 ckman 实例共享同一 `clusters.db`

**不支持。** 即使 SQLite 支持多写者协调，ckman 内部的 cron 调度、master 节点选举等都假设单实例对持久层独占。多实例部署必须用 MySQL / Postgres backend，这点和旧 JSON 行为一致。

### 15.4 测试用例补充

在 §10.3 中新增：

| 用例 | 验证点 |
|---|---|
| `TestDumpToJSON_ConcurrentWithWrites` | 起一个 goroutine 持续写 SQLite 集群表；并发跑 `dump-to-json`；导出文件中数据是一致快照，不出现半截 row 或字段错位 |
