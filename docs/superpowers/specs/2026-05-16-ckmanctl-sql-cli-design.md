# `ckmanctl sql`：ckman 持久化层 SQL 终端

**日期：** 2026-05-16
**作者：** brainstorming 会话
**状态：** Draft（待用户审阅）

## 1. 背景与动机

`local` 持久化后端从 JSON 文件换成 SQLite 后，运维"打开文件用编辑器看一眼"的便利性丢失。`ckmanctl dump-to-json` 解决了"看全部"的痛点（一次性导出），但不能做 ad-hoc 查询，例如：
- "哪些 policy 启用了但 inflight runs 数为 0？"
- "昨天 backup 失败的 run 详情 + 错误信息？"
- "找出 N 天没活动的 cluster 配置"

同时，ckman 配 MySQL / PostgreSQL / DM8 后端时，运维也常会直接登库做诊断。三种 DB client 用法各异，运维要切换思维。

目标：给 ckmanctl 加一个统一的 SQL 终端，**根据 ckman.hjson 的 `persistent_policy` 自动连到正确的后端**，全权限透传 SQL。定位是"辅助 ckman 运维"，不是大而全的 DB 客户端。

## 2. 范围

**本次包含：**
- 新增 `cmd/sqlcli` 包（含 REPL + 单次执行 + 多后端 dispatch + 翻译表 + 渲染）
- `cmd/ckmanctl/ckmanctl.go` 注册 `sql` 子命令
- 四个 backend 包 export `BuildDSN(cfgMap) (driver, dsn string, err error)`（小重构）
- `github.com/chzyer/readline` 从间接依赖提升为直接依赖

**本次不包含（YAGNI）：**
- 多行 SQL（一条 SQL 一行写完，`;` 末尾可选）
- 写入护栏（无 `--write` 开关，无 DROP/ALTER 黑名单，连上去就是全权限）
- 复杂 meta commands（除 SHOW DATABASES / SHOW TABLES / DESC 之外没有）
- 凭证管理（DSN 直接复用 ckman.hjson 里的明文密码字段）
- 跨表事务（事务靠 `BEGIN; ... COMMIT;` SQL 透传，客户端不介入）

## 3. 关键设计决策

| 决策点 | 选择 | 理由 |
|---|---|---|
| 执行模式 | REPL + 单次 `-q` 都支持 | 一次实现覆盖交互排障 + 脚本管道 |
| 多后端 dispatch | 读 ckman.hjson 的 `persistent_policy` 自动选 | 用户配什么连什么，零心智负担 |
| 连接方式 | 直接 `gorm.Open(driver, dsn)`，不复用 `repository.Ps` | 不触发启动期迁移；不需要 PersistentMgr 抽象层 |
| SQL 透传 | 100% raw，仅 trim 末尾分号 | 不解析、不修改、不护栏 |
| Meta 命令 | 三条翻译：`SHOW DATABASES` / `SHOW TABLES` / `DESC <tbl>` | 跨后端体验一致；其他都用 SQL 原生写法 |
| 行编辑 | `chzyer/readline` | 方向键 + 历史 + Ctrl-R 反搜一揽子，代码就一行 |
| 输出 | ASCII 表格默认 + `--format=json|csv` + `--vertical` + `--no-truncate` | mysql 客户端风格，管道兼容 |
| 错误处理 | REPL 错误不退出；`-q` 模式错误退出码 1 | 交互友好；脚本可检测失败 |

## 4. 命令接口

```
ckmanctl sql [flags]

Flags:
  -c, --conf string       ckman config file path (default "/etc/ckman/conf/ckman.hjson")
  -q, --query string      execute single statement then exit
      --format string     output format: table | json | csv (default "table")
  -E, --vertical          render each row vertically (mysql \G style)
  -N, --no-truncate       do not truncate long cell values (default 60 chars)
```

**两种典型用法：**

```bash
# REPL
ckmanctl sql -c /etc/ckman/conf/ckman.hjson
ckman> SHOW TABLES;
ckman> SELECT cluster_name, comment FROM tbl_cluster WHERE deleted_at IS NULL;
ckman> UPDATE tbl_backup_policy SET enabled = 0 WHERE policy_id = 'p1';
ckman> exit

# 脚本/管道
ckmanctl sql -q "SELECT * FROM tbl_cluster" --format=json | jq '.[].cluster_name'
ckmanctl sql -q "SELECT * FROM tbl_backup_run" --format=csv > runs.csv
```

## 5. 后端 dispatch + 连接

启动时按 `persistent_policy` 选驱动：

| policy | 驱动 | DSN 来源 |
|---|---|---|
| `local` | `github.com/glebarez/sqlite` | `file:<config_dir>/<config_file>.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)` |
| `mysql` | `gorm.io/driver/mysql` | `<user>:<pass>@tcp(<host>:<port>)/<database>?charset=utf8mb4&parseTime=True&loc=Local` |
| `postgres` | `gorm.io/driver/postgres` | `host=<host> port=<port> user=<user> password=<pass> dbname=<database> sslmode=disable` |
| `dm8` | `github.com/wanlay/gorm-dm8` | 复用 `repository/dm8.Dm8Config` 的 DSN 字符串构造 |

**DSN 复用而非 PersistentMgr 复用：** 每个 backend 包 export 一个公开函数：

```go
// repository/sqlite/config.go (示例)
func BuildDSN(cfgMap map[string]interface{}) (driver, dsn string, err error)
```

`cmd/sqlcli` 读 ckman.hjson → 找对应包 → `gorm.Open(driver, dsn)`。一次重构覆盖四个 backend，每个 ~10 行。

**为什么不走 `repository.Ps`：**
- 不需要 PersistentMgr 抽象（透传 SQL）
- 不需要触发启动期迁移（sqlite 的 `migrateLegacyIfAny` 在 SQL 工具场景下完全无意义）
- 直接拿 `*gorm.DB`，输出列名/类型更直接（避免 GORM 高层抽象遮蔽 raw row）

**连接生命周期：** REPL 启动时 open 一次，会话期复用；退出（exit / quit / EOF / 信号）时 close。`-q` 模式同理（单次执行后 close）。

## 6. 输入处理

### 6.1 行编辑：`chzyer/readline`

```go
rl, _ := readline.NewEx(&readline.Config{
    Prompt:          "ckman> ",
    HistoryFile:     filepath.Join(os.Getenv("HOME"), ".ckman_sql_history"),
    InterruptPrompt: "^C",
    EOFPrompt:       "exit",
})
```

得到：
- 上/下方向键翻历史
- 左/右光标移动
- Ctrl-A / Ctrl-E 行首/尾
- Ctrl-R 反向搜索
- History 持久化到 `~/.ckman_sql_history`

### 6.2 单行 SQL，分号可选

```go
line = strings.TrimSpace(line)
line = strings.TrimSuffix(line, ";")
line = strings.TrimSpace(line)
if line == "" { continue }
```

不支持多行（不维护"未完成语句"状态机）。运维场景里复杂 SQL 应该写到文件用 `-q "$(cat foo.sql)"` 或后续考虑 `.read file.sql`（本次 YAGNI）。

### 6.3 退出

下列任一触发：
- 输入 `exit` / `quit`（大小写无关，无分号）
- `Ctrl-D` (EOF)
- `Ctrl-C` 两次（一次清除当前行，两次退出 —— readline 默认行为）

### 6.4 Meta 翻译表

仅三条，全部基于"输入首关键字大小写无关匹配"：

| 输入 | sqlite | mysql / dm8 | postgres |
|---|---|---|---|
| `SHOW DATABASES` | `PRAGMA database_list` | `SHOW DATABASES`（原生透传） | `SELECT datname FROM pg_database WHERE datistemplate = false` |
| `SHOW TABLES` | `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\_%'` | `SHOW TABLES`（原生透传） | `SELECT tablename FROM pg_tables WHERE schemaname='public'` |
| `DESC <tbl>` 或 `DESCRIBE <tbl>` | `PRAGMA table_info(<tbl>)` | `DESC <tbl>`（原生透传） | `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '<tbl>'` |

**实现：**

```go
// translate.go
type metaRule struct {
    Name      string                                       // "SHOW DATABASES" etc.
    Match     func(input string) (matched bool, tableName string)
    Translate map[string]string                            // backend → SQL template (with {tbl} placeholder for DESC)
}
```

匹配后整条 SQL 被替换为后端原生 SQL；没匹配就 raw 透传。

### 6.5 SQL 分发

执行前提取首关键字（trim 后第一个空白前的单词），判定是 read 还是 write：

```go
var readKeywords = map[string]bool{
    "SELECT": true, "PRAGMA": true, "EXPLAIN": true,
    "WITH":   true, "SHOW": true, "DESC": true, "DESCRIBE": true,
}
```

- read 关键字 → `db.Raw(sql).Rows()` → 走行集渲染
- 其他（INSERT / UPDATE / DELETE / CREATE / DROP / ALTER / BEGIN / COMMIT / ...）→ `db.Exec(sql)` → 输出 `N rows affected (Xs)`

事务由用户用 SQL 显式控制（`BEGIN; ...; COMMIT;`），客户端不参与。

## 7. 输出渲染

### 7.1 ASCII 表格（默认）

用 `text/tabwriter` 标准库 + 自绘 `+--+--+` 分隔线：

```
+--------------+-----------------+------------------------+
| cluster_name | comment         | created_at             |
+--------------+-----------------+------------------------+
| ck1          | prod-shanghai   | 2026-05-16 10:30:45    |
| ck2          | (NULL)          | 2026-05-16 11:02:11    |
| ck3          | test-{"hosts... | 2026-05-16 11:48:03    |
+--------------+-----------------+------------------------+
3 rows in set (0.003s)
```

**值格式化规则：**

| 类型 | 渲染 |
|---|---|
| `nil` | `(NULL)`（与字符串 `"NULL"` 用引号区分） |
| `string` > 60 char | 截断 `...`，footer 提示加 `-N` 看完整 |
| `time.Time` | `2006-01-02 15:04:05` |
| `[]byte` | 当 UTF-8 字符串显示；非法 UTF-8 → hex |
| 数值 / bool | `fmt.Sprintf("%v", v)` |

**特殊场景：**
- 空结果集 → `Empty set (Xs)`，不画表头
- DML 结果 → `X rows affected (Xs)`

### 7.2 竖排 `-E` / `--vertical`

```
*************************** 1. row ***************************
cluster_name: ck1
comment:      prod-shanghai
created_at:   2026-05-16 10:30:45
config:       {"shards":[{"replicas":[...]}],"path":"/var/lib/clickhouse",...}
*************************** 2. row ***************************
cluster_name: ck2
...
```

按最长字段名右对齐冒号。值仍受截断规则约束。

### 7.3 JSON `--format=json`

NDJSON（每行一个对象）：

```
{"cluster_name":"ck1","comment":"prod-shanghai","created_at":"2026-05-16T10:30:45Z"}
{"cluster_name":"ck2","comment":null,"created_at":"2026-05-16T11:02:11Z"}
```

NULL → JSON `null`。时间字段 → RFC3339。`[]byte` → base64。不输出 footer。

### 7.4 CSV `--format=csv`

标准 RFC 4180：第一行表头；含逗号/换行/引号的值用双引号包围；内部双引号转义为 `""`。NULL → 空字符串。不输出 footer。

```csv
cluster_name,comment,created_at
ck1,prod-shanghai,2026-05-16T10:30:45Z
ck2,,2026-05-16T11:02:11Z
```

### 7.5 `--no-truncate` / `-N`

完整值不截断。**用户责任：** `config` 列可能含几 KB JSON，行将很宽 —— 建议配合 `-E` 使用。

## 8. 错误处理

**REPL 模式：**
- SQL 语法错误 / 约束违反 / 连接断开 → 打印 `ERROR: <message>` → 继续 REPL
- 连接彻底死了（多次 retry 失败） → 打印错误 + 退出码 2

**单次 `-q` 模式：**
- 任何错误 → 打印到 stderr → 退出码 1
- 成功 → 退出码 0

**启动期错误：**
- 找不到 ckman.hjson / 解析失败 → stderr + 退出码 1
- 后端连接失败（DSN 错、网络断、权限不足） → stderr + 退出码 1
- `persistent_policy` 未注册 / 不支持 → stderr + 退出码 1

## 9. 文件布局

```
cmd/sqlcli/
├── sqlcli.go         # Run() 入口（被 ckmanctl 调用）+ REPL 循环 + 单次模式
├── connect.go        # 后端 dispatch + DSN 复用 + gorm.Open
├── translate.go      # 三条 meta 翻译 + 首关键字提取
├── render.go         # table / vertical / json / csv 四种 Renderer
└── sqlcli_test.go    # 翻译 + 渲染 + 关键字分发的单测

repository/sqlite/config.go
repository/mysql/config.go
repository/postgres/config.go
repository/dm8/config.go
                      # 各 export BuildDSN(cfgMap map[string]interface{}) (driver, dsn string, err error)
                      # 把现有 Init() 里的 DSN 拼装抽出来，Init 改为调用 BuildDSN

cmd/ckmanctl/ckmanctl.go
                      # 注册 sqlCmd kingpin 子命令 + case "sql": sqlcli.Run(opts)

go.mod
                      # github.com/chzyer/readline v1.x.x（从间接提升为直接依赖）
```

## 10. 测试计划

### 10.1 单测（`cmd/sqlcli/sqlcli_test.go`）

| 用例 | 验证 |
|---|---|
| `TestTranslate_ShowDatabases_SQLite` | `SHOW DATABASES` → `PRAGMA database_list` |
| `TestTranslate_ShowTables_Postgres` | `SHOW TABLES` → `SELECT tablename FROM pg_tables WHERE schemaname='public'` |
| `TestTranslate_Desc_SQLite` | `DESC tbl_cluster` → `PRAGMA table_info(tbl_cluster)` |
| `TestTranslate_Desc_QuotedTable` | `DESC "weird name"` 也能解析出表名 |
| `TestTranslate_PassThrough` | `SELECT 1` 不被翻译 |
| `TestDispatch_ReadKeywords` | `SELECT/WITH/PRAGMA/EXPLAIN/SHOW/DESC` → read 路径 |
| `TestDispatch_WriteKeywords` | `INSERT/UPDATE/DELETE/CREATE/DROP/BEGIN` → write 路径 |
| `TestRender_Table_Null` | nil 值渲染为 `(NULL)` |
| `TestRender_Table_Truncate` | > 60 字符值被截断为 `...` |
| `TestRender_Vertical` | 多行竖排格式 |
| `TestRender_JSON_NullAndTime` | nil → `null`；time.Time → RFC3339 |
| `TestRender_CSV_Escaping` | 含逗号/引号/换行的值正确转义 |

### 10.2 集成测（`cmd/sqlcli/sqlcli_integration_test.go`）

| 用例 | 验证 |
|---|---|
| `TestE2E_SQLite_FreshDB` | 创建临时 sqlite db → 跑 `SHOW TABLES` → 命中预期表 |
| `TestE2E_SQLite_SelectInsert` | 插入 + 查询往返 |
| `TestE2E_SingleQueryMode` | `Run(opts{Query: "SELECT 1"})` 行为 |

MySQL / Postgres / DM8 后端的集成测留作 follow-up（需要外部 DB 实例）。

### 10.3 人工验收

1. 建 ckman.hjson 配 `persistent_policy: local`，跑 `ckmanctl sql`，验证：
   - REPL prompt 出现 `ckman> `
   - 上箭头能翻历史
   - `SHOW TABLES;` 列出 8 张表
   - `DESC tbl_cluster;` 列字段
   - `SELECT * FROM tbl_cluster LIMIT 1;` 输出表格
   - `UPDATE tbl_cluster SET comment='test'`（直接生效）
   - `exit` 退出，再启动看 `~/.ckman_sql_history` 有内容、上箭头能翻
2. 换 `persistent_policy: mysql`（如有可用 MySQL），同样验证
3. `ckmanctl sql -q "SELECT 1" --format=json` 验证管道输出

## 11. 依赖变更

`go.mod`：
- `github.com/chzyer/readline`（从间接提升为直接依赖；具体版本号在实施期取最新 stable）

无新增包；DSN 复用现有的 glebarez/sqlite、gorm.io/driver/mysql、gorm.io/driver/postgres、wanlay/gorm-dm8。

## 12. 风险与限制

| 风险 | 缓解 |
|---|---|
| 运维误操作（DROP TABLE / DELETE 无 WHERE） | 设计明确选择不护栏；运维 SOP 文档应包含"操作前先 dump-to-json 备份" |
| SQLite 与运行中 ckman 并发写 | WAL 模式 + busy_timeout=5000 兜底；偶发 "database is locked" 会向用户报错并保留 REPL |
| MySQL/Postgres/DM8 密码字段在 ckman.hjson 里明文存在 | 现状已如此（gsypt 解密后是明文）；与本次设计无关 |
| `DESC <tbl>` 表名包含反引号 / 引号 / 中文 | 简化实现只取首单词 + strip 引号；复杂场景用户自己写原生 SQL |
| 各后端 NULL / 时间 / boolean 在 row scan 时的类型差异 | render 层用 `sql.RawBytes` + `interface{}` 通用 path，按运行时类型分支处理 |

## 13. 已知限制（文档化）

- 不支持多行 SQL（必须一行写完）
- 不支持 ctrl-C 中断正在执行的长 SQL（一旦提交给 GORM 就跑完）
- 不维护"当前数据库"（`USE database` 在 MySQL 透传后影响后续语句，但客户端不显示这个状态）
- 不做 schema 验证（DROP TABLE / ALTER TABLE 直接生效，无确认提示）
