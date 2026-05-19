# ckmanctl 命令行工具

`ckmanctl` 是与 `ckman` 一起发行的命令行运维工具，用于完成 Web 界面之外的低频但关键操作：持久层迁移、ZooKeeper 维护、Schema 升级、直连 SQL 查询等。

::: warning 仅给运维 / DBA 使用
ckmanctl 直接读写 ckman 的持久层和被管控集群的 ZooKeeper。**操作前请先备份**，并优先在测试环境演练。
:::

## 安装位置

| 安装方式 | ckmanctl 路径 |
| --- | --- |
| tar.gz | `<工作目录>/bin/ckmanctl` |
| rpm | `/usr/local/bin/ckmanctl` |
| 源码 | `cmd/ckmanctl/ckmanctl`（`make backend` 自动构建） |

日志默认写到 `/var/log/ckmanctl.log`。

## 全局帮助

```bash
ckmanctl --help
```

子命令列表：

| 子命令 | 作用 |
| --- | --- |
| `migrate` | 在不同持久层之间迁移集群配置 |
| `dump-to-json` | 把本地 SQLite 导出为 legacy `clusters.json` |
| `sql` | 持久层 SQL shell（交互或单次执行） |
| `get znodes` | 统计 ZooKeeper znode 数量 |
| `cleanup znodes suball` | 删除指定集群在 ZK 上的全部子节点 |
| `cleanup znodes queue` | 清理副本表的复制队列 |
| `set metacache` | 重建集群的 metacache |
| `upgrade backup` | 升级旧 Backup 表到新 BackupPolicy / BackupRun schema |

每个子命令都支持 `--help`：

```bash
ckmanctl migrate --help
ckmanctl cleanup znodes suball --help
```

---

## migrate

在不同持久层之间迁移 ckman 的集群配置。典型场景：

- 从 `local` 切到 `mysql` / `postgres` / `dm8`
- 从 `mysql` 切到 `postgres`
- 一份本地配置文件复制到另一份

```bash
ckmanctl migrate -c /etc/ckman/conf/migrate.hjson
```

### 配置文件

源 / 目标在一个独立的 `migrate.hjson` 中描述，与 `ckman.hjson` 解耦：

```hjson
{
  "source": "local2",     // 引用下面 persistent_config 中的某个 key
  "target": "mysql",
  "persistent_config": {
    "local1": {
      "policy": "local",
      "config": {
        "format": "yaml",
        "config_dir": "/etc/ckman/conf",
        "config_file": "ckman"
      }
    },
    "local2": {
      "policy": "local",
      "config": {
        "format": "json",
        "config_dir": "/etc/ckman/conf",
        "config_file": "clusters"
      }
    },
    "mysql": {
      "policy": "mysql",
      "config": {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "1qaz@WSX",
        "database": "ckman_migrate"
      }
    }
  }
}
```

| 字段 | 含义 |
| --- | --- |
| `source` | 数据从哪里读，键名引用 `persistent_config` 中的条目 |
| `target` | 数据写到哪里，同上 |

::: tip 关于 `local` 条目
新版本 CKMAN 的 `local` 策略已经是 SQLite（`<config_dir>/<config_file>.db`）。在 `migrate.hjson` 中：

- 不写 `format` → 自动识别新 SQLite 或老 `clusters.json` / `clusters.yaml`
- `format: "json"` / `format: "yaml"` → 显式从老格式读取，方便从历史数据迁出

target 端若为 `local`，总是写入 SQLite，无视 `format`。
:::

| `persistent_config.<key>.policy` | 该条目的持久化类型：`local` / `mysql` / `postgres` / `dm8` |
| `persistent_config.<key>.config` | 该类型对应的连接参数 |

### 执行流程

1. 解析配置文件
2. 同时连上 source 与 target
3. 读取 source 全部集群数据
4. 逐条写入 target
5. 任一步失败立即中断（不会写入半数据）

::: tip 安全建议
- 迁移前停掉 ckman 服务，避免源数据在迁移途中被改
- target 数据库**最好是空库**，避免与已有数据冲突
- 迁移成功后再启动 ckman，并把 `ckman.hjson` 的 `persistent_policy` 切到新后端
:::

---

## dump-to-json

把当前 SQLite（`local` 策略下的 `conf/clusters.db`）导出为兼容旧版的 `clusters.json` 文件。

```bash
ckmanctl dump-to-json -c /etc/ckman/conf/ckman.hjson -o conf/clusters.json
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-c, --conf` | `/etc/ckman/conf/ckman.hjson` | ckman 配置文件 |
| `-o, --output` | `conf/clusters.json` | 输出 JSON 路径 |

::: tip 应用场景
- **跨版本回退**：新版本 CKMAN 的 `local` 策略落到 SQLite，旧版本只认 `clusters.json`；导出后即可在旧版上加载
- **调试 / 离线分析**：把 SQLite 内容快速读成结构化 JSON
:::

::: warning 不是常规备份方式
日常备份直接复制 `conf/clusters.db` 即可（SQLite WAL 模式下也能安全在线复制）。`dump-to-json` 主要服务于版本兼容场景。
:::

---

## sql

直接对 ckman 持久层执行 SQL，支持交互式 shell 和单次执行。

### 交互模式

```bash
ckmanctl sql -c /etc/ckman/conf/ckman.hjson
```

进入提示符后可直接输入 SQL，支持历史回滚、`\q` 退出。

### 单次执行

```bash
ckmanctl sql \
  -c /etc/ckman/conf/ckman.hjson \
  -q "SELECT cluster_name, mode FROM tbl_cluster"
```

### 输出格式

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `--format` | `table` | `table` / `json` / `csv` |
| `-E, --vertical` | `false` | 行竖排（类似 mysql `\G`），列数多时更易读 |
| `-N, --no-truncate` | `false` | 不截断长字段 |

示例：

```bash
# 导出为 csv
ckmanctl sql -q "SELECT * FROM tbl_task" --format csv > tasks.csv

# 列多时用竖排查看单行
ckmanctl sql -q "SELECT * FROM tbl_cluster LIMIT 1" -E
```

::: warning 谨慎执行写操作
sql 子命令具有**完整读写权限**。请避免直接 `UPDATE` / `DELETE` ckman 表，可能破坏后台运行逻辑。生产排查建议只用 SELECT。
:::

---

## get znodes

统计 ZooKeeper 上指定路径的 znode 数量，便于评估 ZK 数据规模。

```bash
ckmanctl get znodes /clickhouse/tables/test_cluster -r -s 20 -h 127.0.0.1:2181
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `<path>` (位置参数) | `/clickhouse` | 起始路径 |
| `-h, --host` | `127.0.0.1:2181` | ZK 地址 |
| `-r, --recursive` | `false` | 递归统计子节点 |
| `-s, --sort` | — | 按子节点数量排序，输出前 N 个 |

示例输出（按数量排序，仅看 top 20）：

```
/clickhouse/tables/test/01/cdc_log_local           1234567
/clickhouse/tables/test/02/cdc_log_local           1234500
...
```

适用场景：**ZK 数据膨胀**排查——找出哪个表/集群占用最多 znode。

---

## cleanup znodes suball

删除指定集群在 ZK 上的**全部子节点**。

::: danger 不可逆操作
该操作会清掉该集群相关的复制元数据、分布式 DDL 记录、任务标记等。**集群必须先停服**，否则会导致数据状态混乱。

务必先用 `--dryrun` 演练一次。
:::

```bash
# dryrun：只打印会删什么，不真删
ckmanctl cleanup znodes suball test_cluster -c /etc/ckman/conf/ckman.hjson -d

# 真正执行
ckmanctl cleanup znodes suball test_cluster -c /etc/ckman/conf/ckman.hjson
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `<cluster>` (位置参数) | — | 集群名 |
| `-c, --conf` | `/etc/ckman/conf/ckman.hjson` | ckman 配置（用于读取该集群的 ZK 地址） |
| `-p, --path` | — | 自定义 znode 根路径（默认根据集群信息推断） |
| `-d, --dryrun` | `false` | 演练模式，不真删 |

适用场景：

- **彻底销毁集群后**清理残留 ZK 数据
- 重建集群前清空旧的副本元信息

---

## cleanup znodes queue

清理副本表的**复制队列**（`/clickhouse/tables/.../replicas/<host>/queue`）。

复制队列长期堆积会导致副本同步停滞、磁盘占用上涨。在确认队列项已经过期或失效时可以清理。

```bash
# dryrun
ckmanctl cleanup znodes queue test_cluster -c /etc/ckman/conf/ckman.hjson -d

# 执行（默认会话超时 300 秒）
ckmanctl cleanup znodes queue test_cluster -c /etc/ckman/conf/ckman.hjson -t 600
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `<cluster>` (位置参数) | — | 集群名 |
| `-c, --conf` | `/etc/ckman/conf/ckman.hjson` | ckman 配置 |
| `-t, --session_timeout` | `300` | ZK 会话超时（秒） |
| `-d, --dryrun` | `false` | 演练模式 |

::: warning 优先用 ClickHouse 内置命令
如果只是个别表的复制队列异常，建议先尝试 ClickHouse 的：

```sql
SYSTEM RESTART REPLICA <db>.<table>;
SYSTEM RESTORE REPLICA <db>.<table>;
```

`ckmanctl cleanup znodes queue` 适用于**整集群批量**场景。
:::

---

## set metacache

为指定集群重建 metacache（ckman 持久层中缓存的集群元数据，如表结构）。

```bash
ckmanctl set metacache test_cluster -c /etc/ckman/conf/ckman.hjson

# dryrun
ckmanctl set metacache test_cluster -d
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `<cluster>` (位置参数) | — | 集群名 |
| `-c, --conf` | `/etc/ckman/conf/ckman.hjson` | ckman 配置 |
| `-d, --dryrun` | `false` | 演练模式，只打印计划 |

适用场景：

- 大量后台改动 ClickHouse 表结构后，ckman 缓存仍是旧版本
- metacache 损坏导致 Web 界面表信息错乱

---

## upgrade backup

把旧版 `Backup` 表升级到新的 `BackupPolicy` / `BackupRun` 双表 schema。

```bash
# 演练
ckmanctl upgrade backup -c /etc/ckman/conf/ckman.hjson --dry-run

# 正式迁移
ckmanctl upgrade backup -c /etc/ckman/conf/ckman.hjson

# 详细日志 + 完成后清理旧表数据
ckmanctl upgrade backup -c /etc/ckman/conf/ckman.hjson -v --cleanup

# 新表非空时强制写入（默认会拒绝）
ckmanctl upgrade backup -c /etc/ckman/conf/ckman.hjson --force
```

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `-c, --conf` | `/etc/ckman/conf/ckman.hjson` | ckman 配置 |
| `--dry-run` | `false` | 演练，只打印映射计划，不写入 |
| `-v, --verbose` | `false` | 每行详细日志 |
| `--force` | `false` | 允许在新表已有数据时继续写入 |
| `--cleanup` | `false` | 迁移成功后删除旧 `Backup` 表中已迁移的行 |

::: tip 跨版本升级时需要
当你从带有旧 Backup schema 的 ckman 升级到新版本后，**必须**跑一次 `ckmanctl upgrade backup`，否则历史备份记录看不到。

操作顺序：

1. 停 ckman
2. 升级 ckman 包
3. `ckmanctl upgrade backup --dry-run` 看映射对不对
4. `ckmanctl upgrade backup` 正式迁移
5. 验证 ckman 界面备份历史正常显示
6. （可选）再跑一次 `--cleanup` 清理旧表
:::

---

## 退出码

| 退出码 | 含义 |
| --- | --- |
| `0` | 成功 |
| `1` | 参数错误或运行时错误（具体看 stderr） |

## 排错

### `parse config failed`

- 检查 `-c` 指定的 ckman.hjson 路径是否存在且可读
- 注意 HJSON 语法（允许注释、宽松逗号），但仍要求基本 JSON 结构正确

### `persistent_policy not set`

- ckman.hjson 中 `server.persistent_policy` 为空
- sql / upgrade backup 等子命令需要读取 ckman 持久层，必须有该字段

### ZK 连接失败

- 防火墙 / 网络
- 检查 ckman.hjson 里集群的 ZK 节点配置是否准确
- 用 `-h` 显式指定 ZK 地址绕过配置

### 迁移到 mysql 时表创建失败

- 确认目标数据库**编码为 UTF-8**
- 用户具备 `CREATE TABLE` 权限
- 数据库本身已存在（migrate 不会自动建库）
