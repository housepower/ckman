# persistent_config

CKMAN 自身元数据（集群配置、任务、查询历史、备份策略等）的持久化策略。

## 策略选择

通过 `server.persistent_policy` 选定：

| 策略 | 适用场景 | 是否支持多实例 |
| --- | --- | --- |
| `local` | 单实例部署 | 否（默认） |
| `mysql` | 生产环境，最常用 | 是 |
| `postgres` | 生产环境 | 是 |
| `dm8` | 生产环境（达梦数据库） | 是 |

::: warning 多实例必选数据库后端
多实例部署必须使用 `mysql`/`postgres`/`dm8`，`local` 策略下实例之间数据无法同步。
:::

## local

::: tip 后端已切换为 SQLite
从近期版本起，`local` 策略由 JSON / YAML 文件改为 **SQLite**（默认 `conf/clusters.db`）。配置项名字保持与旧版兼容——你不用改 `ckman.hjson`，CKMAN 启动时会**自动迁移**旧的 `clusters.json` / `clusters.yaml` 到 SQLite。
:::

```hjson
"persistent_config": {
  "local": {
    "config_dir": "/etc/ckman/conf",
    "config_file": "clusters"
  }
}
```

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `config_dir` | `<工作目录>/conf` | SQLite 文件存放目录 |
| `config_file` | `clusters` | SQLite 文件名（不含扩展名），最终落到 `<config_dir>/<config_file>.db` |
| `format` | `""` | **可选**，仅在启动时自动迁移用——为空时自动嗅探旧 `.json` / `.yaml`；填 `json` 或 `yaml` 可显式指定旧文件格式 |

### 自动迁移行为

CKMAN 启动时按以下顺序检查：

1. 如果 `<config_dir>/<config_file>.db` 已存在 → 直接使用，跳过迁移
2. 否则查找同目录下的 `<config_file>.json` 或 `<config_file>.yaml` → 找到则导入到新建的 SQLite
3. 旧文件**保留不删**（迁移完成后可以人工备份/删除）

### 导出为 legacy clusters.json

如果你出于兼容性需要把 SQLite 内容导回 `clusters.json`，使用 [`ckmanctl dump-to-json`](/reference/ckmanctl#dump-to-json)：

```bash
ckmanctl dump-to-json -c /etc/ckman/conf/ckman.hjson -o conf/clusters.json
```

适用场景：跨版本回退、离线分析。

## mysql

```hjson
"persistent_config": {
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "ENC(E310E892E56801CED9ED98AA177F18E6)",
    "database": "ckman_db"
  }
}
```

| 字段 | 说明 |
| --- | --- |
| `host` | MySQL 主机 |
| `port` | 端口，默认 `3306` |
| `user` | 连接用户 |
| `password` | 密码，建议用 `ENC()` 包裹密文 |
| `database` | 数据库名，必须**提前创建**且编码为 UTF-8 |

::: tip 表结构
mysql 策略下 CKMAN **启动时自动建表**，无需手工执行 SQL 脚本。
:::

## postgres

```hjson
"persistent_config": {
  "postgres": {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "ckman",
    "password": "ENC(...)",
    "database": "ckman_db"
  }
}
```

字段含义同 mysql，端口默认 `5432`。

::: warning 需手动建表
postgres 策略**需要手工执行** `dbscript/postgres.sql` 创建表结构，CKMAN 不会自动建表。
:::

## dm8

```hjson
"persistent_config": {
  "dm8": {
    "host": "127.0.0.1",
    "port": 5236,
    "user": "CKMAN",
    "password": "ENC(...)",
    "database": "CKMAN"
  }
}
```

::: tip 达梦用户
dm8 策略下需要提前创建好**用户**（数据库会自动建在该用户名下）。表结构由 CKMAN 自动创建。
:::

## 密码加密

获取密文：

```bash
ckman --encrypt 你的密码
# E310E892E56801CED9ED98AA177F18E6
```

写入配置：

```hjson
password: ENC(E310E892E56801CED9ED98AA177F18E6)
```

未用 `ENC()` 包裹则视为明文（不推荐生产环境）。

## 数据迁移

从 `local`（SQLite）切到 `mysql` / `postgres` / `dm8`，或在数据库后端之间互相迁移：

1. 停 CKMAN 服务
2. 备份当前持久层（`conf/clusters.db` 或对应数据库）
3. 用 [`ckmanctl migrate`](/reference/ckmanctl#migrate) 命令在新旧后端之间复制数据
4. 修改 `ckman.hjson` 的 `persistent_policy` 与 `persistent_config` 指向新后端
5. 重启 CKMAN

`ckmanctl migrate` 会读取一个独立的 `migrate.hjson`，同时连上源与目标，按集群配置 / 任务 / 备份策略等表逐条搬运。详细配置见 [ckmanctl 工具 > migrate](/reference/ckmanctl#migrate)。
