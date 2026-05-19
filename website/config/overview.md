# 配置总览

CKMAN 的主配置文件为 `conf/ckman.hjson`（HJSON 格式，对人类友好），也支持 YAML。

::: tip HJSON
[HJSON](https://hjson.github.io/) 是 JSON 的扩展：支持注释、省略引号、宽松逗号。
:::

## 配置文件位置

| 安装方式 | 路径 |
| --- | --- |
| tar.gz | `<工作目录>/conf/ckman.hjson` |
| rpm | `/etc/ckman/conf/ckman.hjson` |

启动时也可通过 `-c` 指定：

```bash
ckman -c /path/to/ckman.hjson
```

## 配置结构

```hjson
{
  "server":            { ... },   // HTTP 服务、鉴权、metrics
  "clickhouse":        { ... },   // ClickHouse 连接池
  "log":               { ... },   // 日志级别与切割
  "cron":              { ... },   // 定时任务
  "persistent_config": { ... },   // 持久层（mysql/postgres/dm8/local）
  "nacos":             { ... }    // 服务发现（多实例时启用）
}
```

各小节配置项详见左侧侧边栏对应页面。

## 密码加密

任何密码字段都可以使用密文：

```bash
ckman --encrypt 123456
# 输出 E310E892E56801CED9ED98AA177F18E6
```

写入配置时用 `ENC()` 包裹：

```hjson
password: ENC(E310E892E56801CED9ED98AA177F18E6)
```

未包裹则视为明文。

## 完整示例

```hjson
// ckman 配置文件
// 密码可使用 ENC(密文)，密文通过 ./ckman --encrypt 明文 获取

{
  "server": {
    "port": 8808,
    "https": false,
    // certfile:
    // keyfile:
    "pprof": true,
    "session_timeout": 3600,
    "persistent_policy": "local",
    "task_interval": 5
    // public_key:
  },

  "log": {
    "level": "INFO",
    "max_count": 5,
    "max_size": 10,    // MB
    "max_age": 10      // day
  },

  "clickhouse": {
    "max_open_conns": 10,
    "max_idle_conns": 2,
    "conn_max_idle_time": 10
  },

  "cron": {
    "sync_logic_schema":    "0 * * * * ?",
    "watch_cluster_status": "0 */3 * * * ?",
    "sync_dist_schema":     "30 */10 * * * ?"
  },

  // 单机部署用 local 即可；多实例参见持久化配置页
  // "persistent_config": {
  //   "mysql": {
  //     "host": "127.0.0.1",
  //     "port": 3306,
  //     "user": "root",
  //     "password": "ENC(E310E892E56801CED9ED98AA177F18E6)",
  //     "database": "ckman_db"
  //   }
  // },

  "nacos": {
    "enabled": false,
    "hosts": ["127.0.0.1"],
    "port": 8848,
    "user_name": "nacos",
    "password": "ENC(A7561228101CB07938FAFF00C4444546)"
  }
}
```

## 修改后是否要重启

| 修改项 | 是否需要重启 |
| --- | --- |
| `server.port`, `https`, `certfile`, `keyfile` | 是 |
| `log.level` | 否（动态生效） |
| `cron.*` | 是 |
| `persistent_config`, `persistent_policy` | 是 |
| `nacos.*` | 是 |

::: tip 用户密码
登录用户的密码不在 `ckman.hjson` 中——它存放在 `conf/password` 或持久层数据库的用户表里，由 CKMAN 自己管理。
:::
