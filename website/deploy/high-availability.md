# 高可用部署

CKMAN 支持多实例部署，避免单点故障。

## 架构

```
                ┌─────────┐
                │  Nacos  │
                └────┬────┘
       注册 / 选主   │   服务发现
         ┌──────────┼──────────┐
         ↓          ↓          ↓
   ┌─────────┐ ┌─────────┐ ┌─────────┐
   │ ckman 1 │ │ ckman 2 │ │ ckman 3 │
   └────┬────┘ └────┬────┘ └────┬────┘
        └───────────┼───────────┘
                    ↓
        ┌────────────────────────┐
        │ MySQL/PG/DM8 持久层    │
        └────────────────────────┘
```

每个 CKMAN 实例：

- 向 Nacos 注册并发送心跳
- 共用同一个持久层（mysql / postgres / dm8）
- 只有 **master 实例**执行定时任务，避免重复触发
- 用户登录任一实例都可以做集群操作

## 前置依赖

| 组件 | 要求 |
| --- | --- |
| Nacos | ≥ 1.4 |
| 数据库 | MySQL / PostgreSQL / DM8 之一，独立部署且**编码为 UTF-8** |
| 负载均衡 | 推荐 Nginx，将 `:80/8808` 反代到多个实例 |

## 步骤 1：准备持久层

以 PostgreSQL 为例：

```bash
# 1. 创建数据库
psql -U postgres -c "CREATE DATABASE ckman_db ENCODING 'UTF8';"

# 2. 执行建表脚本（仅 postgres 需要）
psql -U postgres -d ckman_db -f dbscript/postgres.sql
```

::: tip 数据库初始化差异
- **mysql / dm8**：CKMAN 启动时自动建表，仅需提前创建库
- **postgres**：需要手工执行 `dbscript/postgres.sql` 初始化表结构
:::

## 步骤 2：准备 Nacos

参考 [Nacos 官方文档](https://nacos.io/) 部署。生产环境推荐三节点集群。

## 步骤 3：配置每个 CKMAN 实例

每个实例使用**相同的** `ckman.hjson`，关键配置：

```hjson
{
  "server": {
    "port": 8808,
    "persistent_policy": "postgres"
  },
  "persistent_config": {
    "postgres": {
      "host": "10.0.0.10",
      "port": 5432,
      "user": "ckman",
      "password": "ENC(...)",
      "database": "ckman_db"
    }
  },
  "nacos": {
    "enabled": true,
    "hosts": ["10.0.0.20", "10.0.0.21", "10.0.0.22"],
    "port": 8848,
    "user_name": "nacos",
    "password": "ENC(...)",
    "group": "DEFAULT_GROUP",
    "data_id": "ckman"
  }
}
```

密码加密：

```bash
ckman --encrypt 你的密码
# 输出形如 E310E892E56801CED9ED98AA177F18E6
# 在配置中写成 password: ENC(E310E892E56801CED9ED98AA177F18E6)
```

环境变量也可以覆盖：

| 变量 | 作用 |
| --- | --- |
| `NACOS_HOST` | 逗号分隔的 `host:port` 列表，覆盖 hosts/port |
| `HOST_IP` | 强制指定本实例对外暴露的 IP |

## 步骤 4：依次启动

```bash
# 节点 1
systemctl start ckman

# 节点 2 / 3 同样操作
```

启动后通过 `/api/v1/instances` 查看实例列表：

```bash
curl http://10.0.0.1:8808/api/v1/instances -H "token: ..."
```

## 步骤 5：负载均衡

Nginx 示例：

```nginx
upstream ckman_backend {
  server 10.0.0.1:8808;
  server 10.0.0.2:8808;
  server 10.0.0.3:8808;
}

server {
  listen 80;
  server_name ckman.example.com;

  location / {
    proxy_pass http://ckman_backend;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
  }
}
```

::: warning JWT 与客户端 IP
CKMAN 的 JWT 与客户端 IP 绑定。通过反代时务必设置 `X-Forwarded-For`，否则换实例后会被强制要求重新登录。
:::

## Master 选举与定时任务

- master 选举由 Nacos 协调，无需人工干预
- master 故障后其他节点会接管，约 1 个心跳周期内完成切换（默认 `beat_interval = 5000ms`）
- `config.IsMasterNode()` 在代码层判断当前是否 master，cron 任务只在 master 上执行

## 常见问题

**Q：实例之间数据不同步？**
A：检查是否所有实例都连接同一个 mysql/postgres/dm8 实例，并且 `persistent_policy` 一致。

**Q：cron 任务在多个实例上重复触发？**
A：检查 Nacos 注册是否成功（看启动日志），未注册成功的实例会退化为单机模式，所有 cron 都跑。
