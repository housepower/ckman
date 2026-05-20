# 节点日志

在 CKMAN 界面直接查看 ClickHouse 节点的日志文件，无需 SSH 登录每台节点。

![节点日志查看](/img/features/logs/viewer.png)

## 支持的日志类型

| 类型 | 默认路径 |
| --- | --- |
| 普通日志 | `/var/log/clickhouse-server/clickhouse-server.log` |
| 错误日志 | `/var/log/clickhouse-server/clickhouse-server.err.log` |

::: tip 路径依赖部署
具体路径取决于 ClickHouse 安装方式与配置。CKMAN 部署的集群按上述默认路径读取。
:::

## 操作步骤

集群管理页节点列表 → 任一节点 → **查看日志** → 选择日志类型 + 行数。

## 接口

`POST /api/v1/ck/node/log/:cluster`

参数：

- `host`：目标节点 IP
- `type`：日志类型（普通 / 错误）
- `lines`：返回的行数（从文件末尾向前读）

## 常见用途

- 排查 ClickHouse 启动失败
- 查看具体某条 SQL 执行时的服务端日志
- 跟踪副本同步异常
- 调查 Merge / Mutation 失败原因

## 与 Background Pool / DDL Queue 配合

排查"DDL 卡住"通常按以下顺序：

1. [Distributed DDL Queue](/features/tables/overview#distributed-ddl-queue) 看是哪个 DDL 卡住
2. [Background Pool](/features/tables/overview#background-pool) 看后台线程池是否饱和
3. **节点日志** 查看具体错误
4. [Open Sessions](/features/tables/overview#open-sessions) 看是否有长时间运行的查询占资源
