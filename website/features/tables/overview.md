# 表 & 会话管理

CKMAN 提供多个面板查看集群中表的运行状态、会话与后台任务。

## Table Metrics

非 `system` 库下表的统计指标：

| 字段 | 说明 |
| --- | --- |
| Table Name | 表名 |
| Columns | 列数 |
| Rows | 行数 |
| Partitions | 当前所有未合并的分区数 |
| Parts Count | 分区数 |
| Disk Space (uncompress) | 未压缩占用 |
| Disk Space (compress) | 实际落盘占用 |
| RWStatus | 读写状态（`TRUE` 可读写 / `FALSE` 不可读写） |
| Completed Queries in last 24h | 24 小时内成功的查询数 |
| Failed Queries in last 24h | 24 小时内失败的查询数 |
| Queries cost (0.5, 0.99, max) in last 7days (ms) | 过去 7 天查询耗时 P50/P99/最大 |

![Table Metrics](/img/features/tables/metrics.png)

## Table Replication Status

按 shard 统计**复制表**在各副本间的状态对齐情况。

理论上同一 shard 内各副本同一张表的统计应该相等。如果不等，说明有副本落后——落后副本会**标黄**。

![Replication Status](/img/features/tables/replication.png)

::: tip 何时该担心
偶尔标黄是正常的（数据正在追赶）。如果某副本上**所有表**都标黄，说明该副本可能宕了。
:::

## Zookeeper Status

通过 ZooKeeper 暴露的 `8080` 端口（v3.5.0+）读取 `mntr` 指标。

可查看：版本、主从状态、平均延迟、近似数据总和大小、znode 数等。

![ZooKeeper Status](/img/features/tables/zk-status.png)

## Open Sessions

当前正在执行的查询。如果有正在执行的 SQL，可在界面上**直接 kill**。

![Open Sessions](/img/features/tables/open-sessions.png)

适用场景：

- 排查长时间运行的慢查询
- 紧急回收被某个查询占据的资源

## Slow Sessions

最近 7 天最慢的 10 条 SQL。

显示字段：

- 执行时间
- 耗时
- SQL 语句
- CK 用户
- query id
- 客户端 IP
- 线程号

![Slow Sessions](/img/features/tables/slow-sessions.png)

支持参数：`start`（默认 7 天前）、`end`（默认当前）、`limit`（默认 10）。

## Distributed DDL Queue

集群的分布式 DDL 队列状态。可看到正在执行或等待执行的 DDL 操作（CREATE / ALTER / DROP 等）。

排查"DDL 执行很慢"或"DDL 卡住"时先看这个面板。

## Table Merges

正在进行的表**合并（Merge）操作**状态。

观察：

- 哪些表在合并
- 每个节点上的合并进度（已合并行数 / 待合并行数）
- 资源占用

合并卡住可能引发磁盘占用激增、查询变慢，需要重点关注。

## Background Pool

ClickHouse 后台线程池状态。包括 `Merge`、`Mutation` 等后台任务的线程使用情况。

排查"后台任务延迟"问题时使用。如果线程池接近饱和（活动 / 总数 > 80%），考虑：

- 调整 `background_pool_size` 等服务端参数
- 减少同时进行的 mutation
- 增加机器资源
