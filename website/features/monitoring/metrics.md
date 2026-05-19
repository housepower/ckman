# 监控指标

CKMAN 的 Overview 监控面板**默认直读 ClickHouse 系统表**（`system.metric_log` / `system.asynchronous_metric_log`），开箱即用，无需额外组件。

如果你已经有 Prometheus 生态、需要做长期指标存储或告警，可以把 CKMAN 接入 Prometheus 作为**可选增强**。

::: tip 默认能看监控
导入或部署集群后，进入"监控管理"页面即可看到指标。**不需要**先搭 Prometheus、node_exporter、ZooKeeper metrics 这些组件。
:::

## 数据来源

| 来源 | 用途 | 是否默认开启 |
| --- | --- | --- |
| `system.metric_log` | 实时性能（QPS、读写字节、合并、复制等） | ClickHouse 21.x+ 默认开启 |
| `system.asynchronous_metric_log` | 异步采样指标（CPU、内存、磁盘等） | ClickHouse 21.x+ 默认开启 |

底层接口：`GET /api/v1/metric/query_metric/:cluster?metric=<name>&start=<ts>&end=<ts>&step=<sec>`

CKMAN 会对每个节点单独查询，结果聚合后返回前端绘图。某个节点查询失败不会影响其他节点（v3.x 起 per-host 隔离）。

## 监控页面

![监控总览](/img/features/monitoring/overview.png)

## 指标分类

### ClickHouse Database KPIs

集群级聚合指标：

| 指标 | 说明 |
| --- | --- |
| `clickhouse.Query` | 集群分布式表查询次数随时间分布 |

### ClickHouse Node KPIs

节点级实时指标（来自 `metric_log` / `asynchronous_metric_log`）：

| 指标 | 说明 |
| --- | --- |
| `cpu usage` | CPU 占用率 |
| `memory usage` | 内存占用 |
| `disk usage` | 磁盘占用 |
| `IOPS` | IO 指标 |
| `Queries/second` | QPS |
| `Selected/Inserted Bytes/Rows` | 读写吞吐 |
| `Merged` / `Merged Rows` / `Merged Time` | 合并相关 |
| `Replicated Part Fetches` | 副本同步指标 |

### ZooKeeper KPIs

ZooKeeper 集群指标，来自 ZK 自身的 `mntr` 4 字命令（8080 端口）：

| 指标 | 说明 |
| --- | --- |
| `znode_count` | znode 总数 |
| `leader_uptime` | leader 存活时间 |
| `stale_sessions_expired` | 过期会话数 |
| `jvm_gc_collection_seconds_count` | JVM GC 次数 |
| `jvm_gc_collection_seconds_sum` | JVM GC 总耗时 |

::: warning ZK 版本要求
ZK 监控基于 v3.5.0+ 才支持的 `mntr` 8080 端口指标。低版本 ZK 这部分无数据。
:::

## 可选：接入 Prometheus

如果你希望：

- **长期保存**指标数据（CK 系统表只保留有限时长）
- 接入既有的 **Grafana / 告警** 体系
- 跨多个集群统一聚合

那么可以把 CKMAN、ClickHouse、ZooKeeper、节点都接入 Prometheus。

### 静态配置

```yaml
scrape_configs:
  - job_name: ckman_self
    static_configs:
      - targets: ['10.0.0.1:8808']

  - job_name: clickhouse
    static_configs:
      - targets: ['192.168.0.1:9363', '192.168.0.2:9363']

  - job_name: zookeeper
    static_configs:
      - targets: ['192.168.0.1:7000', '192.168.0.2:7000', '192.168.0.3:7000']

  - job_name: node
    static_configs:
      - targets: ['192.168.0.1:9100', '192.168.0.2:9100']
```

详细的 ClickHouse / ZooKeeper / node_exporter 端口配置见[安装 > 监控集成](/deploy/install#监控集成-可选)。

### HTTP 自动发现（推荐）

从 v2.3.5 起 CKMAN 提供 HTTP service discovery 接口，Prometheus 无需手工维护 targets 列表：

```yaml
- job_name: ckman_discovery
  http_sd_configs:
    - url: http://192.168.0.1:8808/discovery/node?cluster=abc
    - url: http://192.168.0.1:8808/discovery/zookeeper?cluster=test2
    - url: http://192.168.0.1:8808/discovery/clickhouse
```

| 端点 | 默认抓取端口 |
| --- | --- |
| `/discovery/node` | 9100（node_exporter） |
| `/discovery/zookeeper` | 7000（ZK Prometheus exporter） |
| `/discovery/clickhouse` | 9363（CK 内置 `<prometheus>` 端点） |

URL 参数 `cluster=<name>` 限定只发现该集群的目标；不带参数则发现 CKMAN 管理的全部集群。

### CKMAN 自身指标

CKMAN 默认在 `/metrics` 暴露 Prometheus 指标（路径可通过 `server.metric_path` 修改，开关 `server.metric: false` 关闭）。

## 何时选哪条路

| 场景 | 推荐方案 |
| --- | --- |
| 内部 / 测试 / 临时排障 | **直读 CK 系统表**（默认即用） |
| 单集群 + 短时回看 | **直读 CK 系统表** |
| 多集群统一告警 | 接入 **Prometheus + Grafana** |
| 监控数据需要保留 30+ 天 | 接入 **Prometheus**（CK 系统表默认 TTL 较短） |
| 已有完整可观测体系 | 接入 **Prometheus**，与现有体系一致 |
