# 核心概念

阅读本节有助于理解 CKMAN 的术语和数据模型。

## 集群（Cluster）

CKMAN 操作的最小单位是 **ClickHouse 集群**，不是单个节点。一个集群包含一到多个分片，每个分片有一到多个副本。

| 字段 | 含义 |
| --- | --- |
| `cluster` | 集群名，CKMAN 内唯一 |
| `mode` | `deploy`（CKMAN 部署）或 `import`（外部导入） |
| `isReplica` | 是否启用副本，部署时确定后**不可更改** |
| `shards` | 分片列表，每个分片包含若干 replicas |
| `version` | ClickHouse 版本 |

### 集群模式

- **deploy 模式**：通过 CKMAN 完整部署的集群，可以执行升级、销毁、节点增删、rebalance、启停等运维操作
- **import 模式**：将已存在的集群导入 CKMAN 纳管，**只能查看与查询**，不能执行运维变更（即便集群本身有 SSH 权限）。该模式适合接管历史遗留集群

## 分片与副本（Shard & Replica）

```
集群
├── shard 1
│   ├── replica 1 (192.168.0.1)
│   └── replica 2 (192.168.0.2)
├── shard 2
│   ├── replica 1 (192.168.0.3)
│   └── replica 2 (192.168.0.4)
└── shard 3
    └── replica 1 (192.168.0.5)
```

- 副本之间通过 ZooKeeper 协调数据同步
- 同一分片下副本数可不一致（最后一个分片可少副本）
- 增加节点：选择目标 shard，若 shard 已存在则作为新副本加入，若 `shard = max+1` 则新增分片

## 逻辑集群（Logic Cluster）

逻辑集群是**多个物理集群**的聚合体，主要用于跨集群的分布式表。一个物理集群可加入一个逻辑集群，逻辑集群名称由用户指定。

- 普通分布式表只能跨同一物理集群的分片
- 逻辑分布式表（`dist_logic_*`）可以跨多个物理集群
- 创建/删除逻辑分布式表需要表在所有成员物理集群中都存在

## 表与分布式表

CKMAN 创建表时会同时创建：

1. **本地表**：根据 `distinct` 与 `isReplica` 选择引擎

   | distinct | isReplica | engine |
   | --- | --- | --- |
   | true | true | `ReplicatedReplacingMergeTree` |
   | true | false | `ReplacingMergeTree` |
   | false | true | `ReplicatedMergeTree` |
   | false | false | `MergeTree` |

2. **分布式表**：以 `dist_` 开头的 Distributed 表，前端 SQL 查询面向分布式表

## 任务（Task）

CKMAN 的耗时操作（部署、升级、销毁、节点增删、rebalance、归档、备份等）都通过任务系统异步执行。

| 状态 | 含义 |
| --- | --- |
| `pending` | 等待中 |
| `running` | 执行中 |
| `success` | 已成功 |
| `failed` | 已失败 |
| `stopped` | 已停止 |

只有 `pending` / `running` 状态的任务可以**停止**，已完成/已停止的任务可以**删除**。

## 角色权限

CKMAN 内置三种角色（RBAC）：

| 角色 | 权限范围 |
| --- | --- |
| **管理员（admin）** | 全部操作，包括集群部署/升级/销毁、节点管理、用户管理、配置变更 |
| **普通用户（ordinary）** | 在已有集群上做表 / 分区 / 备份 / DML 等运维操作，不可改集群拓扑、不能管理用户 |
| **游客（guest）** | 只读：集群信息、表结构、SQL 查询、监控指标、任务/备份列表 |

详细列表见[用户与角色权限](/features/users/overview)。

## 持久化策略

CKMAN 自身的元数据（集群配置、任务、查询历史等）存放在持久层，可选：

| 策略 | 适用场景 |
| --- | --- |
| `local` | 单实例部署，存到本地 SQLite（默认 `conf/clusters.db`） |
| `mysql` / `postgres` / `dm8` | 多实例部署或需要 HA 时使用 |

详见[持久化配置](/config/persistent)。
