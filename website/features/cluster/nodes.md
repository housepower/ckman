# 节点管理

对集群节点进行增加、删除、启停操作。仅 `mode = deploy` 的集群支持。

## 增加节点

集群管理页 → **Add Node**：

![增加节点](/img/features/cluster/add-node.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/cluster/add-node.png（增加节点对话框） -->

### 表单字段

| 字段 | 说明 |
| --- | --- |
| **New Node IP** | 新节点的 IP，可以一次性增加多个（多个节点会落在**同一个** shard 上） |
| **Node Shard** | 目标分片编号 |

### 分片编号规则

- 填**已有 shard**：新节点作为该 shard 的副本加入（**集群必须是副本模式**）
- 填**新 shard**：只能是 `max(shard) + 1`，不能跳号

非副本模式集群：

- 每个 shard 只能有一个节点
- **不允许**给已有 shard 添加副本
- 只能新增 shard

### 强制覆盖

同[部署集群](/features/cluster/deploy#强制覆盖)。如果目标节点上已有 ClickHouse 服务，勾选后销毁后重新部署。

## 删除节点

集群管理页 → 节点列表 → 删除按钮。

::: warning 删除不等于销毁
删除节点只会**停止该节点的 ClickHouse 服务**，并从 CKMAN 的持久层中移除。ClickHouse 软件**不会**被卸载。

如果要彻底清理节点，请先删除节点，再到该节点上手工卸载。
:::

### 删除规则

| 条件 | 允许删除？ |
| --- | --- |
| shard 有多个副本 | 允许 |
| shard 只剩 1 个节点，且该节点位于**最大 shard 编号** | 允许 |
| shard 只剩 1 个节点，且不是最大 shard | 不允许（会形成"中间空 shard"） |
| 删除会导致 shard 缩容且节点仍有数据 | 不允许（除非勾选**强制删除**） |

::: tip 节点有数据时
建议先用[数据均衡](#数据均衡--rebalance)把数据迁移到其他 shard，再删节点。

如果数据本身不要了，可勾选**强制删除**主动丢弃。
:::

## 启停节点

集群管理页节点列表中：

- **Start Node**：仅 `red` 状态的节点可启动
- **Stop Node**：仅 `green` 状态的节点可停止
- 节点状态颜色：`green` 正常，`red` 异常

启动操作通过 SSH 远程执行 `systemctl start clickhouse-server` 或对应 tar.gz 启动脚本。

## 启停集群

集群详情 → **Start Cluster** / **Stop Cluster**：

- 全集群启动：所有 `red` 节点都会被启动；如所有节点已是 `green`，按钮变灰
- 全集群停止：所有 `green` 节点都会被停止

## 数据均衡（Rebalance）

详细见数据均衡专题（待补）。简要说明：

- 支持**按 partition** rebalance（非副本模式需要 rsync + SSH 互信）
- 支持**按 shardingkey** rebalance（计算 hash → 落到目标 shard）
- 数据均衡前可调用 `rebalance_info` 接口预演
- 可选择是否清空最后一个 shard 数据（方便缩容）
