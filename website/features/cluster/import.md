# 导入集群

将已经在运行的 ClickHouse 集群纳入 CKMAN 管理。

::: tip 适用场景
- 历史遗留集群，希望统一在 CKMAN 中查看
- 不希望 CKMAN 接管节点 SSH 与运维变更，仅作为只读管理面板
:::

## 前置条件

- 集群已经存在且健康
- CKMAN 能从网络上访问该集群的 TCP 端口与 ZK 端口
- 你知道集群的 CK 用户名、密码（一般不是 default）

## 操作步骤

主页点击 **Import a ClickHouse Cluster**：

![导入集群表单](/img/features/cluster/import-form.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/cluster/import-form.png（导入集群表单） -->

## 表单字段

| 字段 | 说明 |
| --- | --- |
| Cluster Name | 实际存在的集群名（与 ClickHouse 内 `system.clusters` 中一致），且不与 CKMAN 已有名重复 |
| ClickHouse Node IP | 节点 IP 列表，逗号分隔。理论上填一个即可，CKMAN 会从中随机挑一个查询 `system.clusters` 获取完整拓扑 |
| ClickHouse TCP Port | 默认 `9000` |
| Zookeeper Node List | ZK 节点列表 |
| ZooKeeper Port | 默认 `2181` |
| ZK Status Port | 默认 `8080`，需要 ZK ≥ 3.5.0 才能监控指标 |
| Cluster Username | CK 用户名 |
| Cluster Password | CK 密码（**导入模式可选**，但留空则部分功能不可用） |

## 导入模式的限制

::: warning import 模式只能查看
导入的集群 `mode = import`，**不能**进行：

- 修改集群配置
- rebalance（数据再均衡）
- 启停集群、启停节点
- 升级集群
- 增加 / 删除节点

可以做的事：

- 查看集群状态、节点状态
- 表管理（创建/删除/修改表，包括 DML、TTL、物化视图等）
- SQL 查询、分区管理
- 监控指标、慢查询、会话查看
- 备份与恢复
- 从 CKMAN 移除集群（不会真的销毁集群本身）
:::

## 从 import 转 deploy？

当前版本**不支持**直接转换。若想获得完整运维能力，可以：

1. 在 CKMAN 中删除导入的集群（不会动到 ClickHouse 节点）
2. 重新使用[部署集群](/features/cluster/deploy)流程，**勾选强制覆盖**

这会破坏当前 ClickHouse 上的数据，谨慎操作并提前做好备份。
