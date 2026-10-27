# 数据备份与恢复

CKMAN 提供完整的备份策略管理与恢复能力。

## 备份能力

| 能力 | 支持 |
| --- | --- |
| 备份目标 | 本地文件系统、S3 |
| 触发方式 | 立即备份、定时备份（cron 表达式） |
| 备份粒度 | 全量、分区级 |
| 压缩格式 | `gzip`, `brotli`, `xz`, `zstd`, `none` |
| 自动清理 | 按保留天数自动清理过期备份 |
| 备份历史 | 可查看每次备份的详情 |
| 运行控制 | 可停止排队中或执行中的备份、恢复 |

![备份策略列表](/img/features/backup/policy-list.png)

## 创建备份策略

**数据管理** → **新建备份策略**：

![新建策略](/img/features/backup/create-policy.png)

关键字段：

| 字段 | 说明 |
| --- | --- |
| 策略名 | 用于识别 |
| 集群 | 备份对象集群 |
| 数据库 / 表 | 备份范围 |
| 备份目标 | 本地 / S3 |
| 触发方式 | 立即一次性 / cron 定时 |
| 压缩格式 | 默认 gzip |
| 保留天数 | 超期自动清理 |
| 执行实例 | 多 ckman 实例部署时可指定哪个实例执行（避免跨区流量打满） |
| 按分区备份 | 选择表后可查看 active 分区及体量，并勾选要备份的分区 |
| 单分区读超时 | `max_read_seconds`，默认 6 小时，最多 24 小时；大分区备份可适当调高 |
| S3 上传限速 | `max_bandwidth_mb`，任务的集群总量（MB/s），按 shard 均分；`0` 不限速。非零值要求 ClickHouse ≥ 23.4 |

定时备份支持按日期滚动窗口；一次性备份也可指定固定日期范围。增量且按分区备份时，至少要选择一个分区。

## S3 备份配置

| 字段 | 说明 |
| --- | --- |
| Endpoint | S3 端点地址 |
| AccessKeyID | 访问 Key |
| SecretAccessKey | 访问 Secret |
| Region | S3 region |
| Bucket | 数据存放 bucket，不存在则自动创建 |
| Compression | 压缩格式（默认 gzip） |
| S3 上传限速 | 可选；按任务的集群总量设置，`0` 表示不限制 |

::: tip 压缩率参考
- `gzip` 对比 `none`：约 80 倍压缩率
- `gzip` 压缩入 S3 vs 原 CK 存储：约 2 倍压缩率（CK 自己已经做了 LZ4 压缩）
:::

## 备份产物路径

```
<bucket-or-path>/shard_%d_host/cluster/database.table/archive_table_slotbegin/data.suffix.compression
```

## 恢复数据

**数据管理** → **恢复**：

![恢复](/img/features/backup/restore.png)

选择历史备份记录后触发恢复任务。恢复过程：

1. CKMAN 从备份目标拉取数据
2. 写回目标集群对应表
3. 任务异步执行，可在[任务管理](/features/tasks/overview)跟踪进度

## 备份历史

每次执行（立即或定时）都会产生一条 run 记录。可查看：

- 开始 / 结束时间
- 耗时
- 备份大小
- 各 shard / 节点的状态
- 失败时的错误日志

## 停止备份或恢复

运行记录处于 `queued` 或 `running` 时可以停止。排队中的 run 会从队列移除；执行中的 run 会取消正在进行的操作，最终记录为 `interrupted`（原因 `cancelled`）。已完成的 run 不能停止。多实例部署时，应在该 run 所属的执行实例上操作。

停止不会清除已经写入备份目标的数据，也不会自动回滚已经恢复的数据。重新执行前先检查 run 的分区状态。

::: tip 删除备份
删除策略 → CKMAN **只删除自己的台账**，不会真的删 S3 上的数据。如果要清理 S3 数据，请用 S3 工具单独操作。
:::

## 增量去重

CKMAN 备份带有去重逻辑：连续的备份不会重复存储未变化的分区数据，节省存储成本。

## 备份相关接口

| 接口 | 功能 |
| --- | --- |
| `POST /data_manage/backup/:cluster` | 创建备份任务 |
| `GET /data_manage/backup/:cluster` | 备份策略列表 |
| `GET /data_manage/backup/policy/:policy_id/runs` | 指定策略的运行记录 |
| `GET /data_manage/backup/table/:cluster/:database/:table/partitions` | 表的可选分区及体量 |
| `POST /data_manage/backup/run/:run_id/stop` | 停止排队中或运行中的备份、恢复 |
| `DELETE /data_manage/backup/run/:run_id` | 删除单次备份记录 |
| `POST /data_manage/restore/:cluster` | 恢复数据 |
| `GET /data_manage/disks/:cluster` | 获取集群可用本地磁盘 |

详细参见[API 接口](/reference/api)。
