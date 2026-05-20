# 分区管理

对 ClickHouse 表的分区进行查看与操作。

## 查看分区

通过 **分区管理** 页面查看指定表的分区信息：

| 字段 | 说明 |
| --- | --- |
| Partition | 分区名 / partition expression value |
| Rows | 分区行数 |
| Disk Space | 该分区落盘大小 |
| Min Date / Max Date | 数据时间范围 |
| Parts | parts 数量 |

支持同时查看**多张表**的分区信息（多选）。

![分区列表](/img/features/partition/list.png)

## 删除分区

按分区 ID 删除（`ALTER TABLE ... DROP PARTITION`）。

::: danger 删除分区不可逆
分区被删除后数据不可恢复。删除前请确认：
- 数据是否已备份
- 业务侧是否已下线相关查询
:::

## 冻结 / 解冻

支持 `FREEZE` 与 `UNFREEZE` 操作：

| 操作 | 用途 |
| --- | --- |
| **FREEZE** | 创建分区数据的硬链接快照到 `shadow` 目录，用于备份 |
| **UNFREEZE** | 删除冻结的快照 |

::: tip 冻结不影响读写
FREEZE 是基于硬链接的瞬时操作，原表仍可正常读写。冻结后的数据需要后续手工/脚本搬运到备份目标。

如果想要更完整的备份能力，直接用[数据备份](/features/backup/overview)。
:::

## 操作分区接口

CKMAN 提供以下接口：

| 接口 | 功能 |
| --- | --- |
| `GET /api/v1/ck/partition/:cluster` | 单表分区列表 |
| `POST /api/v1/ck/partition/:cluster` | 多表分区列表 |
| `PUT /api/v1/ck/partition/:cluster` | 操作分区（FREEZE / UNFREEZE） |
| `DELETE /api/v1/ck/partition/:cluster` | 删除分区 |

详细参数见[API 接口](/reference/api)。
