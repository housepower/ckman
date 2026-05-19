# Query 管理

在线执行 ClickHouse 查询，并查看查询计划、导出结果、回看历史。

![Query 管理界面](/img/features/query/console.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/query/console.png（SQL 查询界面） -->

## 基本用法

输入 SQL → 点击 **运行** → 结果以表格形式展示。

::: warning 仅支持查询
该工具**只能执行查询**，不能执行 mutation（INSERT/UPDATE/DELETE）。需要 DML 操作请使用[逻辑表 DML](#)。
:::

## 节点选择

默认情况下 CKMAN 随机挑一个节点执行 SQL，因此：

- 针对**分布式表**：结果一致
- 针对**本地表**：每次随机节点意味着每次返回的数据可能不一致

右上角下拉框可指定具体节点执行。

::: tip 本地表查询陷阱
本地表只在它所在的 shard 上有数据。如果你的查询面向本地表但 SQL 不带 shard 路由，结果可能不全。建议：
- 查全集群数据用**分布式表**（通常以 `dist_` 开头）
- 排查单节点数据用**本地表 + 节点选择**
:::

## 查询计划（EXPLAIN）

点击 **EXPLAIN** 按钮查看当前 SQL 的执行计划，用于：

- 确认是否走预期的索引（主键 / skipping index）
- 看分布式查询的步骤分解
- 优化查询性能

## 查询导出（CSV）

点击 **导出** 把结果集下载为 CSV 文件。

::: warning 数据量限制
导出会一次性把结果加载到内存。建议先用 `LIMIT` 或时间过滤限制结果集，避免内存溢出。
:::

## 查询历史

CKMAN 记录每次查询的 SQL、执行时间、耗时、用户。

- 点击历史记录可**复用** SQL
- 支持**删除**单条记录或批量清空（需 ordinary 或 admin 权限）

## 常见用法

### 查看分布式表数据

```sql
SELECT * FROM default.dist_tbtest LIMIT 100
```

### 查看表大小

```sql
SELECT
  database,
  table,
  formatReadableSize(sum(bytes)) AS size
FROM system.parts
WHERE active
GROUP BY database, table
ORDER BY sum(bytes) DESC
LIMIT 20
```

### 查看正在执行的查询

```sql
SELECT
  query_id,
  user,
  elapsed,
  formatReadableSize(memory_usage) AS mem,
  query
FROM system.processes
ORDER BY elapsed DESC
```

::: tip 也可以用 Open Sessions 面板
上面这个查询的可视化版本就是[Open Sessions](/features/tables/overview#open-sessions)，并且支持一键 kill。
:::
