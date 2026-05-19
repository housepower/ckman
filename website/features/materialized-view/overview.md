# 物化视图管理

## 创建 / 删除物化视图

通过 **物化视图** 页面创建或删除物化视图。

![物化视图](/img/features/mv/list.png)
<!-- TODO(screenshot): 当前为占位图，请用实际截图替换 /img/features/mv/list.png（物化视图列表） -->

## 状态查看

查看集群中物化视图的运行状态：

- 是否正常追上源表
- 最近的执行时间
- 失败次数

## groupUniqArray 聚合表

CKMAN 提供基于 `groupUniqArray` 函数的聚合表能力，用于高效统计某个维度上的去重值集合。

接口：

| 接口 | 功能 |
| --- | --- |
| `POST /api/v1/ck/table/group_uniq_array/:cluster` | 创建聚合表 |
| `GET /api/v1/ck/table/group_uniq_array/:cluster` | 查询聚合表 |
| `DELETE /api/v1/ck/table/group_uniq_array/:cluster` | 删除聚合表 |

## 何时用物化视图

- 实时聚合（按小时 / 按天的指标）
- 预计算热路径数据（避免重复扫描原始表）
- 不同维度的去重 / 去重计数

## 注意事项

::: warning 物化视图维护成本
物化视图相当于**对原表增量插入触发器**。原表写入越频繁，物化视图维护开销越大。

- 控制物化视图数量，避免每个表挂太多
- 物化视图的 SELECT 应当只涉及当前 INSERT 的行（不要依赖历史数据，否则结果可能不对）
- 主备复制：复制表的物化视图建议使用 `ReplicatedMergeTree` 系列引擎
:::
