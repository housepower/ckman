# ClickHouse 真库集成测试

build tag `ckintegration`,平时不参与构建。跑法:

```bash
export CKTEST_HOST=127.0.0.1 CKTEST_PORT=9000 \
       CKTEST_USER=default CKTEST_PASS=xxx CKTEST_CLUSTER=latest

go test -tags ckintegration ./service/clickhouse/ -run TestQueryInfo -v
```

不设 `CKTEST_HOST` 则自动 Skip。测试全是只读查询(`system.numbers`),不写任何数据。

## 守的是什么

`QueryInfo` 为防 ad-hoc 查询 OOM,把结果**静默**截断在 10000 行数据 —— 无 error、
无日志,调用方完全无从察觉。这个语义有两面:

- **必须保留**:它是 35 个调用方共享的 OOM 保护。`TestQueryInfoStillTruncatesAt10000`
  守住这条,防止有人"顺手"把上限去掉。
- **必须能绕开**:行数随集群规模增长、且结果必须完整的查询不能用它。
  `QueryInfoAll` 是不截断的变体,`TestQueryInfoAllReturnsAllRows` 守住它真的不限量。

历史教训:`GetCkTableMetrics` 的两条查询(`system.parts` 按 (table, database) 分组、
`system.replicas` 按副本表列出)行数都等于表数。走 `QueryInfo` 时,表数超万的集群里
第 10000 名之后的表**指标恒为 0、读写状态恒为"正常"**,刷新多少次都是 0。
`getCkTableMetricColumns` 当年也踩过同一个坑(列数恒为 0),已单独改用 `Conn.Query`。

这类 bug 藏在真实驱动/DB 边界内,mock 单测一律抓不到 —— 只有连真库、且数据量真的
超过 1 万行才会暴露,所以才有了这套集成测试。
