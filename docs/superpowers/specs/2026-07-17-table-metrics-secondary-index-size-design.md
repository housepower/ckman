# 表指标展示二级索引大小 — 设计

日期:2026-07-17
状态:已与用户确认

## 背景

ckman 目前**完全没有统计二级索引(skip index)大小**。

表指标的取数 SQL 在 `service/clickhouse/clickhouse.go:310`:

```sql
SELECT table, uniqExact(partition) AS partitions, count(*) AS parts,
       sum(data_compressed_bytes)   AS compressed,
       sum(data_uncompressed_bytes) AS uncompressed,
       sum(rows) AS rows, database
FROM cluster('<cluster>', system.parts)
WHERE (database in ('<dbs>')) AND (active = '1')
GROUP BY table, database
```

`system.parts` 的 `data_compressed_bytes` / `data_uncompressed_bytes` **只统计列数据文件**,
不含 skip index、mark 文件、primary.idx。因此页面上的「占用磁盘(压缩前/压缩后)」两列,
无论表上建了多少 bloom_filter / minmax / set 索引都不会有任何体现,用户无法判断
二级索引的空间占用,也无法定位索引膨胀。

ClickHouse 提供两个现成数据源:

| 数据源 | 粒度 | 字段 |
|---|---|---|
| `system.parts` | 每表汇总 | `secondary_indices_compressed_bytes`、`secondary_indices_uncompressed_bytes`、`secondary_indices_marks_bytes` |
| `system.data_skipping_indices` | 每个索引一行 | `name`、`type`、`expr`、`data_compressed_bytes`、`data_uncompressed_bytes`、`marks_bytes` |

本次采用第一条路:`system.parts` 那条 SQL 已经在查了,加两个 `sum()` 即可,
**不增加任何额外查询开销**。

## 口径(确认结论)

- 新增**两列**:索引压缩前大小、索引压缩后大小,与现有 uncompressed/compressed 对称。
- **不改**现有 compressed/uncompressed 的含义(不把索引累加进去),避免静默改变
  已有列的语义、让老用户困惑。
- **不统计 marks**:`secondary_indices_compressed_bytes` 不含索引的 mark 文件
  (那是 `secondary_indices_marks_bytes`),正如现有 `data_compressed_bytes` 也不含
  数据 mark 文件。两者口径一致,故不额外加 marks 列。

## 范围(确认结论)

只改**表指标的「表」tab**。不改物化视图 tab(vmMetrics,底表一般不建 skip index,
加了大概率全 0),不改分区详情(tablePartitions)。

## 改动一:model 新增字段(后端)

`model/ck_table.go` 的 `CkTableMetrics`,在 UnCompressed 之后、RWStatus 之前插入:

```go
type CkTableMetrics struct {
	Columns           uint64 `json:"columns"`
	Rows              uint64 `json:"rows"`
	Partitions        uint64 `json:"partitions"`
	Parts             uint64 `json:"parts"`
	Compressed        uint64 `json:"compressed"`
	UnCompressed      uint64 `json:"uncompressed"`
	IndexUnCompressed uint64 `json:"index_uncompressed"` // 新增
	IndexCompressed   uint64 `json:"index_compressed"`   // 新增
	RWStatus          bool   `json:"readwrite_status"`
}
```

## 改动二:SQL 追加两个 sum(后端)

`service/clickhouse/clickhouse.go` 的 `GetCkTableMetrics`:

新列**追加在 SELECT 末尾**(下标 7、8),保持 `database` 在下标 6 不变,
避免打乱现有的 `value[i][N]` 取值逻辑:

```sql
SELECT table, uniqExact(partition), count(*),
       sum(data_compressed_bytes), sum(data_uncompressed_bytes), sum(rows), database,
       sum(secondary_indices_uncompressed_bytes) AS index_uncompressed,
       sum(secondary_indices_compressed_bytes)   AS index_compressed
FROM cluster('<cluster>', system.parts) WHERE ... GROUP BY table, database
```

**版本门控**:仅当 `common.CompareClickHouseVersion(conf.Version, "20.4") >= 0` 时才拼这
两列;低版本走原 SQL,两字段保持 0。

门控的理由是 blast radius:整个表指标页共用这一条 SQL,老集群若没有这几列,
**整页直接报错**,而不是两列显示为空。三行代码换掉这个风险,值得。
(依据:ClickHouse 官方 `system.parts` 文档的示例输出里,该列对应 part 的
`modification_time` 为 2020-06,说明 20.x 时代已存在;取 20.4 为保守下界。)

**cols 过滤**:`index_compressed` / `index_uncompressed` 需要
- 加入 `found` 的判断条件(与 partitions/parts/compressed/uncompressed/rows 并列)
- 各自加赋值分支(`common.ArraySearch(...) || len(cols) == 0`)

## 改动三:swagger(后端)

`controller/clickhouse.go` 的 `GetTableMetric` 注释里 columns 参数的 default 值补上新列名,
之后跑 `swag init` 重新生成 `docs/`。

## 改动四:i18n(前端)

`ckman-fe/src/services/i18n.ts` 的 tables 命名空间,措辞对齐现有词条
(en 为 `Disk Space(uncompress)`,zh 为 `占用磁盘(压缩前)`):

- en:`'IndexUnCompressed': 'Index Size(uncompress)'`、`'IndexCompressed': 'Index Size(compress)'`
- zh:`'IndexUnCompressed': '二级索引(压缩前)'`、`'IndexCompressed': '二级索引(压缩后)'`

## 改动五:表格列(前端)

`ckman-fe/src/views/tables/components/tableMetrics.vue`,只改两处:

1. `columns` 计算属性的 table 分支,在 `compressed` 之后插入两列,
   prop 为 `index_uncompressed` / `index_compressed`,`sortable: true`,
   minWidth 与相邻的 compressed 列(200)保持一致。
2. `fetchData` 的 table 分支映射里透传:
   `index_uncompressed: values.index_uncompressed`、`index_compressed: values.index_compressed`。

**模板不需要动**:第 44 行 `column.property.endsWith('compressed')` 已经会自动套
`byteConvert`,两个新 prop 都以 `compressed` 结尾,天然命中。

## 最终表格形态

```
表名 | 列数 | 行数 | 分区 | Parts | 未压缩 | 压缩 | 索引未压缩 | 索引压缩 | 读写状态
---- | ---- | ---- | ---- | ----- | ------ | ---- | ---------- | -------- | --------
t1   |  22  | 1.3B |  192 |  512  | 51 GB  | 12GB |   1.2 GB   |  340 MB  |  TRUE
```

## 测试

- 本改动**不碰持久层**,不需要跑 `repository/dbtest/`。
- `service/clickhouse/clickhouse_test.go` 已有 metrics 相关测试,保持通过。
- 真实验证:对接真集群拉起页面,确认建有 skip index 的表两列非 0、无索引的表为 0,
  且列宽/排序/字节格式化正常。

## 提交路径

前端改动在 `../ckman-fe/`(即 `/data/root/go/src/github.com/housepower/ckman-fe/`)的
`main` 分支提交,**不在 `ckman/frontend` 子模块内编辑**。推送后回到 ckman:

```bash
cd frontend && git fetch && git checkout <new-sha> && cd .. && git add frontend && git commit
```
