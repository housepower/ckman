# 表指标展示二级索引大小 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 ckman 表指标页的「表」tab 新增「二级索引(压缩前)」「二级索引(压缩后)」两列,数据取自 `system.parts` 的 `secondary_indices_*` 字段。

**Architecture:** 复用 `GetCkTableMetrics` 已有的那条 `system.parts` 查询,在 SELECT 末尾追加两个 `sum()`(下标 7、8),不增加额外查询。把 SQL 构造与结果合并各抽成一个纯函数以便单测(照搬同文件里 `mergeCkTableMetricColumns` 的既有写法)。前端只加列定义、字段透传和 i18n 词条,模板无需改动。

**Tech Stack:** Go 1.24 / Gin(后端),Vue 2 + TypeScript + vxe-table(前端,位于独立仓库 `ckman-fe`)

**Spec:** `docs/superpowers/specs/2026-07-17-table-metrics-secondary-index-size-design.md`

## Global Constraints

- **口径**:新增两列,**不改**现有 `compressed` / `uncompressed` 的语义(不把索引累加进去)。
- **不统计 marks**:只取 `secondary_indices_compressed_bytes` / `secondary_indices_uncompressed_bytes`,不取 `secondary_indices_marks_bytes`。
- **版本门控**:仅当 `common.CompareClickHouseVersion(conf.Version, "20.4") >= 0` 才把索引列拼进 SQL;低版本走原 SQL,两字段保持 0。理由是整个表指标页共用这一条 SQL,老集群缺列会导致**整页报错**而非两列为空。
- **SQL 下标不许动**:新列一律追加在 SELECT 末尾,`database` 必须保持在下标 6。
- **范围**:只改表指标的「表」tab。不碰物化视图 tab(`vmMetrics`)、不碰分区详情(`tablePartitions.vue`)。
- **JSON 字段名**:`index_uncompressed`、`index_compressed`(前端 prop 同名——必须以 `compressed` 结尾,才能命中模板已有的 `byteConvert` 分支)。
- **前端仓库**:所有前端改动在 `/data/root/go/src/github.com/housepower/ckman-fe/` 的 `main` 分支提交,**绝不在 `ckman/frontend` 子模块内编辑**。
- **不碰持久层**,因此不需要跑 `repository/dbtest/`。
- 提交信息末尾附:`Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

| 文件 | 动作 | 职责 |
|---|---|---|
| `model/ck_table.go` | 修改 | `CkTableMetrics` 增加 `IndexUnCompressed` / `IndexCompressed` 两个字段 |
| `service/clickhouse/clickhouse.go` | 修改 | 新增纯函数 `buildTableMetricsPartsQuery`(SQL 构造 + 版本门控)与 `mergeCkTableMetricParts`(表驱动的结果合并);`GetCkTableMetrics` 改为调用两者 |
| `service/clickhouse/clickhouse_test.go` | 修改 | 两个纯函数的单测 |
| `controller/clickhouse.go` | 修改 | `GetTableMetric` 的 swagger 注释补新列名 |
| `docs/`(swagger 产物) | 重新生成 | `swag init` |
| `ckman-fe/src/services/i18n.ts` | 修改 | 中英文各 2 条词条 |
| `ckman-fe/src/views/tables/components/tableMetrics.vue` | 修改 | table tab 的列定义 + `fetchData` 字段透传 |

---

## Task 1: 后端 model 字段与 SQL 构造(含版本门控)

**Files:**
- Modify: `model/ck_table.go:205-216`
- Modify: `service/clickhouse/clickhouse.go`(在 `GetCkTableMetrics` 上方新增常量与函数)
- Test: `service/clickhouse/clickhouse_test.go`

**Interfaces:**
- Produces:
  - `model.CkTableMetrics.IndexUnCompressed uint64`(json `index_uncompressed`)
  - `model.CkTableMetrics.IndexCompressed uint64`(json `index_compressed`)
  - `func buildTableMetricsPartsQuery(cluster, dbs, version string) string`(包内私有)
  - `const tableMetricsIndexMinVersion = "20.4"`
- Consumes:
  - `common.CompareClickHouseVersion(v1, v2 string) int`(`common/ck.go:263`;v1>v2 返回 1,相等 0,小于 -1)

- [ ] **Step 1: 给 model 加两个字段**

修改 `model/ck_table.go` 的 `CkTableMetrics`,在 `UnCompressed` 之后、`RWStatus` 之前插入两行:

```go
type CkTableMetrics struct {
	Columns           uint64 `json:"columns"`
	Rows              uint64 `json:"rows"`
	Partitions        uint64 `json:"partitions"`
	Parts             uint64 `json:"parts"`
	Compressed        uint64 `json:"compressed"`
	UnCompressed      uint64 `json:"uncompressed"`
	IndexUnCompressed uint64 `json:"index_uncompressed"`
	IndexCompressed   uint64 `json:"index_compressed"`
	RWStatus          bool   `json:"readwrite_status"`
	// CompletedQueries uint64      `json:"completedQueries"`
	// FailedQueries    uint64      `json:"failedQueries"`
	// QueryCost        CkTableCost `json:"queryCost"`
}
```

- [ ] **Step 2: 写失败的测试**

在 `service/clickhouse/clickhouse_test.go` 末尾追加。注意该文件当前 import 只有 `testing` 和 `model`,需要补 `strings`:

```go
func TestBuildTableMetricsPartsQueryIncludesIndexColumns(t *testing.T) {
	query := buildTableMetricsPartsQuery("abc", "default','logs", "23.8.1.2992")

	if !strings.Contains(query, "sum(secondary_indices_uncompressed_bytes) AS index_uncompressed") {
		t.Fatalf("query missing index_uncompressed: %s", query)
	}
	if !strings.Contains(query, "sum(secondary_indices_compressed_bytes) AS index_compressed") {
		t.Fatalf("query missing index_compressed: %s", query)
	}
	// database 必须保持在下标 6,索引列一律追加在末尾
	if !strings.Contains(query, "sum(rows) AS rows, database, sum(secondary_indices_uncompressed_bytes)") {
		t.Fatalf("index columns must be appended after database: %s", query)
	}
	if !strings.Contains(query, "cluster('abc', system.parts)") {
		t.Fatalf("query missing cluster: %s", query)
	}
	if !strings.Contains(query, "database in ('default','logs')") {
		t.Fatalf("query missing dbs: %s", query)
	}
}

func TestBuildTableMetricsPartsQueryOmitsIndexColumnsOnOldVersion(t *testing.T) {
	query := buildTableMetricsPartsQuery("abc", "default", "20.3.8.53")

	if strings.Contains(query, "secondary_indices") {
		t.Fatalf("CH < 20.4 must not select secondary_indices columns: %s", query)
	}
	if !strings.Contains(query, "sum(data_compressed_bytes) AS compressed") {
		t.Fatalf("query lost base columns: %s", query)
	}
}
```

把 import 块改成:

```go
import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)
```

- [ ] **Step 3: 运行测试,确认失败**

Run: `go test ./service/clickhouse/ -run TestBuildTableMetricsPartsQuery -v`
Expected: 编译失败,`undefined: buildTableMetricsPartsQuery`

- [ ] **Step 4: 写实现**

在 `service/clickhouse/clickhouse.go` 的 `func GetCkTableMetrics(` 之前插入:

```go
// tableMetricsIndexMinVersion system.parts 的 secondary_indices_* 系列字段自 CH 20.4 起提供。
// 表指标整页共用下面这一条 SQL,低版本集群若拼进这两列,失败的是整个查询(整页报错),
// 而不是两列为空,故做版本门控。
const tableMetricsIndexMinVersion = "20.4"

// buildTableMetricsPartsQuery 构造表指标的 system.parts 查询。
//
// 列下标是稳定契约,mergeCkTableMetricParts 依赖它:
//
//	0=table 1=partitions 2=parts 3=compressed 4=uncompressed 5=rows 6=database
//	7=index_uncompressed 8=index_compressed  (仅 CH >= 20.4)
//
// 索引列一律追加在 SELECT 末尾,database 保持在下标 6 不动。
func buildTableMetricsPartsQuery(cluster, dbs, version string) string {
	selects := "table, uniqExact(partition) AS partitions, count(*) AS parts, sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS uncompressed, sum(rows) AS rows, database"
	if common.CompareClickHouseVersion(version, tableMetricsIndexMinVersion) >= 0 {
		selects += ", sum(secondary_indices_uncompressed_bytes) AS index_uncompressed, sum(secondary_indices_compressed_bytes) AS index_compressed"
	}
	return fmt.Sprintf("SELECT %s FROM cluster('%s', system.parts) WHERE (database in ('%s')) AND (active = '1') GROUP BY table, database;", selects, cluster, dbs)
}
```

- [ ] **Step 5: 运行测试,确认通过**

Run: `go test ./service/clickhouse/ -run TestBuildTableMetricsPartsQuery -v`
Expected: 两个测试均 PASS

- [ ] **Step 6: 提交**

```bash
cd /data/root/go/src/github.com/housepower/ckman
git add model/ck_table.go service/clickhouse/clickhouse.go service/clickhouse/clickhouse_test.go
git commit -F - <<'EOF'
feat(metrics): 表指标 model 增加二级索引大小字段与 SQL 构造

system.parts 的 data_compressed_bytes 只统计列数据,不含 skip index,
二级索引占用此前完全不可见。buildTableMetricsPartsQuery 在 CH >= 20.4
时追加 secondary_indices_* 两个 sum,索引列追加在 SELECT 末尾以保持
database 固定在下标 6。低版本走原 SQL,两字段留 0——整页共用这条 SQL,
缺列会导致整页报错而非两列为空。

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 2: 后端结果合并与接线

**Files:**
- Modify: `service/clickhouse/clickhouse.go:302-337`(`GetCkTableMetrics` 里 "get bytes, parts, rows" 那一段)
- Modify: `service/clickhouse/clickhouse.go`(新增 `mergeCkTableMetricParts`,放在 `mergeCkTableMetricColumns` 旁边)
- Test: `service/clickhouse/clickhouse_test.go`

**Interfaces:**
- Consumes(Task 1 产出):
  - `buildTableMetricsPartsQuery(cluster, dbs, version string) string`
  - `model.CkTableMetrics.IndexUnCompressed` / `.IndexCompressed`
- Consumes(既有):
  - `ckMetricUint64(v interface{}) (uint64, bool)`(`service/clickhouse/clickhouse.go:420`,把各种整型统一转 uint64,负数/未知类型返回 false)
  - `common.ArraySearch[T](target T, str_array []T) bool`(`common/util.go:288`)
  - `service.QueryInfo(query) ([][]interface{}, error)`(结果第 0 行是列名行)
  - `conf.Version string`(`model/deploy_ck.go:91`)
- Produces:
  - `var tableMetricsPartsFields []struct{ col string; idx int; set func(*model.CkTableMetrics, uint64) }`(字段名/下标/写回位置的单一事实源,合并与 `found` 判断共用)
  - `func mergeCkTableMetricParts(metrics map[string]*model.CkTableMetrics, value [][]interface{}, cols []string)`

- [ ] **Step 1: 写失败的测试**

在 `service/clickhouse/clickhouse_test.go` 末尾追加:

```go
func TestMergeCkTableMetricPartsFillsIndexSizes(t *testing.T) {
	metrics := map[string]*model.CkTableMetrics{
		"default.events": {},
	}

	mergeCkTableMetricParts(metrics, [][]interface{}{
		{"table", "partitions", "parts", "compressed", "uncompressed", "rows", "database", "index_uncompressed", "index_compressed"},
		{"events", uint64(3), uint64(12), uint64(1000), uint64(4000), uint64(500), "default", uint64(1200), uint64(340)},
	}, nil)

	m := metrics["default.events"]
	if m.Partitions != 3 || m.Parts != 12 || m.Compressed != 1000 || m.UnCompressed != 4000 || m.Rows != 500 {
		t.Fatalf("base metrics wrong: %+v", m)
	}
	if m.IndexUnCompressed != 1200 {
		t.Fatalf("IndexUnCompressed = %d, want 1200", m.IndexUnCompressed)
	}
	if m.IndexCompressed != 340 {
		t.Fatalf("IndexCompressed = %d, want 340", m.IndexCompressed)
	}
}

// CH < 20.4 时 buildTableMetricsPartsQuery 只返回 7 列,合并不能 panic,索引字段保持 0。
func TestMergeCkTableMetricPartsToleratesRowsWithoutIndexColumns(t *testing.T) {
	metrics := map[string]*model.CkTableMetrics{
		"default.events": {},
	}

	mergeCkTableMetricParts(metrics, [][]interface{}{
		{"table", "partitions", "parts", "compressed", "uncompressed", "rows", "database"},
		{"events", uint64(3), uint64(12), uint64(1000), uint64(4000), uint64(500), "default"},
	}, nil)

	m := metrics["default.events"]
	if m.Compressed != 1000 {
		t.Fatalf("Compressed = %d, want 1000", m.Compressed)
	}
	if m.IndexUnCompressed != 0 || m.IndexCompressed != 0 {
		t.Fatalf("index sizes must stay 0 on old CH, got %d/%d", m.IndexUnCompressed, m.IndexCompressed)
	}
}

// cols 显式指定时,只写请求的字段。
func TestMergeCkTableMetricPartsRespectsColsFilter(t *testing.T) {
	metrics := map[string]*model.CkTableMetrics{
		"default.events": {},
	}

	mergeCkTableMetricParts(metrics, [][]interface{}{
		{"table", "partitions", "parts", "compressed", "uncompressed", "rows", "database", "index_uncompressed", "index_compressed"},
		{"events", uint64(3), uint64(12), uint64(1000), uint64(4000), uint64(500), "default", uint64(1200), uint64(340)},
	}, []string{"index_compressed"})

	m := metrics["default.events"]
	if m.IndexCompressed != 340 {
		t.Fatalf("IndexCompressed = %d, want 340", m.IndexCompressed)
	}
	if m.Compressed != 0 || m.Rows != 0 || m.IndexUnCompressed != 0 {
		t.Fatalf("unrequested fields must stay 0: %+v", m)
	}
}

// metrics 里没有的表(非 MergeTree)直接跳过,不新建条目。
func TestMergeCkTableMetricPartsSkipsUnknownTable(t *testing.T) {
	metrics := map[string]*model.CkTableMetrics{}

	mergeCkTableMetricParts(metrics, [][]interface{}{
		{"table", "partitions", "parts", "compressed", "uncompressed", "rows", "database", "index_uncompressed", "index_compressed"},
		{"events", uint64(3), uint64(12), uint64(1000), uint64(4000), uint64(500), "default", uint64(1200), uint64(340)},
	}, nil)

	if len(metrics) != 0 {
		t.Fatalf("must not create entries for unknown tables, got %+v", metrics)
	}
}
```

- [ ] **Step 2: 运行测试,确认失败**

Run: `go test ./service/clickhouse/ -run TestMergeCkTableMetricParts -v`
Expected: 编译失败,`undefined: mergeCkTableMetricParts`

- [ ] **Step 3: 写实现**

在 `service/clickhouse/clickhouse.go` 里 `mergeCkTableMetricColumns` 函数之后插入:

```go
// tableMetricsPartsFields 描述 buildTableMetricsPartsQuery 结果里每个可选字段的
// cols 过滤名、列下标与写回位置。这里同时是 GetCkTableMetrics 判断"是否需要跑这条
// 查询"的依据,两处共用一张表,避免字段列表日后走样。
//
// index_* 两项下标 >= 7,只有 CH >= 20.4 的结果行才有,mergeCkTableMetricParts
// 按行长度跳过。
var tableMetricsPartsFields = []struct {
	col string
	idx int
	set func(*model.CkTableMetrics, uint64)
}{
	{"partitions", 1, func(m *model.CkTableMetrics, v uint64) { m.Partitions = v }},
	{"parts", 2, func(m *model.CkTableMetrics, v uint64) { m.Parts = v }},
	{"compressed", 3, func(m *model.CkTableMetrics, v uint64) { m.Compressed = v }},
	{"uncompressed", 4, func(m *model.CkTableMetrics, v uint64) { m.UnCompressed = v }},
	{"rows", 5, func(m *model.CkTableMetrics, v uint64) { m.Rows = v }},
	{"index_uncompressed", 7, func(m *model.CkTableMetrics, v uint64) { m.IndexUnCompressed = v }},
	{"index_compressed", 8, func(m *model.CkTableMetrics, v uint64) { m.IndexCompressed = v }},
}

// mergeCkTableMetricParts 把 buildTableMetricsPartsQuery 的结果写回 metrics。
//
// value[0] 是列名行,从 1 开始才是数据。列下标见 buildTableMetricsPartsQuery 的注释;
// CH < 20.4 时每行只有 7 列(无索引列),故按 len(row) 容错而非假定长度。
// cols 为空表示全取;否则只写 cols 里点名的字段。
func mergeCkTableMetricParts(metrics map[string]*model.CkTableMetrics, value [][]interface{}, cols []string) {
	all := len(cols) == 0
	for i := 1; i < len(value); i++ {
		row := value[i]
		if len(row) < 7 {
			continue
		}
		table, ok := row[0].(string)
		if !ok {
			continue
		}
		database, ok := row[6].(string)
		if !ok {
			continue
		}
		metric, ok := metrics[fmt.Sprintf("%s.%s", database, table)]
		if !ok {
			continue
		}
		for _, f := range tableMetricsPartsFields {
			if f.idx >= len(row) {
				continue // CH < 20.4:结果行没有索引列
			}
			if !all && !common.ArraySearch(f.col, cols) {
				continue
			}
			if v, ok := ckMetricUint64(row[f.idx]); ok {
				f.set(metric, v)
			}
		}
	}
}
```

- [ ] **Step 4: 运行测试,确认通过**

Run: `go test ./service/clickhouse/ -run TestMergeCkTableMetricParts -v`
Expected: 四个测试全部 PASS

- [ ] **Step 5: 把新函数接进 GetCkTableMetrics**

`service/clickhouse/clickhouse.go` 里,把 "get bytes, parts, rows" 整段(当前 302-337 行,从 `// get bytes, parts, rows` 到 `found` 的 `if` 块结束)整体替换为:

```go
	// get bytes, parts, rows
	found := len(cols) == 0
	for _, f := range tableMetricsPartsFields {
		if common.ArraySearch(f.col, cols) {
			found = true
			break
		}
	}
	if found {
		query = buildTableMetricsPartsQuery(conf.Cluster, dbs, conf.Version)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		mergeCkTableMetricParts(metrics, value, cols)
	}
```

`found` 复用 `tableMetricsPartsFields`,不再手抄一遍字段名——原来那串 `||` 就是漏加新字段的经典入口。

注意:`query` 和 `value` 两个变量在上方(292-293 行)已声明,下面的 is_readonly 块还要用,**不要**改成 `:=`。

- [ ] **Step 6: 跑全包测试 + 编译 + lint**

Run: `go build ./... && go test ./service/clickhouse/ ./model/ ./common/ -v 2>&1 | tail -30`
Expected: 编译通过,测试全 PASS,无 `declared and not used` 之类的报错

Run: `make lint`
Expected: 无新增告警

- [ ] **Step 7: 提交**

```bash
cd /data/root/go/src/github.com/housepower/ckman
git add service/clickhouse/clickhouse.go service/clickhouse/clickhouse_test.go
git commit -F - <<'EOF'
feat(metrics): 表指标返回二级索引压缩前/后大小

抽出 mergeCkTableMetricParts 承接 system.parts 结果合并,按行长度容错
(CH < 20.4 只有 7 列),并接受 cols 过滤。字段名/下标/写回位置收进
tableMetricsPartsFields 一张表,合并与 found 判断共用,免得再漏加字段。
取值改用既有的 ckMetricUint64,顺带去掉原来 value[i][N].(uint64)
在类型意外时会 panic 的隐患。

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 3: swagger 文档

**Files:**
- Modify: `controller/clickhouse.go:2172`(`GetTableMetric` 的 `@Param columns` 注释)
- Modify: `docs/swagger.json`、`docs/swagger.yaml`、`docs/docs.go`(由 `swag init` 生成,不手改)

**Interfaces:**
- Consumes(Task 1 产出):JSON 字段名 `index_uncompressed` / `index_compressed`
- Produces:无(纯文档)

- [ ] **Step 1: 改 swagger 注释**

`controller/clickhouse.go` 第 2172 行,把:

```go
// @Param columns query string false "return columns, if columns is empty, return all columns" default(columns,partitions,parts,compressed,uncompressed,is_readonly,queries,cost)
```

改为:

```go
// @Param columns query string false "return columns, if columns is empty, return all columns" default(columns,partitions,parts,compressed,uncompressed,index_compressed,index_uncompressed,is_readonly,queries,cost)
```

- [ ] **Step 2: 重新生成 swagger**

Run: `cd /data/root/go/src/github.com/housepower/ckman && swag init`
Expected: 无报错,`docs/swagger.json` / `docs/swagger.yaml` / `docs/docs.go` 被更新

- [ ] **Step 3: 确认改动只涉及本次字段**

Run: `git diff --stat docs/ controller/clickhouse.go`
Expected: 只有上述文件变更。若 `swag init` 顺带重排了大量无关内容,只保留本次相关的 hunk(`git add -p`),不要把无关噪声混进提交。

- [ ] **Step 4: 编译确认**

Run: `go build ./...`
Expected: 通过

- [ ] **Step 5: 提交**

```bash
cd /data/root/go/src/github.com/housepower/ckman
git add controller/clickhouse.go docs/
git commit -F - <<'EOF'
docs(swagger): table_metric 的 columns 参数补充二级索引列名

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 4: 前端表格列与 i18n

**Files:**
- Modify: `/data/root/go/src/github.com/housepower/ckman-fe/src/services/i18n.ts:246`(en)、`:1196`(zh)
- Modify: `/data/root/go/src/github.com/housepower/ckman-fe/src/views/tables/components/tableMetrics.vue`(`columns` 计算属性的 table 分支、`fetchData` 的 table 映射)

**Interfaces:**
- Consumes(Task 1/2 产出):接口 `GET /api/v2/ck/table_metric/{cluster}` 响应中每张表的 `index_uncompressed` / `index_compressed`(number)
- Produces:无(终端消费方)

- [ ] **Step 1: 确认 ckman-fe 工作区干净、在 main 分支**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe
git status --short && git branch --show-current
```
Expected: 无输出(干净)且分支为 `main`。若不干净或不在 main,**停下来问用户**,不要自行 stash 或切分支。

- [ ] **Step 2: 加 i18n 词条(英文)**

`src/services/i18n.ts` 第 246 行 `'Compressed': 'Disk Space(compress)',` 之后插入两行(措辞对齐同组既有词条):

```ts
      'UnCompressed': 'Disk Space(uncompress)',
      'Compressed': 'Disk Space(compress)',
      'IndexUnCompressed': 'Index Size(uncompress)',
      'IndexCompressed': 'Index Size(compress)',
      'Disk Space': 'Disk Space',
```

- [ ] **Step 3: 加 i18n 词条(中文)**

`src/services/i18n.ts` 第 1196 行 `'Compressed': '占用磁盘(压缩后)',` 之后插入两行:

```ts
      'UnCompressed': '占用磁盘(压缩前)',
      'Compressed': '占用磁盘(压缩后)',
      'IndexUnCompressed': '二级索引(压缩前)',
      'IndexCompressed': '二级索引(压缩后)',
      'Disk Space': '占用磁盘',
```

- [ ] **Step 4: 加表格列定义**

`src/views/tables/components/tableMetrics.vue`,`columns` 计算属性的 `if (this.activeTab === 'table')` 分支里,在 `compressed` 那一项(当前 166-171 行)之后、`readwrite_status` 之前插入两项:

```js
          {
            prop: "compressed",
            label: this.$t('tables.Compressed'),
            minWidth: 200,
            sortable: true
          },
          {
            prop: "index_uncompressed",
            label: this.$t('tables.IndexUnCompressed'),
            minWidth: 200,
            sortable: true
          },
          {
            prop: "index_compressed",
            label: this.$t('tables.IndexCompressed'),
            minWidth: 200,
            sortable: true
          },
          {
            prop: "readwrite_status",
            label: this.$t('tables.RWStatus'),
```

**不要动 `else` 分支**(那是物化视图 tab,本次不改)。

- [ ] **Step 5: 透传接口字段**

同文件 `fetchData` 里,`this.activeTab == 'table'` 的映射(当前 283-292 行)加两行:

```js
          return this.activeTab == 'table' ? {
            tableName: key,
            columns: values.columns,
            rows: values.rows,
            partitions: values.partitions,
            parts: values.parts,
            uncompressed: values.uncompressed,
            compressed: values.compressed,
            index_uncompressed: values.index_uncompressed,
            index_compressed: values.index_compressed,
            readwrite_status: values.readwrite_status.toString().toUpperCase(),
          } : {
```

**模板不需要改动**:第 44 行 `column.property.endsWith('compressed')` 会自动给这两个 prop 套 `byteConvert`——这正是 prop 必须以 `compressed` 结尾的原因。

- [ ] **Step 6: 构建与 lint**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe
make lint && make build
```
Expected: lint 无新增告警,build 成功

- [ ] **Step 7: 提交并推送**

```bash
cd /data/root/go/src/github.com/housepower/ckman-fe
git add src/services/i18n.ts src/views/tables/components/tableMetrics.vue
git commit -F - <<'EOF'
feat(tables): 表指标新增二级索引压缩前/后两列

后端已从 system.parts 的 secondary_indices_* 返回索引大小。prop 命名以
compressed 结尾,直接命中模板已有的 byteConvert 分支,模板无需改动。
仅表 tab,物化视图 tab 不加(底表一般不建 skip index)。

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
git push origin main
```

---

## Task 5: 更新子模块指针并端到端验证

**Files:**
- Modify: `ckman/frontend`(子模块指针)

**Interfaces:**
- Consumes:Task 4 推到 `ckman-fe` `main` 的 commit sha

- [ ] **Step 1: 更新子模块指针**

```bash
cd /data/root/go/src/github.com/housepower/ckman/frontend
git fetch
git checkout $(cd /data/root/go/src/github.com/housepower/ckman-fe && git rev-parse HEAD)
cd /data/root/go/src/github.com/housepower/ckman
git add frontend
```

- [ ] **Step 2: 整体构建**

Run: `cd /data/root/go/src/github.com/housepower/ckman && make build`
Expected: 前后端均构建成功,产出二进制

- [ ] **Step 3: 端到端验证(必做,不能只靠单测)**

拉起 ckman 连到真实 ClickHouse 集群,打开表指标页的「表」tab,逐项确认:

1. 「二级索引(压缩前)」「二级索引(压缩后)」两列出现在「占用磁盘(压缩后)」和「读写状态」之间。
2. 建有 skip index 的表两列**非 0**;没有二级索引的表显示 `0 B`。
3. 数值经过 `byteConvert` 格式化(形如 `340 MB`),不是裸字节数。
4. 两列点击表头可正常排序。
5. 用集群里某张有索引的表交叉核对(在 ClickHouse 上直接跑):

```sql
SELECT sum(secondary_indices_compressed_bytes)   AS index_compressed,
       sum(secondary_indices_uncompressed_bytes) AS index_uncompressed
FROM cluster('<cluster>', system.parts)
WHERE database = '<db>' AND table = '<tbl>' AND active = 1;
```
页面数值应与该结果一致。

6. 切到物化视图 tab,确认列没有多出来、页面正常。

若找不到有二级索引的表,先造一张验证:

```sql
CREATE TABLE default.idx_probe ON CLUSTER '<cluster>' (
  id UInt64, uid String,
  INDEX idx_uid uid TYPE bloom_filter GRANULARITY 1
) ENGINE = MergeTree ORDER BY id;
INSERT INTO default.idx_probe SELECT number, toString(number % 100000) FROM numbers(2000000);
```

验证完记得 `DROP TABLE default.idx_probe ON CLUSTER '<cluster>';`

- [ ] **Step 4: 提交子模块指针**

```bash
cd /data/root/go/src/github.com/housepower/ckman
git commit -F - <<'EOF'
chore(frontend): 更新子模块指针至表指标二级索引列

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## 已知的相邻问题(本次不修,仅记录)

`GetCkTableMetrics` 里这条 parts 查询走的是 `service.QueryInfo`,而 `QueryInfo` 为防 ad-hoc 查询 OOM 会把结果**硬截断在 10001 行**(见 `clickhouse_service.go` 的 `len(colData) >= 10001` break)。本查询按 `(table, database)` 分组,行数 = 全库全表数,理论上和 `getCkTableMetricColumns` 当年踩的是同一个坑——表数超万时,靠后的表所有指标(含新加的索引列)都会停在 0。

`getCkTableMetricColumns` 已经改用 `Conn.Query` + 全量 Scan 绕开了,但 parts 这条没有。属于既有缺陷,与本次需求无关,不在此修——**留给后续单独处理**。
