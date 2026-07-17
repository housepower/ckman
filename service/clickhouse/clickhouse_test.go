package clickhouse

import (
	"strings"
	"testing"

	"github.com/housepower/ckman/model"
)

func TestMergeCkTableMetricColumnsKeepsMaxColumnCount(t *testing.T) {
	metrics := map[string]*model.CkTableMetrics{
		"default.events": {},
	}

	mergeCkTableMetricColumns(metrics, [][]interface{}{
		{"table", "columns", "database"},
		{"events", uint64(0), "default"},
	})
	mergeCkTableMetricColumns(metrics, [][]interface{}{
		{"table", "columns", "database"},
		{"events", uint64(12), "default"},
	})
	mergeCkTableMetricColumns(metrics, [][]interface{}{
		{"table", "columns", "database"},
		{"events", uint64(8), "default"},
	})

	if got := metrics["default.events"].Columns; got != 12 {
		t.Fatalf("columns = %d, want 12", got)
	}
}

func TestMergeCkTableMetricColumnsAcceptsSignedCount(t *testing.T) {
	metrics := map[string]*model.CkTableMetrics{
		"default.events": {},
	}

	mergeCkTableMetricColumns(metrics, [][]interface{}{
		{"table", "columns", "database"},
		{"events", int64(7), "default"},
	})

	if got := metrics["default.events"].Columns; got != 7 {
		t.Fatalf("columns = %d, want 7", got)
	}
}

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
