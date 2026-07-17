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
