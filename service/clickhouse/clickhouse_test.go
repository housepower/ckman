package clickhouse

import (
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
