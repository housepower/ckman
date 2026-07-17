//go:build ckintegration

package clickhouse

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

// 真 ClickHouse 集成测试(build tag `ckintegration`,平时不参与构建)。
//
// 守的是 QueryInfo 的截断语义:它为防 ad-hoc 查询 OOM,把结果硬截断在 10000 行数据,
// 且**静默**截断——无 error、无日志,调用方无从察觉。GetCkTableMetrics 的两条查询
// (system.parts 按 (table,database) 分组、system.replicas 按表列出)行数都随表数增长,
// 表数超万时靠后的表指标恒为 0、读写状态恒为"正常"。这类 bug 只有连真库才抓得到。
//
// 跑法:
//
//	export CKTEST_HOST=127.0.0.1 CKTEST_PORT=9000 \
//	       CKTEST_USER=default CKTEST_PASS=xxx CKTEST_CLUSTER=latest
//	go test -tags ckintegration ./service/clickhouse/ -run TestQueryInfo -v
func newTestCkService(t *testing.T) *CkService {
	t.Helper()
	host := os.Getenv("CKTEST_HOST")
	if host == "" {
		t.Skip("CKTEST_HOST not set, skipping ClickHouse integration test")
	}
	port, err := strconv.Atoi(os.Getenv("CKTEST_PORT"))
	if err != nil {
		t.Fatalf("bad CKTEST_PORT: %v", err)
	}
	log.InitLoggerConsole()
	service := NewCkService(&model.CKManClickHouseConfig{
		Hosts:    []string{host},
		Port:     port,
		User:     os.Getenv("CKTEST_USER"),
		Password: os.Getenv("CKTEST_PASS"),
		Cluster:  os.Getenv("CKTEST_CLUSTER"),
	})
	if err := service.InitCkService(); err != nil {
		t.Fatalf("connect to ClickHouse failed: %v", err)
	}
	return service
}

// 返回值第 0 行是列名行,故数据行数 = len(value)-1。
func dataRows(value [][]interface{}) int { return len(value) - 1 }

// QueryInfo 的 10000 行上限是 35 个调用方共享的 OOM 保护,必须原样保留。
func TestQueryInfoStillTruncatesAt10000(t *testing.T) {
	service := newTestCkService(t)

	value, err := service.QueryInfo("SELECT number FROM numbers(20000)")
	if err != nil {
		t.Fatalf("QueryInfo failed: %v", err)
	}
	if got := dataRows(value); got != 10000 {
		t.Fatalf("QueryInfo data rows = %d, want 10000 (OOM 保护不能被改掉)", got)
	}
}

// 上限以内 QueryInfo 不应有任何行为变化。
func TestQueryInfoUnderLimitIsUnaffected(t *testing.T) {
	service := newTestCkService(t)

	value, err := service.QueryInfo("SELECT number FROM numbers(5)")
	if err != nil {
		t.Fatalf("QueryInfo failed: %v", err)
	}
	if got := dataRows(value); got != 5 {
		t.Fatalf("QueryInfo data rows = %d, want 5", got)
	}
}

// QueryInfoAll 不设上限——这是 GetCkTableMetrics 两条按表分组的查询所需要的。
func TestQueryInfoAllReturnsAllRows(t *testing.T) {
	service := newTestCkService(t)

	for _, want := range []int{5, 10000, 20000} {
		value, err := service.QueryInfoAll(fmt.Sprintf("SELECT number FROM numbers(%d)", want))
		if err != nil {
			t.Fatalf("QueryInfoAll(%d) failed: %v", want, err)
		}
		if got := dataRows(value); got != want {
			t.Fatalf("QueryInfoAll data rows = %d, want %d (超过 10000 行被截断了)", got, want)
		}
	}
}

// QueryInfoAll 必须与 QueryInfo 返回同样的结构:第 0 行是列名行,数据行列序一致。
func TestQueryInfoAllShapeMatchesQueryInfo(t *testing.T) {
	service := newTestCkService(t)
	const query = "SELECT number, toString(number) AS s FROM numbers(3)"

	limited, err := service.QueryInfo(query)
	if err != nil {
		t.Fatalf("QueryInfo failed: %v", err)
	}
	all, err := service.QueryInfoAll(query)
	if err != nil {
		t.Fatalf("QueryInfoAll failed: %v", err)
	}

	if len(limited) != len(all) {
		t.Fatalf("row count differs: QueryInfo=%d QueryInfoAll=%d", len(limited), len(all))
	}
	for i := range limited {
		if fmt.Sprint(limited[i]) != fmt.Sprint(all[i]) {
			t.Fatalf("row %d differs:\n QueryInfo    = %v\n QueryInfoAll = %v", i, limited[i], all[i])
		}
	}
}
