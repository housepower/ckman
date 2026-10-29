package model

import (
	"os"
	"regexp"
	"testing"
)

// TestMetricMapCoversFrontendMetrics 前端 KPI 列表（frontend/src/constants/Metrics.ts）
// 里的每个指标名都必须能在 MetricMap 找到映射，否则 query_metric 接口返回
// E_INVALID_PARAMS，页面上该指标图无声无数据。
func TestMetricMapCoversFrontendMetrics(t *testing.T) {
	const metricsFile = "../frontend/src/constants/Metrics.ts"
	data, err := os.ReadFile(metricsFile)
	if os.IsNotExist(err) {
		t.Skipf("frontend submodule 未检出（%s 不存在），跳过", metricsFile)
	}
	if err != nil {
		t.Fatalf("read %s: %v", metricsFile, err)
	}
	names := regexp.MustCompile(`expect:\s*"([^"]+)"`).FindAllStringSubmatch(string(data), -1)
	if len(names) == 0 {
		t.Fatalf("%s 中未解析到任何指标名，正则可能已过时", metricsFile)
	}
	for _, n := range names {
		if _, ok := MetricMap[n[1]]; !ok {
			t.Errorf("前端指标 %q 在 MetricMap 中没有映射", n[1])
		}
	}
}
