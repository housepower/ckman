package backup

import "testing"

func TestIsDailyCompatible(t *testing.T) {
	cases := []struct {
		key  string
		want bool
	}{
		{"toYYYYMMDD(event_time)", true},
		// toDate 分区值是 '2026-06-04'(带横线),与扫描 SQL 的 YYYYMMDD 字符串比较永不匹配
		{"toDate(event_time)", false},
		{"formatDateTime(event_time, '%Y%m%d')", true},
		{"toYYYYMM(event_time)", false},
		{"toStartOfHour(event_time)", false},
		{"region", false},
		{"tuple()", false},
		{"", false},
		// 大小写不敏感
		{"TOYYYYMMDD(event_time)", true},
		{"todate(t)", false},
		// toDate32 与 toDate 同语义(分区值带横线),同样不兼容
		{"toDate32(event_time)", false},
		// toYYYYMMDDhhmmss 分区值是 14 位 YYYYMMDDhhmmss,与 8 位 YYYYMMDD 扫描永不匹配
		{"toYYYYMMDDhhmmss(event_time)", false},
	}
	for _, c := range cases {
		got := IsDailyCompatible(c.key)
		if got != c.want {
			t.Errorf("IsDailyCompatible(%q) = %v, want %v", c.key, got, c.want)
		}
	}
}

func TestPartitionFmt(t *testing.T) {
	cases := []struct {
		key  string
		want string
	}{
		{"toYYYYMMDD(event_time)", "day"},
		{"toDate(t)", "custom"},
		{"toDate32(t)", "custom"},
		{"toYYYYMMDDhhmmss(t)", "custom"},
		{"toYYYYMM(t)", "month"},
		{"toStartOfHour(t)", "hour"},
		{"region", "custom"},
		{"", "none"},
		{"tuple()", "none"},
	}
	for _, c := range cases {
		got := PartitionFmt(c.key)
		if got != c.want {
			t.Errorf("PartitionFmt(%q) = %s, want %s", c.key, got, c.want)
		}
	}
}
