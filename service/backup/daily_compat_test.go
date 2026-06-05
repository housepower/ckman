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
