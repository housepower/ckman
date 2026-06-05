package backup

import (
	"strings"
	"testing"
	"time"
)

func TestValidateLocalPath(t *testing.T) {
	cases := []struct {
		path string
		ok   bool
	}{
		{"/data/backups", true},
		{"/var/lib/ckman/backup-1", true},
		{"backups", false},                  // 必须绝对路径
		{"", false},                         // 空路径
		{"/data/../etc/passwd", false},      // 含 ..
		{"/data//x", false},                 // 含 //
		{"/data/./x", false},               // 含 /./
		{"/data;rm -rf /", false},           // 含分号
		{"/data $(echo)", false},            // 含 $
		{"/data with space", false},         // 空格
		{strings.Repeat("/a", 200), false},  // 超长 (>256)
	}
	for _, c := range cases {
		err := ValidateLocalPath(c.path)
		if (err == nil) != c.ok {
			t.Errorf("ValidateLocalPath(%q): want ok=%v got err=%v", c.path, c.ok, err)
		}
	}
}

func TestValidateIdentifier(t *testing.T) {
	for _, name := range []string{"events_log", "dba", "user-action"} {
		if err := ValidateIdentifier(name); err != nil {
			t.Errorf("identifier %q should pass: %v", name, err)
		}
	}
	for _, name := range []string{"a`b", "a';drop", "", "-foo", "--drop"} {
		if err := ValidateIdentifier(name); err == nil {
			t.Errorf("identifier %q should reject", name)
		}
	}
}

func TestValidateCrontabMinInterval(t *testing.T) {
	// 间隔 >= 1h 通过（spec §7.3：crontab 最小间隔 1 小时）
	if err := ValidateCrontabMinInterval("0 */2 * * *"); err != nil {
		t.Errorf("every-2h should pass: %v", err)
	}
	if err := ValidateCrontabMinInterval("0 3 * * *"); err != nil {
		t.Errorf("daily should pass: %v", err)
	}
	// 恰好 1h 间隔（每小时整点）应 PASS
	if err := ValidateCrontabMinInterval("0 * * * *"); err != nil {
		t.Errorf("hourly should pass: %v", err)
	}
	// 列表形：8:00→9:00 间隔恰好 1h，按 spec (>= 1h) 应 PASS
	if err := ValidateCrontabMinInterval("0 8,9 * * *"); err != nil {
		t.Errorf("cron '0 8,9 * * *' min interval=1h should pass: %v", err)
	}
	if err := ValidateCrontabMinInterval("* * * * *"); err == nil {
		t.Error("every minute should reject")
	}
	if err := ValidateCrontabMinInterval("not-a-cron"); err == nil {
		t.Error("invalid cron should reject")
	}
	// 兼容 6 段 Quartz 风格（老架构迁移过来的格式，例如 "0 0 0 * * ?"）
	if err := ValidateCrontabMinInterval("0 0 0 * * ?"); err != nil {
		t.Errorf("6-field daily should pass: %v", err)
	}
	if err := ValidateCrontabMinInterval("0 0 */2 * * ?"); err != nil {
		t.Errorf("6-field every-2h should pass: %v", err)
	}
	if err := ValidateCrontabMinInterval("0 * * * * *"); err == nil {
		t.Error("6-field every-minute should reject")
	}
}

// 立即备份 + 滚动窗口反转(start_date > 今天−days_before)时本次必然空跑,应直接拒绝;
// 定时备份不拦:「从未来某天开始备份」合法,空窗期会被标 skipped(no_partitions)。
func TestValidateDailyRange_ImmediateInvertedWindow(t *testing.T) {
	today := time.Now().Format("20060102")
	yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")

	// 窗口 [今天, 昨天] 反转 → 拒绝
	if err := validateDailyRange("immediate", "incremental", "daily", today, "", "", 1); err == nil {
		t.Fatal("expected inverted window rejection for immediate")
	}
	// 窗口 [昨天, 昨天] 恰好一个分区 → 放行(回归:闭区间临界值)
	if err := validateDailyRange("immediate", "incremental", "daily", yesterday, "", "", 1); err != nil {
		t.Fatalf("boundary date should pass: %v", err)
	}
	// 定时备份未来 start_date → 放行
	if err := validateDailyRange("scheduled", "incremental", "daily", today, "", "", 1); err != nil {
		t.Fatalf("scheduled future start_date should pass: %v", err)
	}
	// start_date 为空 → 放行(回归)
	if err := validateDailyRange("immediate", "incremental", "daily", "", "", "", 1); err != nil {
		t.Fatalf("empty start_date should pass: %v", err)
	}
}
