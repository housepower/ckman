package backup

import (
	"fmt"
	"time"

	"github.com/housepower/ckman/service/backup/bvalidate"
	"github.com/robfig/cron/v3"
)

// ValidateLocalPath 拒绝注入面：要求绝对路径、白名单字符、不含 ..、长度上限。
// 委托给 bvalidate 子包，避免 storage 子包循环依赖。
func ValidateLocalPath(p string) error { return bvalidate.ValidateLocalPath(p) }

// ValidateIdentifier 用于 database / table / 分区名等会被拼到 SQL 的字符串。
func ValidateIdentifier(s string) error { return bvalidate.ValidateIdentifier(s) }

// ValidateCrontabMinInterval 解析 cron，检查接下来连续 24 次触发中任一相邻间隔 >= 1 小时。
//
// 兼容两种格式：
//   - 5 段：minute hour dom month dow（新建任务）
//   - 6 段：second minute hour dom month dow（老架构迁移过来的 Quartz 风格，
//     例如 "0 0 0 * * ?"；service/cron 实际用 cron.WithSeconds() 6 段调度）
func ValidateCrontabMinInterval(spec string) error {
	parser5 := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parser6 := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser5.Parse(spec)
	if err != nil {
		var err6 error
		sched, err6 = parser6.Parse(spec)
		if err6 != nil {
			return fmt.Errorf("invalid cron: %w", err)
		}
	}
	const checkCount = 24
	prev := sched.Next(time.Now())
	for range checkCount {
		next := sched.Next(prev)
		if next.Sub(prev) < time.Hour {
			return fmt.Errorf("crontab interval too small (%s < 1h)", next.Sub(prev))
		}
		prev = next
	}
	return nil
}

// NextRunAfter 解析 crontab（兼容 5/6 段），返回 after 之后的下一次触发时间。
// 与 ValidateCrontabMinInterval 共用一套 parser 选项，保证「能保存的 cron」就能算出下次时间。
func NextRunAfter(spec string, after time.Time) (time.Time, error) {
	parser5 := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	parser6 := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser5.Parse(spec)
	if err != nil {
		var err6 error
		sched, err6 = parser6.Parse(spec)
		if err6 != nil {
			return time.Time{}, fmt.Errorf("invalid cron: %w", err)
		}
	}
	return sched.Next(after), nil
}
