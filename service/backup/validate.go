package backup

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/housepower/ckman/model"
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

func validateDailyRange(scheduleType, backupStyle, backupType, startDate, rangeStartDate, rangeEndDate string, endDaysBefore int) error {
	if backupStyle != "incremental" || backupType != "daily" {
		return nil
	}
	startDate = strings.TrimSpace(startDate)
	rangeStartDate = strings.TrimSpace(rangeStartDate)
	rangeEndDate = strings.TrimSpace(rangeEndDate)

	hasFixedRange := rangeStartDate != "" || rangeEndDate != ""
	if hasFixedRange {
		if scheduleType == model.BACKUP_SCHEDULED {
			return errors.New("daily fixed range backup only supports immediate schedule_type")
		}
		if rangeStartDate == "" || rangeEndDate == "" {
			return errors.New("daily fixed range backup requires both range_start_date and range_end_date")
		}
		start, err := parseYYYYMMDD("range_start_date", rangeStartDate)
		if err != nil {
			return err
		}
		end, err := parseYYYYMMDD("range_end_date", rangeEndDate)
		if err != nil {
			return err
		}
		if start.After(end) {
			return fmt.Errorf("daily fixed range start must be <= end (%s > %s)", rangeStartDate, rangeEndDate)
		}
		return nil
	}

	if endDaysBefore <= 0 {
		return errors.New("daily backup requires days_before > 0")
	}
	if startDate != "" {
		start, err := parseYYYYMMDD("start_date", startDate)
		if err != nil {
			return err
		}
		// 立即备份:窗口 [start_date, 今天−days_before] 反转时本次必然空跑,直接拒绝。
		// 定时备份不拦——「从未来某天开始备份」是合法语义,空窗期 run 会标 skipped。
		if scheduleType != model.BACKUP_SCHEDULED {
			windowEnd := time.Now().AddDate(0, 0, -endDaysBefore).Format("20060102")
			if start.Format("20060102") > windowEnd {
				return fmt.Errorf("immediate daily backup window is empty: start_date %s is after today-%dd (%s)",
					startDate, endDaysBefore, windowEnd)
			}
		}
	}
	return nil
}

func parseYYYYMMDD(name, value string) (time.Time, error) {
	t, err := time.Parse("20060102", value)
	if err != nil {
		return time.Time{}, fmt.Errorf("daily backup %s must be YYYYMMDD: %w", name, err)
	}
	return t, nil
}
