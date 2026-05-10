package backup

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	pathRe       = regexp.MustCompile(`^/[a-zA-Z0-9_./\-]+$`)
	identifierRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_\-]*$`)
)

const maxLocalPathLen = 256

// ValidateLocalPath 拒绝注入面：要求绝对路径、白名单字符、不含 ..、长度上限
func ValidateLocalPath(p string) error {
	if len(p) == 0 || len(p) > maxLocalPathLen {
		return fmt.Errorf("local path empty or too long (>%d)", maxLocalPathLen)
	}
	if strings.Contains(p, "..") {
		return errors.New("local path may not contain '..'")
	}
	if strings.Contains(p, "//") {
		return errors.New("local path may not contain '//'")
	}
	if strings.Contains(p, "/./") {
		return errors.New("local path may not contain '/./'")
	}
	if !pathRe.MatchString(p) {
		return fmt.Errorf("local path %q contains forbidden characters", p)
	}
	return nil
}

// ValidateIdentifier 用于 database / table / 分区名等会被拼到 SQL 的字符串
func ValidateIdentifier(s string) error {
	if s == "" {
		return errors.New("identifier empty")
	}
	if !identifierRe.MatchString(s) {
		return fmt.Errorf("identifier %q contains forbidden characters", s)
	}
	return nil
}

// ValidateCrontabMinInterval 解析 cron，检查接下来连续 24 次触发中任一相邻间隔 >= 1 小时
func ValidateCrontabMinInterval(spec string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(spec)
	if err != nil {
		return fmt.Errorf("invalid cron: %w", err)
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
