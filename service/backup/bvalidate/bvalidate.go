// Package bvalidate 提供备份路径与标识符的安全校验，
// 供 service/backup 与 service/backup/storage 共同使用而不产生循环依赖。
package bvalidate

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	pathRe       = regexp.MustCompile(`^/[a-zA-Z0-9_./\-]+$`)
	identifierRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_\-]*$`)
)

const maxLocalPathLen = 256

// ValidateLocalPath 拒绝注入面：要求绝对路径、白名单字符、不含 ..、长度上限。
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

// ValidateIdentifier 用于 database / table / 分区名等会被拼到 SQL 的字符串。
func ValidateIdentifier(s string) error {
	if s == "" {
		return errors.New("identifier empty")
	}
	if !identifierRe.MatchString(s) {
		return fmt.Errorf("identifier %q contains forbidden characters", s)
	}
	return nil
}
