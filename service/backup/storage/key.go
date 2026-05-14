package storage

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// keyPrefixSegRe 限制 Local storage 拼到 SSH 命令的 keyPrefix 每段只能由字母数字 + . _ - 组成。
// 覆盖现实输入：cluster name / partition (YYYYMMDD 或数字) / db.table / host (IP 或 hostname)。
// 拒绝任何 shell 元字符 (`;` `&` `|` `$` 反引号 空格 引号 等)，防止 rm -fr / find 命令被注入。
var keyPrefixSegRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// validateKeyPrefix 防止 Local storage 拼接路径时被注入 `..` / 绝对路径 / 危险字符。
// keyPrefix 由 JoinRunKey 受控生成，但加一层防御更稳。
func validateKeyPrefix(p string) error {
	if p == "" {
		return errors.New("keyPrefix empty")
	}
	if strings.HasPrefix(p, "/") {
		return errors.New("keyPrefix must not be absolute")
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." || seg == "." || seg == "" {
			return fmt.Errorf("keyPrefix has illegal segment: %q", seg)
		}
		if !keyPrefixSegRe.MatchString(seg) {
			return fmt.Errorf("keyPrefix segment %q contains forbidden characters", seg)
		}
	}
	return nil
}

// JoinRunKey 拼接 BackupRun 在 storage 后端中的 key 前缀。
//
// 结构：[storagePrefix/]<partition>/<db>.<table>/<host>
//
//   - storagePrefix 通常为 BackupRun.StoragePrefix（新 run 填 cluster 名；老 run 为空兼容老路径）
//   - storagePrefix 两侧 '/' 双向 Trim，避免拼出 "//" 或以 '/' 开头的 key（S3 会当作空段）
//
// CK 在 BACKUP/RESTORE TO/FROM S3 中读到的完整 key 是 "<endpoint>/<bucket>/<JoinRunKey 返回值>/data/..."，
// 因此本函数仅负责输出 key 内部段，不参与 endpoint/bucket 拼接。
func JoinRunKey(storagePrefix, partition, db, table, host string) string {
	base := fmt.Sprintf("%s/%s.%s/%s", partition, db, table, host)
	p := strings.Trim(storagePrefix, "/")
	if p == "" {
		return base
	}
	return p + "/" + base
}
