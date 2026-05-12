package storage

import (
	"fmt"
	"strings"
)

// backupSettings 生成 BACKUP TABLE … 末尾的 SETTINGS 子句。
//
// 对齐重构前的语义：
//   - compression_method 必须传给 CK，否则即使 policy 选了 zstd 等，
//     CK 也会用默认 compression（通常是 zstd）而不是用户选的那个。
//   - compression_level=6 是经验值，兼顾压缩率和速度。
//   - deduplicate_files=0 关闭备份内文件去重，保证 partition 之间互相独立，
//     便于 partition 级恢复。
//
// compression 为空时退化为不带 compression_method（让 CK 用默认），
// 但仍带 level / deduplicate，避免 silent 行为漂移。
func backupSettings(compression string) string {
	var parts []string
	if c := strings.TrimSpace(compression); c != "" {
		parts = append(parts, fmt.Sprintf("compression_method='%s'", strings.ReplaceAll(c, "'", "''")))
	}
	parts = append(parts, "compression_level=6", "deduplicate_files=0")
	return " SETTINGS " + strings.Join(parts, ", ")
}

// restoreSettings 生成 RESTORE TABLE … 末尾的 SETTINGS 子句。
// allow_non_empty_tables=true：允许 restore 到已存在且非空的表，否则 partition
// 级恢复无意义（表通常本来就有数据）。
func restoreSettings() string {
	return " SETTINGS allow_non_empty_tables=true"
}
