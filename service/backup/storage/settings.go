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
//
// 经验证 ClickHouse 部分版本（含我们目标环境）在 RESTORE TABLE PARTITION
// 语法后**不接受** SETTINGS 子句，会报 "Syntax error: failed at SETTINGS"。
// 而且 ckman 的恢复路径全部是 PARTITION 级（SubmitRestoreRequest 强制要求
// 指定 partitions），PARTITION 恢复语义就是「往表里追加分区」，本来就允许
// 表非空，无需 allow_non_empty_tables。
//
// 因此返回空串。如果将来需要全表 restore 且支持 SETTINGS 的版本，可以分支
// 出 restoreSettingsFull() 单独处理。
func restoreSettings() string {
	return ""
}
