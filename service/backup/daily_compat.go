package backup

import "regexp"

// 日级别（partition value 是 YYYYMMDD 格式）
// 注意:
//   - toDate/toDate32 不在内——分区值是 '2026-06-04'(带横线),与 YYYYMMDD 永不匹配。
//   - toYYYYMMDD 必须紧跟左括号(\s*\()以排除 toYYYYMMDDhhmmss——后者分区值是
//     14 位 YYYYMMDDhhmmss,与 8 位 YYYYMMDD 扫描同样不等价。
//
// 误判会让 daily 备份永远扫不到分区。
var dailyCompatibleRe = regexp.MustCompile(
	`(?i)\b(toYYYYMMDD\s*\(|formatDateTime\s*\([^,]+,\s*'%Y%m%d')`,
)

var (
	monthRe = regexp.MustCompile(`(?i)\btoYYYYMM\b`)
	hourRe  = regexp.MustCompile(`(?i)\btoStartOfHour\b|\btoHour\b`)
)

// IsDailyCompatible 判断 partition_key 表达式是否日级别（partition value 为 YYYYMMDD 字符串可比）。
// 用于 daily 模式提交前的校验。
func IsDailyCompatible(partitionKey string) bool {
	if partitionKey == "" || partitionKey == "tuple()" {
		return false
	}
	return dailyCompatibleRe.MatchString(partitionKey)
}

// PartitionFmt 把 partition_key 表达式归类：day / month / hour / custom / none。
// 仅用于前端展示，不影响调度决策。
func PartitionFmt(partitionKey string) string {
	if partitionKey == "" || partitionKey == "tuple()" {
		return "none"
	}
	if IsDailyCompatible(partitionKey) {
		return "day"
	}
	if monthRe.MatchString(partitionKey) {
		return "month"
	}
	if hourRe.MatchString(partitionKey) {
		return "hour"
	}
	return "custom"
}
