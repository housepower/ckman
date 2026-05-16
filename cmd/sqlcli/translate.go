package sqlcli

import (
	"strings"
)

// Translate 检查输入是否匹配 SHOW DATABASES / SHOW TABLES / DESC <tbl> 三条 meta；
// 命中则替换为对应后端的原生 SQL，否则原样返回。
//
// 输入约定：调用方已经 trim 过空白和末尾分号。
func Translate(sql string, backend Backend) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	switch {
	case upper == "SHOW DATABASES":
		return translateShowDatabases(backend)
	case upper == "SHOW TABLES":
		return translateShowTables(backend)
	case strings.HasPrefix(upper, "DESC "), strings.HasPrefix(upper, "DESCRIBE "):
		tbl := extractDescTarget(sql)
		if tbl == "" {
			return sql
		}
		return translateDesc(backend, tbl)
	}
	return sql
}

func translateShowDatabases(backend Backend) string {
	switch backend {
	case BackendSQLite:
		return "PRAGMA database_list"
	case BackendPostgres:
		return "SELECT datname FROM pg_database WHERE datistemplate = false"
	default: // MySQL / DM8 / Unknown
		return "SHOW DATABASES"
	}
}

func translateShowTables(backend Backend) string {
	switch backend {
	case BackendSQLite:
		return "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
	case BackendPostgres:
		return "SELECT tablename FROM pg_tables WHERE schemaname='public'"
	default:
		return "SHOW TABLES"
	}
}

func translateDesc(backend Backend, tbl string) string {
	switch backend {
	case BackendSQLite:
		return "PRAGMA table_info(" + tbl + ")"
	case BackendPostgres:
		return "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '" + tbl + "'"
	default:
		return "DESC " + tbl
	}
}

// extractDescTarget 从 "DESC <tbl>" / "DESCRIBE <tbl>" 中提取表名。
// 简单实现：取关键字之后第一个 token，去掉两侧引号 / 反引号。
func extractDescTarget(sql string) string {
	sql = strings.TrimSpace(sql)
	fields := strings.Fields(sql)
	if len(fields) < 2 {
		return ""
	}
	tbl := fields[1]
	tbl = strings.Trim(tbl, "`\"'")
	return tbl
}
