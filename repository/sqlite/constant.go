package sqlite

// SQLitePersistentName 是 ckman.hjson 中 persistent_policy 的值。
// 保持 "local" 不变以兼容用户配置。
const SQLitePersistentName = "local"

const (
	SQLITE_TBL_CLUSTER       = "tbl_cluster"
	SQLITE_TBL_LOGIC         = "tbl_logic"
	SQLITE_TBL_QUERY_HISTORY = "tbl_query_history"
	SQLITE_TBL_TASK          = "tbl_task"
	SQLITE_TBL_BACKUP        = "tbl_backup"
	SQLITE_TBL_BACKUP_POLICY = "tbl_backup_policy"
	SQLITE_TBL_BACKUP_RUN    = "tbl_backup_run"
	SQLITE_TBL_META          = "tbl_meta"
)

const (
	SQLITE_DEFAULT_DB_FILE = "clusters.db"
	SQLITE_SCHEMA_VERSION  = "1"
)

const (
	METAKEY_MIGRATED_FROM  = "migrated_from"
	METAKEY_SCHEMA_VERSION = "schema_version"

	META_FRESH_INSTALL = "(fresh install)"
)
