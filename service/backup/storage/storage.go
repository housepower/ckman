package storage

import "github.com/housepower/ckman/model"

// Storage 抽象 BACKUP TABLE / RESTORE TABLE 的目标端 (S3 / Local)
type Storage interface {
	// Init：初始化客户端 / bucket 创建 / 路径白名单校验等
	Init() error

	// BackupSQL：返回 BACKUP TABLE … TO …(…) 的尾部子句
	// 调用方负责拼前面 BACKUP TABLE 部分。partition="all" 表示全表。
	// key 是 BackupRun 在 storage 后端中的完整 key 前缀（由调用方用 JoinRunKey 生成）。
	BackupSQL(database, table, partition, key string) string

	// RestoreSQL：返回 RESTORE TABLE … FROM …(…) 的尾部子句
	RestoreSQL(database, table, partition, key string) string

	// CleanPartition：按 keyPrefix 清空目标端残留对象 / 文件（keyPrefix 由 JoinRunKey 生成）。
	// host 用于 Local 后端 ssh 到目标节点；S3 后端忽略。
	CleanPartition(host, keyPrefix string) error

	// CheckPartition：按 keyPrefix 校验目标端文件 md5 = pathInfo 中预期值。
	// host 用于过滤 pathInfo（只校验属于当前 host 的项）。
	CheckPartition(host, keyPrefix string, pathInfo map[string]model.PathInfo) error

	// Type：s3 / local
	Type() string
}
