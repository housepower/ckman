package storage

import "github.com/housepower/ckman/model"

// Storage 抽象 BACKUP TABLE / RESTORE TABLE 的目标端 (S3 / Local)
type Storage interface {
	// Init：初始化客户端 / bucket 创建 / 路径白名单校验等
	Init() error

	// BackupSQL：返回 BACKUP TABLE … TO …(…) 的尾部子句
	// 调用方负责拼前面 BACKUP TABLE 部分。partition="all" 表示全表。
	// key 是 partition 在目标的子路径
	BackupSQL(database, table, partition, key string) string

	// RestoreSQL：返回 RESTORE TABLE … FROM …(…) 的尾部子句
	RestoreSQL(database, table, partition, key string) string

	// CleanPartition：清空目标端某 partition 之前的残留对象 / 文件
	CleanPartition(database, table, host, partition string) error

	// CheckPartition：校验目标端文件 md5 = pathInfo 中预期值
	CheckPartition(host, database, table, partition string, pathInfo map[string]model.PathInfo) error

	// Type：s3 / local
	Type() string
}
