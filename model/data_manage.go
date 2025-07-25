package model

import "time"

const (
	BACKUP_IMMEDIATE = "immediate"
	BACKUP_SCHEDULED = "scheduled"

	OP_BACKUP  = "backup"
	OP_RESTORE = "restore"

	BACKUP_HDFS  = "hdfs"
	BACKUP_LOCAL = "local"
	BACKUP_S3    = "s3"

	BACKUP_STATUS_WAITING = "waiting"
	BACKUP_STATUS_RUNNING = "running"
	BACKUP_STATUS_SUCCESS = "success"
	BACKUP_STATUS_FAILED  = "failed"
	BACKUP_STATUS_CANCEL  = "cancel"
)

type TargetHdfs struct {
	Addr string `json:"addr" example:"localhost:8020"`
	User string `json:"user" example:"hdfs"`
	Dir  string `json:"dir" example:"/data01"`
}

type TargetLocal struct {
	Path string `json:"Path" example:"/data01/clickhouse/backup/"`
}

type TargetS3 struct {
	Endpoint        string `json:"Endpoint" example:"http://192.168.110.8:49000"`
	AccessKeyID     string `json:"AccessKeyID" example:"KZOqVTra982w51MK"`
	SecretAccessKey string `json:"SecretAccessKey" example:"7Zsdaywu7i5C2AyvLkbupSyVlIzP8qJ0"`
	Region          string `json:"Region" example:"zh-west-1"`
	Bucket          string `json:"Bucket" example:"ckman.backup"`
	Compression     string `json:"Compression" example:"gzip"` // none, gzip/gz, brotli/br, xz/LZMA, zstd/zst
	Expired         int    `json:"expired"`
}

type BackupLists struct {
	Host      string `json:"host"`
	QueryId   string `json:"query_id"`
	Partition string `json:"partition"`
}

type Backup struct {
	BackupId           string        `json:"backup_id"` // primary key
	Database           string        `json:"database"`
	ClusterName        string        `json:"cluster_name"`
	Table              string        `json:"table"`
	TodoPartitions     []BackupLists `json:"todo_partitions"`
	CompeltedPatitions []BackupLists `json:"compelted_patitions"`
	Elapsed            string        `json:"elapsed"`
	ScheduleType       string        `json:"schedule_type"` // immediate, scheduled
	Crontab            string        `json:"crontab"`
	DaysBefore         int           `json:"days_before"`
	Clean              bool          `json:"clean"`       // 备份成功后是否删除本地数据
	Operation          string        `json:"operation"`   // backup, restore
	TargetType         string        `json:"target_type"` // local, s3, hdfs
	Local              TargetLocal
	S3                 TargetS3
	HDFS               TargetHdfs
	//Expired            int       `json:"expired"`
	CreateTime time.Time `json:"create_time"`
	UpdateTime time.Time `json:"update_time"`
	Status     string    `json:"status"`
}
