package model

import "time"

const (
	BACKUP_IMMEDIATE = "immediate"
	BACKUP_SCHEDULED = "scheduled"

	BACKUP_TYPE_FULL = "full"
	BACKUP_TYPE_INCR = "incremental"

	BACKUP_BY_PARTITON = "partition"
	BACKUP_BY_DAILY    = "daily"

	OP_BACKUP  = "backup"
	OP_RESTORE = "restore"

	BACKUP_LOCAL = "local"
	BACKUP_S3    = "s3"

	BACKUP_PARTITION_STATUS_WAITING = "waiting"
	BACKUP_PARTITION_STATUS_RUNNING = "running"
	BACKUP_PARTITION_STATUS_SUCCESS = "success"
	BACKUP_PARTITION_STATUS_FAILED  = "failed"

	BACKUP_STATUS_WAITING = "waiting"
	BACKUP_STATUS_INIT    = "init"
	BACKUP_STATUS_PREPARE = "prepare"
	BACKUP_STATUS_BACKUP  = "backup"
	BACKUP_STATUS_RESTORE = "restore"
	BACKUP_STATUS_CHECK   = "check"
	BACKUP_STATUS_CLOSE   = "close"
	BACKUP_STATUS_SUCCESS = "success"
	BACKUP_STATUS_FAILED  = "failed"
)

type TargetLocal struct {
	Path string `json:"path" example:"backups"`
}

type TargetS3 struct {
	Endpoint           string `json:"Endpoint" example:"http://192.168.110.8:49000"`
	AccessKeyID        string `json:"AccessKeyID" example:"KZOqVTra982w51MK"`
	SecretAccessKey    string `json:"SecretAccessKey" example:"7Zsdaywu7i5C2AyvLkbupSyVlIzP8qJ0"`
	Region             string `json:"Region" example:"zh-west-1"`
	Bucket             string `json:"Bucket" example:"ckman.backup"`
	UseSSL             bool
	CAFile             string
	InsecureSkipVerify bool
}

type PathInfo struct {
	Host  string
	RPath string
	LPath string
	MD5   string
	Cnt   int
}

type BackupLists struct {
	//Hosts     []string            `json:"host"`
	//QueryId   string              `json:"query_id"`
	Partition string              `json:"partition"`
	Size      uint64              `json:"size"`
	Rows      uint64              `json:"rows"`
	FileNum   uint64              `json:"fileNum"`
	PathInfo  map[string]PathInfo `json:"-"`
	Status    string              `json:"status"`
	Msg       string              `json:"msg"`
	Elapsed   int                 `json:"elapsed"`
}

type Backup struct {
	BackupId     string        `json:"backup_id"` // primary key
	Database     string        `json:"database"`
	ClusterName  string        `json:"cluster_name"`
	Table        string        `json:"table"`
	Partitions   []BackupLists `json:"partitions"`
	ScheduleType string        `json:"schedule_type"` // immediate, scheduled
	Crontab      string        `json:"crontab"`
	DaysBefore   int           `json:"days_before"`
	Clean        bool          `json:"clean"`       // 备份成功后是否删除本地数据
	Operation    string        `json:"operation"`   // backup, restore
	TargetType   string        `json:"target_type"` // local, s3, hdfs
	Local        TargetLocal
	S3           TargetS3
	Status       string    `json:"status"`
	Compression  string    `json:"Compression" example:"gzip"` // none, gzip/gz, brotli/br, xz/LZMA, zstd/zst
	CreateTime   time.Time `json:"create_time"`
	UpdateTime   time.Time `json:"update_time"`
}

type BackupRequest struct {
	ScheduleType string      `json:"schedule_type"`              //备份类型： 立即备份，定时备份
	Crontab      string      `json:"crontab"`                    //定时备份的cron表达式
	Database     string      `json:"database"`                   //数据库名称
	Tables       []string    `json:"tables"`                     //表名称
	BackupType   string      `json:"backup_style"`               //备份类型： 全量备份，增量备份
	BackupStyle  string      `json:"backup_type"`                //备份方式： 按分区备份，按日期备份
	Partitions   []string    `json:"partitions"`                 //分区名称
	DaysBefore   int         `json:"days_before"`                //保留天数
	Target       string      `json:"target"`                     //备份目标： 本地，S3
	Local        TargetLocal `json:"local"`                      //本地备份路径
	S3           TargetS3    `json:"s3"`                         //S3配置
	Compression  string      `json:"Compression" example:"gzip"` //压缩类型： 不压缩，gzip/gz, brotli/br, xz/LZMA, zstd/zst
	Clean        bool        `json:"clean"`                      //是否清理本地数据
}

type RestoreRequest struct {
	BackupId   string   `json:"backup_id"`
	Partitions []string `json:"partition"`
}

type RestoreResponse struct {
}
