package sqlite

import (
	"time"

	"gorm.io/gorm"
)

// 字段定义与 repository/mysql/model.go 对齐；type:JSON 改为 type:TEXT（SQLite 弱类型，显式 TEXT 避免歧义）。

type TblCluster struct {
	gorm.Model
	ClusterName string `gorm:"index:idx_name,unique; column:cluster_name"`
	Config      string `gorm:"column:config;type:TEXT"`
}

func (TblCluster) TableName() string { return SQLITE_TBL_CLUSTER }

type TblLogic struct {
	gorm.Model
	LogicCluster   string `gorm:"index:idx_logic_name,unique; column:logic_name"`
	PhysicClusters string `gorm:"column:physic_clusters;type:TEXT"`
}

func (TblLogic) TableName() string { return SQLITE_TBL_LOGIC }

type TblQueryHistory struct {
	Cluster    string    `gorm:"index:idx_qh_cluster; column:cluster"`
	CheckSum   string    `gorm:"primaryKey; column:checksum"`
	QuerySql   string    `gorm:"column:query;type:TEXT"`
	CreateTime time.Time `gorm:"column:create_time"`
}

func (TblQueryHistory) TableName() string { return SQLITE_TBL_QUERY_HISTORY }

type TblTask struct {
	TaskId string `gorm:"primaryKey; column:task_id"`
	Status int    `gorm:"column:status"`
	Task   string `gorm:"column:config;type:TEXT"`
}

func (TblTask) TableName() string { return SQLITE_TBL_TASK }

type TblBackup struct {
	BackupId    string `gorm:"column:backup_id"`
	ClusterName string `gorm:"column:cluster_name"`
	UpdateTime  string `gorm:"column:update_time"`
	Backup      string `gorm:"column:backup;type:TEXT"`
}

func (TblBackup) TableName() string { return SQLITE_TBL_BACKUP }

type TblBackupPolicy struct {
	PolicyID     string `gorm:"column:policy_id;primaryKey"`
	ClusterName  string `gorm:"column:cluster_name;index:idx_bp_cluster_db_table"`
	Database     string `gorm:"column:database_name;index:idx_bp_cluster_db_table"`
	Table        string `gorm:"column:table_name;index:idx_bp_cluster_db_table"`
	Instance     string `gorm:"column:instance;index:idx_bp_instance"`
	ScheduleType string `gorm:"column:schedule_type"`
	Enabled      bool   `gorm:"column:enabled"`
	Deleted      bool   `gorm:"column:deleted"`
	Policy       string `gorm:"column:policy;type:TEXT"`
	UpdateTime   string `gorm:"column:update_time"`
}

func (TblBackupPolicy) TableName() string { return SQLITE_TBL_BACKUP_POLICY }

type TblBackupRun struct {
	RunID       string    `gorm:"column:run_id;primaryKey"`
	PolicyID    string    `gorm:"column:policy_id;index:idx_br_policy_started"`
	ClusterName string    `gorm:"column:cluster_name;index:idx_br_table_started"`
	Database    string    `gorm:"column:database_name;index:idx_br_table_started"`
	Table       string    `gorm:"column:table_name;index:idx_br_table_started"`
	Status      string    `gorm:"column:status;index:idx_br_status_instance"`
	Instance    string    `gorm:"column:instance;index:idx_br_status_instance"`
	StartedAt   time.Time `gorm:"column:started_at;index:idx_br_policy_started;index:idx_br_table_started"`
	Run         string    `gorm:"column:run;type:TEXT"`
	CreateTime  time.Time `gorm:"column:create_time"`
}

func (TblBackupRun) TableName() string { return SQLITE_TBL_BACKUP_RUN }

// TblMeta 是单一 KV 表，目前存 migrated_from / schema_version。
type TblMeta struct {
	Key   string `gorm:"primaryKey; column:key"`
	Value string `gorm:"column:value;type:TEXT"`
}

func (TblMeta) TableName() string { return SQLITE_TBL_META }
