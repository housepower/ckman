package mysql

import (
	"time"

	"github.com/housepower/ckman/model"
	"gorm.io/gorm"
)

type TblCluster struct {
	gorm.Model
	ClusterName string `gorm:"index:idx_name,unique; column:cluster_name"`
	Config      string `gorm:"column:config"`
}

func (v TblCluster) TableName() string {
	return MYSQL_TBL_CLUSTER
}

type TblLogic struct {
	gorm.Model
	LogicCluster   string `gorm:"index:idx1,unique; column:logic_name"`
	PhysicClusters string `gorm:"column:physic_clusters"`
}

func (v TblLogic) TableName() string {
	return MYSQL_TBL_LOGIC
}

type TblQueryHistory struct {
	Cluster    string    `gorm:"index:idx1; column:cluster"`
	CheckSum   string    `gorm:"primaryKey; column:checksum"`
	QuerySql   string    `gorm:"column:query"`
	CreateTime time.Time `gorm:"column:create_time"`
}

func (v TblQueryHistory) TableName() string {
	return MYSQL_TBL_QUERY_HISTORY
}

type TblTask struct {
	TaskId string `gorm:"primaryKey; column:task_id"`
	Status int    `gorm:"column:status"`
	Task   string `gorm:"column:config"`
}

func (v TblTask) TableName() string {
	return MYSQL_TBL_TASK
}

type TblBackup struct {
	BackupId    string `gorm:"column:backup_id"`
	ClusterName string `gorm:"column:cluster_name"`
	UpdateTime  string `gorm:"column:update_time"`
	Backup      string `gorm:"column:backup"`
}

func (v TblBackup) TableName() string {
	return MYSQL_TBL_BACKUP
}

// TblBackupPolicy：调度配置；查询模式：按 cluster / 按 (db,table) / 按 instance 过滤
type TblBackupPolicy struct {
	PolicyID     string `gorm:"column:policy_id;primaryKey"`
	ClusterName  string `gorm:"column:cluster_name;index:idx_bp_cluster_db_table"`
	Database     string `gorm:"column:database_name;index:idx_bp_cluster_db_table"`
	Table        string `gorm:"column:table_name;index:idx_bp_cluster_db_table"`
	Instance     string `gorm:"column:instance;index:idx_bp_instance"`
	ScheduleType string `gorm:"column:schedule_type"`
	Enabled      bool   `gorm:"column:enabled"`
	Deleted      bool   `gorm:"column:deleted"`
	Policy       string `gorm:"column:policy;type:JSON"` // 整个 BackupPolicy JSON 序列化
	UpdateTime   string `gorm:"column:update_time"`
}

func (v TblBackupPolicy) TableName() string { return MYSQL_TBL_BACKUP_POLICY }

// TblBackupRun：执行历史；查询模式：按 policy_id / 按 (cluster,db,table) / 按 status+instance
type TblBackupRun struct {
	RunID       string    `gorm:"column:run_id;primaryKey"`
	PolicyID    string    `gorm:"column:policy_id;index:idx_br_policy_started"`
	ClusterName string    `gorm:"column:cluster_name;index:idx_br_table_started"`
	Database    string    `gorm:"column:database_name;index:idx_br_table_started"`
	Table       string    `gorm:"column:table_name;index:idx_br_table_started"`
	Status      string    `gorm:"column:status;index:idx_br_status_instance"`
	Instance    string    `gorm:"column:instance;index:idx_br_status_instance"`
	StartedAt   time.Time `gorm:"column:started_at;index:idx_br_policy_started;index:idx_br_table_started"`
	Run         string    `gorm:"column:run;type:JSON"` // 整个 BackupRun JSON 序列化
	CreateTime  time.Time `gorm:"column:create_time"`
}

func (v TblBackupRun) TableName() string { return MYSQL_TBL_BACKUP_RUN }

type TblUser struct {
	gorm.Model
	Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string `gorm:"column:password_hash; size:64; not null"`
	Policy       string `gorm:"column:policy; size:16; not null"`
	Enabled      bool   `gorm:"column:enabled; not null"`
}

func (TblUser) TableName() string { return MYSQL_TBL_USER }

func tblUserToModel(t TblUser) model.CkmanUser {
	return model.CkmanUser{
		ID:           int64(t.ID),
		Username:     t.Username,
		PasswordHash: t.PasswordHash,
		Policy:       t.Policy,
		Enabled:      t.Enabled,
		CreatedAt:    t.CreatedAt.Unix(),
		UpdatedAt:    t.UpdatedAt.Unix(),
	}
}
