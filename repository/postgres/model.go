package postgres

import (
	"time"

	"github.com/housepower/ckman/model"
)

type TblCluster struct {
	Id          int    `gorm:"column:id;primaryKey"`
	CreateAt    string `gorm:"column:created_at"`
	UpdateAt    string `gorm:"column:update_at"`
	DeleteAt    string `gorm:"column:delete_at"`
	ClusterName string `gorm:"column:cluster_name;unique"`
	Config      string `gorm:"column:config"`
}

func (v TblCluster) TableName() string {
	return PG_TBL_CLUSTER
}

type TblLogic struct {
	LogicCluster   string `gorm:"column:logic_name;unique;primaryKey"`
	PhysicClusters string `gorm:"column:physic_clusters"`
}

func (v TblLogic) TableName() string {
	return PG_TBL_LOGIC
}

type TblQueryHistory struct {
	Cluster    string    `gorm:"column:cluster"`
	CheckSum   string    `gorm:"primaryKey;column:checksum"`
	QuerySql   string    `gorm:"column:query"`
	CreateTime time.Time `gorm:"column:create_time"`
}

func (v TblQueryHistory) TableName() string {
	return PG_TBL_QUERY_HISTORY
}

type TblTask struct {
	TaskId string `gorm:"primaryKey;column:task_id"`
	Status int    `gorm:"column:status;type:bigint"`
	Task   string `gorm:"column:config"`
}

func (v TblTask) TableName() string {
	return PG_TBL_TASK
}

type TblBackup struct {
	BackupId    string `gorm:"primaryKey;column:backup_id"`
	ClusterName string `gorm:"column:cluster_name"`
	UpdateTime  string `gorm:"column:update_time"`
	Backup      string `gorm:"column:backup"`
}

func (v TblBackup) TableName() string {
	return PG_TBL_BACKUP
}

type TblBackupPolicy struct {
	PolicyID     string `gorm:"column:policy_id;primaryKey"`
	ClusterName  string `gorm:"column:cluster_name;index:idx_bp_cluster_db_table"`
	Database     string `gorm:"column:database_name;index:idx_bp_cluster_db_table"`
	Table        string `gorm:"column:table_name;index:idx_bp_cluster_db_table"`
	Instance     string `gorm:"column:instance;index:idx_bp_instance"`
	ScheduleType string `gorm:"column:schedule_type"`
	Enabled      bool   `gorm:"column:enabled"`
	Deleted      bool   `gorm:"column:deleted"`
	Policy       string `gorm:"column:policy;type:JSONB"`
	UpdateTime   string `gorm:"column:update_time"`
}

func (v TblBackupPolicy) TableName() string { return PG_TBL_BACKUP_POLICY }

type TblBackupRun struct {
	RunID       string    `gorm:"column:run_id;primaryKey"`
	PolicyID    string    `gorm:"column:policy_id;index:idx_br_policy_started"`
	ClusterName string    `gorm:"column:cluster_name;index:idx_br_table_started"`
	Database    string    `gorm:"column:database_name;index:idx_br_table_started"`
	Table       string    `gorm:"column:table_name;index:idx_br_table_started"`
	Status      string    `gorm:"column:status;index:idx_br_status_instance"`
	Instance    string    `gorm:"column:instance;index:idx_br_status_instance"`
	StartedAt   time.Time `gorm:"column:started_at;index:idx_br_policy_started;index:idx_br_table_started"`
	Run         string    `gorm:"column:run;type:JSONB"`
	CreateTime  time.Time `gorm:"column:create_time"`
}

func (v TblBackupRun) TableName() string { return PG_TBL_BACKUP_RUN }

type TblUser struct {
	ID           uint      `gorm:"column:id;primaryKey;autoIncrement"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
	Username     string    `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string    `gorm:"column:password_hash; size:64; not null"`
	Policy       string    `gorm:"column:policy; size:16; not null"`
	Enabled      bool      `gorm:"column:enabled; not null"`
}

func (TblUser) TableName() string { return POSTGRES_TBL_USER }

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
