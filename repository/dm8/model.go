package dm8

import (
	"time"

	"github.com/housepower/ckman/model"
	dmSchema "github.com/wanlay/gorm-dm8/schema"
	"gorm.io/gorm"
)

type TblCluster struct {
	gorm.Model
	ClusterName   string        `gorm:"index:idx_name,unique"`
	Configuration dmSchema.Clob `gorm:"size:1024000"`
}

type TblLogic struct {
	gorm.Model
	LogicName      string `gorm:"index:idx1,unique"`
	PhysicClusters string
}

type TblQueryHistory struct {
	ClusterName string        `gorm:"index:idx1"`
	Checksum    string        `gorm:"primaryKey"`
	Query       dmSchema.Clob `gorm:"size:1024000"`
	CreateTime  time.Time
}

type TblTask struct {
	TaskId string `gorm:"primaryKey"`
	Status int
	Task   dmSchema.Clob `gorm:"size:1024000"`
}

type TblBackup struct {
	BackupId    string        `gorm:"column:backup_id,primaryKey"`
	ClusterName string        `gorm:"column:cluster_name,index:idx_name"`
	UpdateTime  string        `gorm:"column:update_time"`
	Backup      dmSchema.Clob `gorm:"size:1024000"`
}

type TblBackupPolicy struct {
	PolicyID     string        `gorm:"column:policy_id,primaryKey"`
	ClusterName  string        `gorm:"column:cluster_name,index:idx_bp_cluster_db_table"`
	Database     string        `gorm:"column:database_name,index:idx_bp_cluster_db_table"`
	Table        string        `gorm:"column:table_name,index:idx_bp_cluster_db_table"`
	Instance     string        `gorm:"column:instance,index:idx_bp_instance"`
	ScheduleType string        `gorm:"column:schedule_type"`
	Enabled      bool          `gorm:"column:enabled"`
	Deleted      bool          `gorm:"column:deleted"`
	Policy       dmSchema.Clob `gorm:"column:policy,size:1024000"`
	UpdateTime   string        `gorm:"column:update_time"`
}

type TblBackupRun struct {
	RunID       string        `gorm:"column:run_id,primaryKey"`
	PolicyID    string        `gorm:"column:policy_id,index:idx_br_policy_started"`
	ClusterName string        `gorm:"column:cluster_name,index:idx_br_table_started"`
	Database    string        `gorm:"column:database_name,index:idx_br_table_started"`
	Table       string        `gorm:"column:table_name,index:idx_br_table_started"`
	Status      string        `gorm:"column:status,index:idx_br_status_instance"`
	Instance    string        `gorm:"column:instance,index:idx_br_status_instance"`
	StartedAt   time.Time     `gorm:"column:started_at,index:idx_br_started"`
	Run         dmSchema.Clob `gorm:"column:run,size:1024000"`
	CreateTime  time.Time     `gorm:"column:create_time"`
}

type TblUser struct {
	gorm.Model
	Username     string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash string `gorm:"column:password_hash; size:64; not null"`
	Policy       string `gorm:"column:policy; size:16; not null"`
	Enabled      bool   `gorm:"column:enabled; not null; default:true"`
}

func (TblUser) TableName() string { return DM8_TBL_USER }

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
