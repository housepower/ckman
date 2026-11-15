package dm8

import (
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
	CreateTime  DmTime
}

type TblTask struct {
	TaskId string `gorm:"primaryKey"`
	Status int
	Task   dmSchema.Clob `gorm:"size:1024000"`
}

// 列名一律大写:达梦把不加引号的标识符折叠成大写存储,而 AutoMigrate 用大小写
// 敏感比较判断列是否已存在。若 model 用小写列名,新增列(经 ALTER ADD)在库里
// 存成大写后会被误判为缺失,重启时反复 ADD 触发 -2116「列已存在」。大写列名与
// 达梦存储/驱动返回的大小写一致,且 dm8.go 里的裸标识符 SQL(WHERE/Order/Updates)
// 会被达梦折叠成大写,无需改动。
type TblBackup struct {
	BackupId    string        `gorm:"column:BACKUP_ID;primaryKey"`
	ClusterName string        `gorm:"column:CLUSTER_NAME;index:idx_name"`
	UpdateTime  string        `gorm:"column:UPDATE_TIME"`
	Backup      dmSchema.Clob `gorm:"size:1024000"`
}

type TblBackupPolicy struct {
	PolicyID     string        `gorm:"column:POLICY_ID;primaryKey"`
	ClusterName  string        `gorm:"column:CLUSTER_NAME;index:idx_bp_cluster_db_table"`
	Database     string        `gorm:"column:DATABASE_NAME;index:idx_bp_cluster_db_table"`
	Table        string        `gorm:"column:TABLE_NAME;index:idx_bp_cluster_db_table"`
	Instance     string        `gorm:"column:INSTANCE;index:idx_bp_instance"`
	ScheduleType string        `gorm:"column:SCHEDULE_TYPE"`
	Enabled      bool          `gorm:"column:ENABLED"`
	Deleted      bool          `gorm:"column:DELETED"`
	Policy       dmSchema.Clob `gorm:"column:POLICY;size:1024000"`
	UpdateTime   string        `gorm:"column:UPDATE_TIME"`
}

type TblBackupRun struct {
	RunID       string        `gorm:"column:RUN_ID;primaryKey"`
	PolicyID    string        `gorm:"column:POLICY_ID;index:idx_br_policy_started"`
	ClusterName string        `gorm:"column:CLUSTER_NAME;index:idx_br_table_started;index:idx_br_cluster_status"`
	Database    string        `gorm:"column:DATABASE_NAME;index:idx_br_table_started"`
	Table       string        `gorm:"column:TABLE_NAME;index:idx_br_table_started"`
	Status      string        `gorm:"column:STATUS;index:idx_br_status_instance;index:idx_br_cluster_status"`
	Instance    string        `gorm:"column:INSTANCE;index:idx_br_status_instance"`
	StartedAt   DmTime        `gorm:"column:STARTED_AT;index:idx_br_policy_started;index:idx_br_table_started"`
	Run         dmSchema.Clob `gorm:"column:RUN;size:1024000"`
	CreateTime  DmTime        `gorm:"column:CREATE_TIME"`
}

type TblDiagnosisRun struct {
	RunID           string        `gorm:"column:RUN_ID;primaryKey;size:64"`
	ClusterName     string        `gorm:"column:CLUSTER_NAME;size:128;index:idx_dr_cluster_created,priority:1"`
	RequestedBy     string        `gorm:"column:REQUESTED_BY;size:64;index:idx_dr_user_created,priority:1"`
	Scene           string        `gorm:"column:SCENE;size:64"`
	ContextJSON     dmSchema.Clob `gorm:"column:CONTEXT_JSON;size:4194304"`
	ChecksJSON      dmSchema.Clob `gorm:"column:CHECKS_JSON;size:4194304"`
	Status          string        `gorm:"column:STATUS;size:16;index:idx_dr_status_updated,priority:1"`
	Execution       string        `gorm:"column:EXECUTION;size:16"`
	Outcome         string        `gorm:"column:OUTCOME;size:16"`
	Severity        string        `gorm:"column:SEVERITY;size:16"`
	ReportJSON      dmSchema.Clob `gorm:"column:REPORT_JSON;size:4194304"`
	ErrorMessage    dmSchema.Clob `gorm:"column:ERROR_MESSAGE;size:4194304"`
	OwnerInstance   string        `gorm:"column:OWNER_INSTANCE;size:128"`
	CancelRequested bool          `gorm:"column:CANCEL_REQUESTED"`
	CreatedAtMS     int64         `gorm:"column:CREATED_AT_MS;index:idx_dr_cluster_created,priority:2;index:idx_dr_user_created,priority:2"`
	StartedAtMS     int64         `gorm:"column:STARTED_AT_MS"`
	FinishedAtMS    int64         `gorm:"column:FINISHED_AT_MS"`
	UpdatedAtMS     int64         `gorm:"column:UPDATED_AT_MS;index:idx_dr_status_updated,priority:2"`
	ExpiresAtMS     int64         `gorm:"column:EXPIRES_AT_MS;index:idx_dr_expires"`
}

func (TblDiagnosisRun) TableName() string { return "tbl_diagnosis_run" }

type TblUser struct {
	gorm.Model
	Username           string `gorm:"index:idx_user_name,unique; column:username; size:32; not null"`
	PasswordHash       string `gorm:"column:password_hash; size:64; not null"`
	Policy             string `gorm:"column:policy; size:16; not null"`
	Enabled            bool   `gorm:"column:enabled; not null"`
	MustChangePassword bool   `gorm:"column:must_change_password; not null; default:false"`
}

func (TblUser) TableName() string { return DM8_TBL_USER }

func tblUserToModel(t TblUser) model.CkmanUser {
	return model.CkmanUser{
		ID:                 int64(t.ID),
		Username:           t.Username,
		PasswordHash:       t.PasswordHash,
		Policy:             t.Policy,
		Enabled:            t.Enabled,
		MustChangePassword: t.MustChangePassword,
		CreatedAt:          t.CreatedAt.Unix(),
		UpdatedAt:          t.UpdatedAt.Unix(),
	}
}
