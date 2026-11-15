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
	// backup_id 是这张表的主键。漏标 primaryKey 时原生 MySQL 仍能建表,但 TDSQL 等
	// 要求每表必须有主键的分支会拒绝;size 与 postgres 的 varchar(382) 对齐,避免
	// gorm 默认 191 截断 cluster+db+table 拼出的长 id。
	BackupId    string `gorm:"primaryKey;column:backup_id;size:382"`
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
	ClusterName string    `gorm:"column:cluster_name;index:idx_br_table_started;index:idx_br_cluster_status"`
	Database    string    `gorm:"column:database_name;index:idx_br_table_started"`
	Table       string    `gorm:"column:table_name;index:idx_br_table_started"`
	Status      string    `gorm:"column:status;index:idx_br_status_instance;index:idx_br_cluster_status"`
	Instance    string    `gorm:"column:instance;index:idx_br_status_instance"`
	StartedAt   MysqlTime `gorm:"column:started_at;index:idx_br_policy_started;index:idx_br_table_started"` // 可能取零值(queued/skipped),用哨兵映射免疫 Error 1292
	Run         string    `gorm:"column:run;type:JSON"`                                                     // 整个 BackupRun JSON 序列化
	CreateTime  time.Time `gorm:"column:create_time"`
}

func (v TblBackupRun) TableName() string { return MYSQL_TBL_BACKUP_RUN }

type TblDiagnosisRun struct {
	RunID           string `gorm:"column:run_id;primaryKey;size:64"`
	ClusterName     string `gorm:"column:cluster_name;size:128;index:idx_dr_cluster_created,priority:1"`
	RequestedBy     string `gorm:"column:requested_by;size:64;index:idx_dr_user_created,priority:1"`
	Scene           string `gorm:"column:scene;size:64"`
	ContextJSON     string `gorm:"column:context_json;type:LONGTEXT"`
	ChecksJSON      string `gorm:"column:checks_json;type:LONGTEXT"`
	Status          string `gorm:"column:status;size:16;index:idx_dr_status_updated,priority:1"`
	Execution       string `gorm:"column:execution;size:16"`
	Outcome         string `gorm:"column:outcome;size:16"`
	Severity        string `gorm:"column:severity;size:16"`
	ReportJSON      string `gorm:"column:report_json;type:LONGTEXT"`
	ErrorMessage    string `gorm:"column:error_message;type:LONGTEXT"`
	OwnerInstance   string `gorm:"column:owner_instance;size:128"`
	CancelRequested bool   `gorm:"column:cancel_requested"`
	CreatedAtMS     int64  `gorm:"column:created_at_ms;index:idx_dr_cluster_created,priority:2;index:idx_dr_user_created,priority:2"`
	StartedAtMS     int64  `gorm:"column:started_at_ms"`
	FinishedAtMS    int64  `gorm:"column:finished_at_ms"`
	UpdatedAtMS     int64  `gorm:"column:updated_at_ms;index:idx_dr_status_updated,priority:2"`
	ExpiresAtMS     int64  `gorm:"column:expires_at_ms;index:idx_dr_expires"`
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

func (TblUser) TableName() string { return MYSQL_TBL_USER }

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
