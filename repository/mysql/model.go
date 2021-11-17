package mysql

import (
	"gorm.io/gorm"
	"time"
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
