package postgres

import (
	"time"
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
