package mysql

import "gorm.io/gorm"

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
	LogicCluster   string `gorm:"index:idx_name,unique; column:logic_name"`
	PhysicClusters string `gorm:"column:physic_clusters"`
}

func (v TblLogic) TableName() string {
	return MYSQL_TBL_LOGIC
}
