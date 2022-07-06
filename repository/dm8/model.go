package dm8

import (
	"time"

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
