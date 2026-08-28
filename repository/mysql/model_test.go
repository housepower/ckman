package mysql

import (
	"sync"
	"testing"

	"gorm.io/gorm/schema"
)

// TestAllModelsHavePrimaryKey 保证 Init 里 AutoMigrate 的每个表都带主键。
// 无主键表在原生 MySQL 上能建成功，但 TDSQL 等要求每表必须有主键的分支会直接
// 拒绝建表；tbl_backup 曾漏标 primaryKey，在 TDSQL 上适配时才暴露。
func TestAllModelsHavePrimaryKey(t *testing.T) {
	models := []interface{}{
		&TblCluster{},
		&TblLogic{},
		&TblQueryHistory{},
		&TblTask{},
		&TblBackup{},
		&TblBackupPolicy{},
		&TblBackupRun{},
		&TblUser{},
	}

	cache := &sync.Map{}
	for _, m := range models {
		s, err := schema.Parse(m, cache, schema.NamingStrategy{})
		if err != nil {
			t.Fatalf("parse %T: %v", m, err)
		}
		if len(s.PrimaryFields) == 0 {
			t.Errorf("表 %s (%T) 没有主键,TDSQL 等强制主键的 MySQL 分支会拒绝建表", s.Table, m)
		}
	}
}
