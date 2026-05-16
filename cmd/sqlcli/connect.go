package sqlcli

import (
	"github.com/housepower/ckman/repository/dm8"
	"github.com/housepower/ckman/repository/mysql"
	"github.com/housepower/ckman/repository/postgres"
	"github.com/housepower/ckman/repository/sqlite"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Backend 标识当前 sqlcli 连接到了哪个后端。
// 用于决定 SHOW DATABASES / SHOW TABLES / DESC 翻译到哪种原生 SQL。
type Backend int

const (
	BackendUnknown  Backend = iota
	BackendSQLite
	BackendMySQL
	BackendPostgres
	BackendDM8
)

func (b Backend) String() string {
	switch b {
	case BackendSQLite:
		return "sqlite"
	case BackendMySQL:
		return "mysql"
	case BackendPostgres:
		return "postgres"
	case BackendDM8:
		return "dm8"
	default:
		return "unknown"
	}
}

// OpenDB 按 ckman 的 persistent_policy 选驱动 + 打开数据库。
// cfgMap 取自 config.GlobalConfig.PersistentConfig[policy]。
// 调用方负责关闭 sqlDB（通过 db.DB() + Close）。
func OpenDB(policy string, cfgMap map[string]interface{}) (*gorm.DB, Backend, error) {
	var (
		dialector gorm.Dialector
		backend   Backend
		err       error
	)
	switch policy {
	case "local":
		dialector, err = sqlite.BuildDialector(cfgMap)
		backend = BackendSQLite
	case "mysql":
		dialector, err = mysql.BuildDialector(cfgMap)
		backend = BackendMySQL
	case "postgres":
		dialector, err = postgres.BuildDialector(cfgMap)
		backend = BackendPostgres
	case "dm8":
		dialector, err = dm8.BuildDialector(cfgMap)
		backend = BackendDM8
	default:
		return nil, BackendUnknown, errors.Errorf("unsupported persistent_policy: %s", policy)
	}
	if err != nil {
		return nil, BackendUnknown, errors.Wrap(err, "build dialector")
	}
	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, BackendUnknown, errors.Wrap(err, "open db")
	}
	return db, backend, nil
}
