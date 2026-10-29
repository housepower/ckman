package repository

import (
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

// OpenGormDB 用 dialector 建立 *gorm.DB 并保证底层 *sql.DB 可用。
// gorm-dm8 的 Initialize 会用回调注册的返回值覆盖 sql.Open 的 err：DSN 解析
// 失败时 ConnPool 是 nil *sql.DB 却不报错，交给 gorm 自动 Ping 会 SIGSEGV
// （线上真实事故：密码含 # 截断 DSN → 启动即崩）。统一关掉自动 Ping、显式校验
// 并 Ping，把故障降级为普通 error。
// gormCfg 允许调用方定制 Logger 等，传 nil 用默认配置；
// DisableAutomaticPing 会被强制置 true。
func OpenGormDB(dialector gorm.Dialector, gormCfg *gorm.Config) (*gorm.DB, error) {
	if gormCfg == nil {
		gormCfg = &gorm.Config{}
	}
	gormCfg.DisableAutomaticPing = true
	db, err := gorm.Open(dialector, gormCfg)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	if sqlDB == nil {
		return nil, errors.New("驱动未建立底层连接(DSN 解析失败?),请检查 persistent_config 配置")
	}
	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, errors.Wrap(err, "ping")
	}
	return db, nil
}
