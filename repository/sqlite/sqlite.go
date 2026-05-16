package sqlite

import (
	"encoding/json"
	"fmt"

	sqlitedriver "github.com/glebarez/sqlite"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	"moul.io/zapgorm2"
)

type SQLitePersistent struct {
	Config   LocalConfig
	Client   *gorm.DB
	ParentDB *gorm.DB
}

func NewSQLitePersistent() *SQLitePersistent {
	return &SQLitePersistent{}
}

func (sp *SQLitePersistent) UnmarshalConfig(configMap map[string]interface{}) interface{} {
	var cfg LocalConfig
	data, err := json.Marshal(configMap)
	if err != nil {
		log.Logger.Errorf("marshal sqlite configMap failed: %v", err)
		return nil
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Logger.Errorf("unmarshal sqlite config failed: %v", err)
		return nil
	}
	return cfg
}

func (sp *SQLitePersistent) Init(cfgIn interface{}) error {
	if cfgIn == nil {
		cfgIn = LocalConfig{}
	}
	sp.Config = cfgIn.(LocalConfig)
	sp.Config.Normalize()

	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", sp.Config.DBPath())

	logger := zapgorm2.New(log.ZapLog)
	logger.SetAsDefault()
	db, err := gorm.Open(sqlitedriver.Open(dsn), &gorm.Config{Logger: logger})
	if err != nil {
		return errors.Wrap(err, "")
	}
	sp.Client = db
	sp.ParentDB = db

	if err := db.AutoMigrate(
		&TblCluster{},
		&TblLogic{},
		&TblQueryHistory{},
		&TblTask{},
		&TblBackup{},
		&TblBackupPolicy{},
		&TblBackupRun{},
		&TblMeta{},
	); err != nil {
		return errors.Wrap(err, "")
	}

	if err := sp.migrateLegacyIfAny(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (sp *SQLitePersistent) Begin() error {
	if sp.Client != sp.ParentDB {
		return repository.ErrTransActionBegin
	}
	tx := sp.Client.Begin()
	sp.Client = tx
	return tx.Error
}

func (sp *SQLitePersistent) Commit() error {
	if sp.Client == sp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := sp.Client.Commit()
	sp.Client = sp.ParentDB
	return tx.Error
}

func (sp *SQLitePersistent) Rollback() error {
	if sp.Client == sp.ParentDB {
		return repository.ErrTransActionEnd
	}
	tx := sp.Client.Rollback()
	sp.Client = sp.ParentDB
	return tx.Error
}

// wrapError 把 gorm 错误归一化到 repository.ErrRecordNotFound。沿用 mysql 后端模式。
func wrapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return repository.ErrRecordNotFound
	}
	return errors.Wrap(err, "")
}

// readMeta 读 TblMeta 中指定 key 的 value。不存在时返回 ("", nil)，不算错误。
func readMeta(db *gorm.DB, key string) (string, error) {
	// 用 Find + slice 而非 First：避免命中 ErrRecordNotFound 时 GORM logger 打 ERROR 噪音
	// （ckman 每次 fresh start 都会经历 not-found 路径，无意义的 ERROR 日志会困扰运维）。
	var ms []TblMeta
	if err := db.Where("key = ?", key).Limit(1).Find(&ms).Error; err != nil {
		return "", errors.Wrap(err, "")
	}
	if len(ms) == 0 {
		return "", nil
	}
	return ms[0].Value, nil
}

// writeMeta upsert 一条 KV 到 TblMeta。
func writeMeta(db *gorm.DB, key, value string) error {
	m := TblMeta{Key: key, Value: value}
	if err := db.Save(&m).Error; err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

// migrateLegacyIfAny is a TEMPORARY STUB for WU6.
// Real implementation lands in WU13 (Plan Task 18).
// For now: check _meta, mark fresh install if empty, else no-op.
func (sp *SQLitePersistent) migrateLegacyIfAny() error {
	cur, err := readMeta(sp.Client, METAKEY_MIGRATED_FROM)
	if err != nil {
		return err
	}
	if cur != "" {
		return nil
	}
	return writeMeta(sp.Client, METAKEY_MIGRATED_FROM, META_FRESH_INSTALL)
}
