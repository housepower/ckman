package mysql

import (
	"encoding/json"
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/pkg/errors"
	driver "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MysqlConfig struct {
	Host            string `yaml:"host" json:"host"`
	Port            int    `yaml:"port" json:"port"`
	User            string `yaml:"user" json:"user"`
	Password        string `yaml:"password" json:"password"`
	DataBase        string `yaml:"database" json:"database"`
	MaxIdleConns    int    `yaml:"max_idle_conns" json:"max_idle_conns"`
	MaxOpenConns    int    `yaml:"max_open_conns" json:"max_open_conns"`
	ConnMaxLifetime int    `yaml:"conn_max_life_time" json:"conn_max_life_time"`
	ConnMaxIdleTime int    `yaml:"conn_max_idle_time" json:"conn_max_idle_time"`
}

func (config *MysqlConfig) Normalize() {
	config.Port = common.GetIntegerwithDefault(config.Port, MYSQL_PORT_DEFAULT)
	config.DataBase = common.GetStringwithDefault(config.DataBase, MYSQL_DATABASE_DEFAULT)
	config.MaxIdleConns = common.GetIntegerwithDefault(config.MaxIdleConns, MYSQL_MAX_IDLE_CONNS_DEFAULT)
	config.MaxOpenConns = common.GetIntegerwithDefault(config.MaxOpenConns, MYSQL_MAX_OPEN_CONNS_DEFAULT)
	config.ConnMaxLifetime = common.GetIntegerwithDefault(config.ConnMaxLifetime, MYSQL_MAX_LIFETIME_DEFAULT)
	config.ConnMaxIdleTime = common.GetIntegerwithDefault(config.ConnMaxIdleTime, MYSQL_MAX_IDLE_TIME_DEFAULT)
	_ = common.Gsypt.Unmarshal(&config)
}

// BuildDialector 把 ckman.hjson 中 persistent_config.mysql 节点转换为 GORM Dialector。
// 用法：sqlcli 等工具想拿原始 *gorm.DB 而不触发 Init() 的迁移路径时，调此函数 + gorm.Open。
// 入参 cfgMap 取自 config.GlobalConfig.PersistentConfig["mysql"]。
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
	cfg := MysqlConfig{}
	data, err := json.Marshal(cfgMap)
	if err != nil {
		return nil, errors.Wrap(err, "marshal cfgMap")
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshal cfgMap")
	}
	cfg.Normalize()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DataBase)
	return driver.Open(dsn), nil
}
