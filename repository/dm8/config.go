package dm8

import (
	"encoding/json"
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/pkg/errors"
	driver "github.com/wanlay/gorm-dm8"
	"gorm.io/gorm"
)

type DM8Config struct {
	Host            string `yaml:"host" json:"host"`
	Port            int    `yaml:"port" json:"port"`
	User            string `yaml:"user" json:"user"`
	Password        string `yaml:"password" json:"password"`
	Schema          string `yaml:"schema" json:"schema"`
	MaxIdleConns    int    `yaml:"max_idle_conns" json:"max_idle_conns"`
	MaxOpenConns    int    `yaml:"max_open_conns" json:"max_open_conns"`
	ConnMaxLifetime int    `yaml:"conn_max_life_time" json:"conn_max_life_time"`
	ConnMaxIdleTime int    `yaml:"conn_max_idle_time" json:"conn_max_idle_time"`
}

func (config *DM8Config) Normalize() {
	config.Port = common.GetIntegerwithDefault(config.Port, DM8_PORT_DEFAULT)
	config.User = common.GetStringwithDefault(config.User, DM8_USER_DEFAULT)
	config.Password = common.GetStringwithDefault(config.Password, DM8_PASSWD_DEFAULT)
	config.MaxIdleConns = common.GetIntegerwithDefault(config.MaxIdleConns, DM8_MAX_IDLE_CONNS_DEFAULT)
	config.MaxOpenConns = common.GetIntegerwithDefault(config.MaxOpenConns, DM8_MAX_OPEN_CONNS_DEFAULT)
	config.ConnMaxLifetime = common.GetIntegerwithDefault(config.ConnMaxLifetime, DM8_MAX_LIFETIME_DEFAULT)
	config.ConnMaxIdleTime = common.GetIntegerwithDefault(config.ConnMaxIdleTime, DM8_MAX_IDLE_TIME_DEFAULT)
	_ = common.Gsypt.Unmarshal(&config)
}

// BuildDialector 把 ckman.hjson 中 persistent_config.dm8 节点转换为 GORM Dialector。
// 用法：sqlcli 等工具想拿原始 *gorm.DB 而不触发 Init() 的迁移路径时，调此函数 + gorm.Open。
// 入参 cfgMap 取自 config.GlobalConfig.PersistentConfig["dm8"]。
func BuildDialector(cfgMap map[string]interface{}) (gorm.Dialector, error) {
	cfg := DM8Config{}
	data, err := json.Marshal(cfgMap)
	if err != nil {
		return nil, errors.Wrap(err, "marshal cfgMap")
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshal cfgMap")
	}
	cfg.Normalize()
	dsn := fmt.Sprintf("dm://%s:%s@%s:%d?autoCommit=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)
	return driver.Open(dsn), nil
}
