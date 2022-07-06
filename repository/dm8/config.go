package dm8

import (
	"github.com/housepower/ckman/common"
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
