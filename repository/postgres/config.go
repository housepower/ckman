package postgres

import (
	"github.com/housepower/ckman/common"
)

type PostgresConfig struct {
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

func (config *PostgresConfig) Normalize() {
	config.Port = common.GetIntegerwithDefault(config.Port, PG_PORT_DEFAULT)
	config.DataBase = common.GetStringwithDefault(config.DataBase, PG_DATABASE_DEFAULT)
	config.MaxIdleConns = common.GetIntegerwithDefault(config.MaxIdleConns, PG_MAX_IDLE_CONNS_DEFAULT)
	config.MaxOpenConns = common.GetIntegerwithDefault(config.MaxOpenConns, PG_MAX_OPEN_CONNS_DEFAULT)
	config.ConnMaxLifetime = common.GetIntegerwithDefault(config.ConnMaxLifetime, PG_MAX_LIFETIME_DEFAULT)
	config.ConnMaxIdleTime = common.GetIntegerwithDefault(config.ConnMaxIdleTime, PG_MAX_IDLE_TIME_DEFAULT)
	_ = common.Gsypt.Unmarshal(&config)
}
