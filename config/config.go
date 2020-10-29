package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

var GlobalConfig CKManConfig

type CKManConfig struct {
	Server     CKManServerConfig
	Log        CKManLogConfig
	ClickHouse CKManClickHouseConfig
	Pprof      CKManPprofConfig
}

type CKManServerConfig struct {
	Id    int
	Ip    string
	Port  int
	Https bool
	Peers []string
}

type CKManLogConfig struct {
	MaxCount int `yaml:"max_count"`
	MaxSize  int `yaml:"max_size"`
	MaxAge   int `yaml:"max_age"`
	Level    string
}

type CKManClickHouseConfig struct {
	Hosts    []string
	User     string
	Password string
	DB       string
	Cluster  string
}

type CKManPprofConfig struct {
	Enabled bool
	Ip      string
	Port    int
}

func fillDefault(c *CKManConfig) {
	c.Server.Ip = "0.0.0.0"
	c.Server.Port = 8808
	c.Log.Level = "INFO"
	c.Log.MaxCount = 5
	c.Log.MaxSize = 10
	c.Log.MaxAge = 10
	c.ClickHouse.DB = "default"
	c.Pprof.Enabled = true
	c.Pprof.Ip = "0.0.0.0"
	c.Pprof.Port = 6060
}

func ParseConfigFile(path string) error {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return err
	}

	data, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	fillDefault(&GlobalConfig)
	err = yaml.Unmarshal(data, &GlobalConfig)
	if err != nil {
		return err
	}

	return nil
}
