package config

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

var GlobalConfig CKManConfig

type CKManConfig struct {
	ConfigFile string `yaml:"-"`
	Server     CKManServerConfig
	Log        CKManLogConfig
	Prometheus CKManPrometheusConfig
	Nacos      CKManNacosConfig
}

type CKManServerConfig struct {
	Id             int
	Bind           string
	Ip             string
	Port           int
	Https          bool
	Pprof          bool
	Peers          []string
	SessionTimeout int `yaml:"session_timeout"`
}

type CKManLogConfig struct {
	Level    string
	MaxCount int `yaml:"max_count"`
	MaxSize  int `yaml:"max_size"`
	MaxAge   int `yaml:"max_age"`
}

type CKManPrometheusConfig struct {
	Hosts   []string
	Timeout int
}

type CKManPprofConfig struct {
	Enabled bool
	Ip      string
	Port    int
}

type CKManNacosConfig struct {
	Enabled   bool
	Hosts     []string
	Port      uint64
	UserName  string `yaml:"user_name"`
	Password  string
	Namespace string
}

func fillDefault(c *CKManConfig) {
	c.Server.Bind = "0.0.0.0"
	c.Server.Port = 8808
	c.Server.SessionTimeout = 3600
	c.Server.Pprof = true
	c.Log.Level = "INFO"
	c.Log.MaxCount = 5
	c.Log.MaxSize = 10
	c.Log.MaxAge = 10
	c.Prometheus.Timeout = 10
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
	GlobalConfig.ConfigFile = path

	return nil
}

func MarshConfigFile() error {
	out, err := yaml.Marshal(GlobalConfig)
	if err != nil {
		return err
	}

	localFd, err := os.OpenFile(GlobalConfig.ConfigFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer localFd.Close()

	if _, err := localFd.Write(out); err != nil {
		return err
	}

	return nil
}

func GetWorkDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(GlobalConfig.ConfigFile))
	if err != nil {
		return ""
	}

	return strings.Replace(filepath.Dir(dir), "\\", "/", -1)
}
