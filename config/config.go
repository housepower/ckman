package config

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"gopkg.in/yaml.v3"
)

var GlobalConfig CKManConfig
var ClusterNodes []ClusterNode = nil
var ClusterMutex sync.RWMutex

type ClusterNode struct {
	Ip   string `json:"ip"`
	Port int    `json:"port"`
}

type CronJob struct {
	SyncLogicSchema    string `yaml:"sync_logic_schema"`
	WatchClusterStatus string `yaml:"watch_cluster_status"`
	SyncDistSchema     string `yaml:"sync_dist_schema"`
}

type CKManConfig struct {
	ConfigFile       string `yaml:"-"`
	Server           CKManServerConfig
	Log              CKManLogConfig
	PersistentConfig map[string]map[string]interface{} `yaml:"persistent_config"`
	Nacos            CKManNacosConfig
	Cron             CronJob
	Version          string `yaml:"-"`
}

type CKManServerConfig struct {
	Bind             string
	Ip               string
	Port             int
	Https            bool
	CertFile         string `yaml:"certfile"`
	KeyFile          string `yaml:"keyfile"`
	Pprof            bool
	SessionTimeout   int    `yaml:"session_timeout"`
	SwaggerEnable    bool   `yaml:"swagger_enable"`
	PublicKey        string `yaml:"public_key"`
	PersistentPolicy string `yaml:"persistent_policy"`
	TaskInterval     int    `yaml:"task_interval"`
}

type CKManLogConfig struct {
	Level    string
	MaxCount int `yaml:"max_count"`
	MaxSize  int `yaml:"max_size"`
	MaxAge   int `yaml:"max_age"`
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
	Group     string
	DataID    string `yaml:"data_id"`
}

func fillDefault(c *CKManConfig) {
	c.Server.Port = 8808
	c.Server.SessionTimeout = 3600
	c.Server.Pprof = true
	c.Server.SwaggerEnable = false
	c.Server.PersistentPolicy = "local"
	c.Log.Level = "INFO"
	c.Log.MaxCount = 5
	c.Log.MaxSize = 10
	c.Log.MaxAge = 10
	c.Nacos.Group = "DEFAULT_GROUP"
	c.Nacos.DataID = "ckman"
	c.Server.CertFile = path.Join(GetWorkDirectory(), "conf", "server.crt")
	c.Server.KeyFile = path.Join(GetWorkDirectory(), "conf", "server.key")
	c.Server.TaskInterval = 5
}

func ParseConfigFile(path, version string) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return errors.Wrap(err, "")
	}

	GlobalConfig.ConfigFile = path
	GlobalConfig.Version = version

	fillDefault(&GlobalConfig)
	err = yaml.Unmarshal(data, &GlobalConfig)
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func MarshConfigFile() error {
	out, err := yaml.Marshal(GlobalConfig)
	if err != nil {
		return errors.Wrap(err, "")
	}

	localFd, err := os.OpenFile(GlobalConfig.ConfigFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer localFd.Close()

	if _, err = localFd.Write(out); err != nil {
		return errors.Wrap(err, "")
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

func GetClusterPeers() []ClusterNode {
	list := make([]ClusterNode, 0)

	ClusterMutex.RLock()
	defer ClusterMutex.RUnlock()
	for index, node := range ClusterNodes {
		if GlobalConfig.Server.Ip != node.Ip && GlobalConfig.Server.Port != node.Port {
			list = append(list, ClusterNodes[index])
		}
	}

	return list
}

func IsMasterNode() bool {
	//if disabled nacos, always consider the current node to be the master node
	if !GlobalConfig.Nacos.Enabled {
		return true
	}
	ClusterMutex.RLock()
	defer ClusterMutex.RUnlock()
	if len(ClusterNodes) == 0 {
		return false
	}
	master := ClusterNodes[0]
	return master.Ip == GlobalConfig.Server.Ip && master.Port == GlobalConfig.Server.Port
}
