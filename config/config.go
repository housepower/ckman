package config

import (
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/hjson/hjson-go/v4"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var GlobalConfig CKManConfig
var ClusterNodes []ClusterNode = nil
var ClusterMutex sync.RWMutex

const (
	FORMAT_JSON  string = ".json"
	FORMAT_HJSON string = ".hjson"
	FORMAT_YAML  string = ".yaml"
)

type ClusterNode struct {
	Ip   string `json:"ip"`
	Port int    `json:"port"`
}

type CronJob struct {
	Enabled            bool
	SyncLogicSchema    string `yaml:"sync_logic_schema" json:"sync_logic_schema"`
	WatchClusterStatus string `yaml:"watch_cluster_status" json:"watch_cluster_status"`
	SyncDistSchema     string `yaml:"sync_dist_schema" json:"sync_dist_schema"`
	ClearZnodes        string `yaml:"clear_znodes" json:"clear_znodes"`
}

type CKManConfig struct {
	ConfigFile       string `yaml:"-" json:"-"`
	Server           CKManServerConfig
	ClickHouse       ClickHouseOpts
	Log              CKManLogConfig
	PersistentConfig map[string]map[string]interface{} `yaml:"persistent_config" json:"persistent_config"`
	Nacos            CKManNacosConfig
	Cron             CronJob
	Version          string `yaml:"-" json:"-"`
}

type ClickHouseOpts struct {
	MaxOpenConns    int `yaml:"max_open_conns" json:"max_open_conns"`
	MaxIdleConns    int `yaml:"max_idle_conns" json:"max_idle_conns"`
	ConnMaxIdleTime int `yaml:"conn_max_idle_time" json:"conn_max_idle_time"`
}

type CKManServerConfig struct {
	Ip               string
	Port             int
	Https            bool
	CertFile         string `yaml:"certfile"`
	KeyFile          string `yaml:"keyfile"`
	Pprof            bool
	SessionTimeout   int    `yaml:"session_timeout" json:"session_timeout"`
	SwaggerEnable    bool   `yaml:"swagger_enable" json:"swagger_enable"`
	PublicKey        string `yaml:"public_key" json:"public_key"`
	PersistentPolicy string `yaml:"persistent_policy" json:"persistent_policy"`
	TaskInterval     int    `yaml:"task_interval" json:"task_interval"`
}

type CKManLogConfig struct {
	Level    string
	MaxCount int `yaml:"max_count" json:"max_count"`
	MaxSize  int `yaml:"max_size" json:"max_size"`
	MaxAge   int `yaml:"max_age" json:"max_age"`
}

type CKManNacosConfig struct {
	Enabled     bool
	Hosts       []string
	Port        uint64
	UserName    string `yaml:"user_name" json:"user_name"`
	Password    string
	NamespaceId string `yaml:"namespace_id" json:"namespace_id"`
	Group       string
	DataID      string `yaml:"data_id" json:"data_id"`
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
	c.Cron.Enabled = true
	c.ClickHouse.MaxOpenConns = 10
	c.ClickHouse.MaxIdleConns = 2
	c.ClickHouse.ConnMaxIdleTime = 10
}

func MergeEnv() {
	// 合并环境变量
	if v := os.Getenv("NACOS_HOST"); v != "" {
		hostports := strings.Split(v, ",")
		var hosts []string
		for _, h := range hostports {
			host, port, _ := net.SplitHostPort(h)
			hosts = append(hosts, host)
			GlobalConfig.Nacos.Port, _ = strconv.ParseUint(port, 10, 64)
		}
		GlobalConfig.Nacos.Hosts = hosts
	}
	if v := os.Getenv("HOST_IP"); v != "" {
		GlobalConfig.Server.Ip = v
	}
}

func ParseConfigFile(p, version string) error {
	f, err := os.Open(p)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return errors.Wrap(err, "")
	}

	GlobalConfig.ConfigFile = p
	GlobalConfig.Version = version

	fillDefault(&GlobalConfig)
	configFmt := path.Ext(p)
	switch configFmt {
	case FORMAT_JSON, FORMAT_HJSON:
		err = hjson.Unmarshal(data, &GlobalConfig)
	case FORMAT_YAML:
		err = yaml.Unmarshal(data, &GlobalConfig)
	default:
		return fmt.Errorf("config format %s unsupported yet", configFmt)
	}
	if err != nil {
		return err
	}
	MergeEnv()
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
