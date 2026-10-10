package config

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
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
var ConfigMutex sync.RWMutex

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
	Consul           CKManConsulConfig
	Apollo           CKManApolloConfig
	Registry         RegistryConfig     `yaml:"registry" json:"registry"`
	ConfigCenter     ConfigCenterConfig `yaml:"config_center" json:"config_center"`
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
	// BackupMaxConcurrent 数据备份 worker 池并发度（同时执行的 run 数），<=0 时取默认值 8
	BackupMaxConcurrent int `yaml:"backup_max_concurrent" json:"backup_max_concurrent"`
	PkgPath          string `yaml:"pkg_path" json:"pkg_path"`
	Metric           bool   `yaml:"metric" json:"metric"`
	MetricPath       string `yaml:"metric_path" json:"metric_path"`
}

type CKManLogConfig struct {
	Level    string
	MaxCount int `yaml:"max_count" json:"max_count"`
	MaxSize  int `yaml:"max_size" json:"max_size"`
	MaxAge   int `yaml:"max_age" json:"max_age"`
}

type CKManNacosConfig struct {
	Enabled        bool
	Hosts          []string
	Port           uint64
	UserName       string `yaml:"user_name" json:"user_name"`
	Password       string
	NamespaceId    string `yaml:"namespace_id" json:"namespace_id"`
	CfgNamespaceId string `yaml:"cfg_namespace_id" json:"cfg_namespace_id"`
	Group          string
	DataID         string `yaml:"data_id" json:"data_id"`
	BeatInterval   int64  `yaml:"beat_interval" json:"beat_interval"`
	SyncConfig     bool   `yaml:"sync_config" json:"sync_config"`
}

// 注册中心 / 配置中心后端类型。
const (
	DISCOVERY_NONE   string = "none"
	DISCOVERY_NACOS  string = "nacos"
	DISCOVERY_CONSUL string = "consul"
	DISCOVERY_APOLLO string = "apollo"
)

// CKManConsulConfig Consul 连接参数，注册中心与 KV 配置中心共用。
type CKManConsulConfig struct {
	// Address 推荐指向本机 consul agent：服务注册挂在 agent 节点上，直连 server 时
	// 那台 server 宕机会连带丢失注册信息。
	Address    string
	Scheme     string
	Datacenter string
	Token      string
	// CheckInterval 心跳间隔，必须小于 CheckTTL。
	CheckInterval string `yaml:"check_interval" json:"check_interval"`
	CheckTTL      string `yaml:"check_ttl" json:"check_ttl"`
	// DeregisterAfter 实例 critical 超过该时长后由 Consul 从 catalog 删除，防止僵尸实例。
	DeregisterAfter string `yaml:"deregister_after" json:"deregister_after"`
	// WaitTime blocking query 的最长等待时间。
	WaitTime string `yaml:"wait_time" json:"wait_time"`
	// KVKey 配置中心使用的 key，内容格式由 key 后缀决定（.hjson / .json / .yaml）。
	KVKey string `yaml:"kv_key" json:"kv_key"`
}

// CKManApolloConfig Apollo 配置中心参数。
type CKManApolloConfig struct {
	// Meta Config Service 地址，多个用逗号分隔，依次重试。
	Meta    string
	AppId   string `yaml:"app_id" json:"app_id"`
	Cluster string
	// Namespace 必须是 json 或 yaml 类型（如 ckman.json），整份配置存放在其 content 中。
	Namespace string
	Secret    string
}

// RegistryConfig 选择注册中心后端；后端连接参数在各自的配置段（如 nacos）里。
type RegistryConfig struct {
	Type        string
	ServiceName string `yaml:"service_name" json:"service_name"`
}

func (r RegistryConfig) Enabled() bool {
	return r.Type != "" && r.Type != DISCOVERY_NONE
}

// ConfigCenterConfig 选择配置中心后端；后端连接参数在各自的配置段（如 nacos）里。
type ConfigCenterConfig struct {
	Type string
}

func (c ConfigCenterConfig) Enabled() bool {
	return c.Type != "" && c.Type != DISCOVERY_NONE
}

// ConfigWarnings 收集解析配置时产生的告警。解析发生在 logger 初始化之前，由调用方在
// logger 就绪后输出。
var ConfigWarnings []string

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
	c.Server.PkgPath = GetWorkDirectory()
	c.Server.TaskInterval = 5
	c.Server.BackupMaxConcurrent = 8
	c.Server.Metric = true
	c.Server.MetricPath = "/metrics"
	c.Cron.Enabled = true
	c.ClickHouse.MaxOpenConns = 10
	c.ClickHouse.MaxIdleConns = 2
	c.ClickHouse.ConnMaxIdleTime = 10
	c.Nacos.BeatInterval = 5000
	c.Nacos.SyncConfig = true
	c.Consul.Address = "127.0.0.1:8500"
	c.Consul.Scheme = "http"
	c.Consul.CheckInterval = "10s"
	c.Consul.CheckTTL = "30s"
	c.Consul.DeregisterAfter = "1m"
	c.Consul.WaitTime = "30s"
	c.Consul.KVKey = "ckman/ckman.hjson"
	c.Apollo.AppId = "ckman"
	c.Apollo.Cluster = "default"
	c.Apollo.Namespace = "ckman.json"
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
	if v := os.Getenv("CONSUL_ADDR"); v != "" {
		GlobalConfig.Consul.Address = v
	}
	if v := os.Getenv("APOLLO_META"); v != "" {
		GlobalConfig.Apollo.Meta = v
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
	warns, err := normalizeDiscovery(&GlobalConfig)
	if err != nil {
		return err
	}
	ConfigWarnings = warns
	return nil
}

// normalizeDiscovery 由旧的 nacos.enabled / nacos.sync_config 推导 registry / config_center
// 的类型，保证旧配置文件不改也能保持原有行为。显式写了新段时以新段为准。
// 规则见 docs/superpowers/specs/2026-10-09-registry-config-split-design.md。
func normalizeDiscovery(c *CKManConfig) ([]string, error) {
	var warns []string
	explicit := c.Registry.Type != "" || c.ConfigCenter.Type != ""

	// 只显式写了其中一个新段时，另一个仍按旧配置推导；推导成 nacos 时告警，
	// 免得只想换注册中心的用户不知不觉还连着 Nacos 拉配置。
	if c.Registry.Type == "" {
		c.Registry.Type = DISCOVERY_NONE
		if c.Nacos.Enabled {
			c.Registry.Type = DISCOVERY_NACOS
			if explicit {
				warns = append(warns, "registry is not set, derived as nacos from nacos.enabled; set registry.type explicitly")
			}
		}
	}
	if c.ConfigCenter.Type == "" {
		c.ConfigCenter.Type = DISCOVERY_NONE
		if c.Nacos.Enabled && c.Nacos.SyncConfig {
			c.ConfigCenter.Type = DISCOVERY_NACOS
			if explicit {
				warns = append(warns, "config_center is not set, derived as nacos from nacos.enabled/sync_config; set config_center.type explicitly")
			}
		}
	}
	if c.Registry.ServiceName == "" {
		c.Registry.ServiceName = c.Nacos.DataID
		if c.Registry.ServiceName == "" {
			c.Registry.ServiceName = "ckman"
		}
	}

	switch c.Registry.Type {
	case DISCOVERY_NONE, DISCOVERY_NACOS, DISCOVERY_CONSUL:
	default:
		return nil, fmt.Errorf("unsupported registry type %q", c.Registry.Type)
	}
	switch c.ConfigCenter.Type {
	case DISCOVERY_NONE, DISCOVERY_NACOS, DISCOVERY_CONSUL, DISCOVERY_APOLLO:
	default:
		return nil, fmt.Errorf("unsupported config_center type %q", c.ConfigCenter.Type)
	}

	usesNacos := c.Registry.Type == DISCOVERY_NACOS || c.ConfigCenter.Type == DISCOVERY_NACOS
	// 新格式里 nacos.enabled 可以不写（即 false），所以只在它为 true 却用不上时告警。
	if explicit && c.Nacos.Enabled && !usesNacos {
		warns = append(warns, fmt.Sprintf(
			"nacos.enabled=%v is ignored because registry/config_center are set explicitly (registry=%s, config_center=%s)",
			c.Nacos.Enabled, c.Registry.Type, c.ConfigCenter.Type))
	}
	return warns, nil
}

// DiscoveryEndpoints 返回已启用的注册中心 / 配置中心后端地址，key 为后端类型。
// 供 watchdog 做 TCP 探测，不启动 SDK。
func DiscoveryEndpoints(c *CKManConfig) map[string][]string {
	out := make(map[string][]string)
	for _, typ := range []string{c.Registry.Type, c.ConfigCenter.Type} {
		if _, ok := out[typ]; ok {
			continue
		}
		switch typ {
		case DISCOVERY_NACOS:
			port := strconv.FormatUint(c.Nacos.Port, 10)
			var addrs []string
			for _, h := range c.Nacos.Hosts {
				addrs = append(addrs, net.JoinHostPort(strings.TrimSpace(h), port))
			}
			if len(addrs) > 0 {
				out[typ] = addrs
			}
		case DISCOVERY_CONSUL:
			if c.Consul.Address != "" {
				out[typ] = []string{c.Consul.Address}
			}
		case DISCOVERY_APOLLO:
			var addrs []string
			for _, m := range strings.Split(c.Apollo.Meta, ",") {
				if u, err := url.Parse(strings.TrimSpace(m)); err == nil && u.Hostname() != "" {
					port := u.Port()
					if port == "" {
						port = "80"
						if u.Scheme == "https" {
							port = "443"
						}
					}
					addrs = append(addrs, net.JoinHostPort(u.Hostname(), port))
				}
			}
			if len(addrs) > 0 {
				out[typ] = addrs
			}
		}
	}
	return out
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
		if GlobalConfig.Server.Ip != node.Ip || GlobalConfig.Server.Port != node.Port {
			list = append(list, ClusterNodes[index])
		}
	}

	return list
}

// SetClusterNodes 由注册中心的成员变更回调调用：并入 self、去重并按 ip:port 排序后写入
// ClusterNodes。排序保证各节点基于同一份成员列表得出相同的主节点。
func SetClusterNodes(nodes []ClusterNode) {
	self := ClusterNode{Ip: GlobalConfig.Server.Ip, Port: GlobalConfig.Server.Port}
	seen := map[ClusterNode]struct{}{}
	list := make([]ClusterNode, 0, len(nodes)+1)
	for _, n := range append(nodes, self) {
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		list = append(list, n)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Ip != list[j].Ip {
			return list[i].Ip < list[j].Ip
		}
		return list[i].Port < list[j].Port
	})

	ClusterMutex.Lock()
	ClusterNodes = list
	ClusterMutex.Unlock()
}

func IsMasterNode() bool {
	// 未启用注册中心即单机部署，当前节点总是主节点
	if !GlobalConfig.Registry.Enabled() {
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
