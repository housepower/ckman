package model

import (
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	CkClusterImport string = "import"
	CkClusterDeploy string = "deploy"

	ClickHouseDefaultDB          string = "default"
	ClickHouseDefaultUser        string = "default"
	ClickHouseDefaultPort        int    = 9000
	ClickHouseDefaultHttpPort    int    = 8123
	ClickHouseDefaultZkPort      int    = 2181
	ZkStatusDefaultPort          int    = 8080
	ClickHouseMericPort          int    = 9363
	ZookeeperMetricPort          int    = 7000
	NodeExporterPort             int    = 9100
	SshDefaultPort               int    = 22
	PromHostDefault              string = "127.0.0.1"
	PromPortDefault              int    = 9090
	PkgTypeDefault               string = "x86_64.rpm"
	ClickHouseUserProfileDefault string = "default"
	ClickHouseUserQuotaDefault   string = "default"
	ClickHouseUserNetIpDefault   string = "::/0"

	SshPasswordSave      int = 0
	SshPasswordNotSave   int = 1
	SshPasswordUsePubkey int = 2

	MaxTimeOut int = 3600

	ClickhouseKeeper string = "clickhouse-keeper"
	Zookeeper        string = "zookeeper"
	ClickHouse       string = "clickhouse"

	KeeperRuntimeStandalone = "standalone"
	KeeperRuntimeInternal   = "internal"
)

type CkDeployExt struct {
	Policy         string
	Ipv6Enable     bool
	Restart        bool
	ChangeCk       bool
	CurClusterOnly bool //仅修改当前集群的配置
	NumCPU         int
}

type CkShard struct {
	Replicas []CkReplica `json:"replicas"`
}

type CkReplica struct {
	Ip       string `json:"ip" example:"192.168.101.105"`
	HostName string `json:"hostname" swaggerignore:"true"`
	Watch    bool   `json:"watch" swaggerignore:"true"`
}

type CkImportConfig struct {
	Hosts        []string `json:"hosts" example:"192.168.101.105,192.168.101.107"`
	Port         int      `json:"port" example:"9000"`
	HttpPort     int      `json:"httpPort" example:"8123"`
	Protocol     string   `json:"protocol" example:"native"`
	Secure       bool     `json:"secure" example:"false"`
	User         string   `json:"user" example:"ck"`
	Password     string   `json:"password" example:"123456"`
	Cluster      string   `json:"cluster" example:"test"`
	LogicCluster string   `json:"logic_cluster" example:"logic_test"`
	ZkNodes      []string `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort       int      `json:"zkPort" example:"2181"`
	PromHost     string   `json:"prom_host" example:"127.0.01"`
	PromPort     int      `json:"prom_port" example:"9090"`
}

type PromMetricPort struct {
	ClickHouse int
	ZooKeeper  int
	NodeExport int
}

type CKManClickHouseConfig struct {
	Cluster          string    `json:"cluster" example:"test"`
	Comment          string    `json:"comment" example:"test"`
	PkgType          string    `json:"pkgType" example:"x86_64.rpm"`
	PkgName          string    `json:"pkgName" example:"clickhouse-common-static-22.3.3.44.noarch.rpm"`
	Version          string    `json:"version" example:"21.9.1.7647"`
	Cwd              string    `json:"cwd" example:"/home/eoi/clickhouse"`
	LogicCluster     *string   `json:"logic_cluster" yaml:"logic_cluster" example:"logic_test"`
	Shards           []CkShard `json:"shards" swaggerignore:"true"`
	Protocol         string    `json:"protocol" yaml:"protocol" example:"native"`
	Port             int       `json:"port" example:"9000"`
	HttpPort         int       `json:"httpPort" example:"8123"`
	Secure           bool      `json:"secure"`
	IsReplica        bool      `json:"isReplica" example:"true"`
	Hosts            []string  `json:"hosts" example:"192.168.0.1,192.168.0.2,192.168.0.3,192.168.0.4"`
	Keeper           string    `json:"keeper" example:"zookeeper"`
	KeeperConf       *KeeperConf
	ZkNodes          []string `json:"zkNodes" example:"192.168.0.1,192.168.0.2,192.168.0.3"`
	ZkPort           int      `json:"zkPort" example:"2181"`
	PromHost         string   `json:"promHost" example:"127.0.0.1"`
	PromPort         int      `json:"promPort" example:"9090"`
	PromMetricPort   PromMetricPort
	User             string `json:"user" example:"ck"`
	Password         string `json:"password" example:"123456"`
	EncryptType      int
	Path             string `json:"path" example:"/var/lib/"`
	SshUser          string `json:"sshUser" example:"root"`
	AuthenticateType int    `json:"authenticateType" example:"0"`
	SshPassword      string `json:"sshPassword" example:"123456"`
	SshPort          int    `json:"sshPort" example:"22"`
	Storage          *Storage
	UsersConf        UsersConf `swaggerignore:"true"`
	Expert           map[string]string

	// don't need to regist to schema
	Mode     string `json:"mode" swaggerignore:"true"`
	NeedSudo bool   `json:"needSudo" swaggerignore:"true"`
}

type Coordination struct {
	OperationTimeoutMs int
	SessionTimeoutMs   int
	ForceSync          bool
	AutoForwarding     bool
	Expert             map[string]string
}

type KeeperConf struct {
	Runtime      string   `json:"runtime" example:"standalone"`
	KeeperNodes  []string `json:"keeperNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	TcpPort      int      `json:"tcpPort" example:"9181"`
	RaftPort     int      `json:"raftPort" example:"9234"`
	LogPath      string
	SnapshotPath string
	Coordination Coordination
	Expert       map[string]string
}

// Refers to https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-multiple-volumes
type Storage struct {
	Disks    []Disk
	Policies []Policy
}

type Disk struct {
	Name          string     `example:"hdfs1"`
	AllowedBackup bool       `example:"true"`
	Type          string     `example:"hdfs"`
	DiskLocal     *DiskLocal `swaggerignore:"true"`
	DiskHdfs      *DiskHdfs
	DiskS3        *DiskS3 `swaggerignore:"true"`
}

type DiskLocal struct {
	Path               string
	KeepFreeSpaceBytes *int64
}

type DiskHdfs struct {
	Endpoint string `example:"hdfs://localhost:8020/clickhouse/data/test/"`
}

type DiskS3 struct {
	Endpoint                  string
	AccessKeyID               string
	SecretAccessKey           string
	Region                    *string
	UseEnvironmentCredentials *bool
	Expert                    map[string]string
}

type Policy struct {
	Name       string `example:"hdfs_only"`
	Volumns    []Volumn
	MoveFactor *float32 `example:"0.1"`
}

type Volumn struct {
	Name string `example:"main"`
	// Every disk shall be in storage.Disks
	Disks                []string `example:"default,hdfs1"`
	MaxDataPartSizeBytes *int64   `example:"10000000"`
	PreferNotToMerge     *string  `example:"true"`
}

// Refers to https://clickhouse.tech/docs/en/operations/settings/settings-users/
type UsersConf struct {
	Users    []User
	Profiles []Profile
	Quotas   []Quota
	Roles    []Role
	Expert   map[string]string
}

type User struct {
	Name         string
	Password     string
	EncryptType  int
	Profile      string        // shall be in Profiles
	Quota        string        // shall be in Quotas
	Roles        string        // shall be in Roles
	Networks     Networks      // List of networks from which the user can connect to the ClickHouse server.
	DbRowPolices []DbRowPolicy // For the given database.table, only rows pass the filter are granted. For other database. tables, all rows are granted.
}

// https://clickhouse.com/docs/en/operations/settings/settings-users#roles
type Role struct {
	Name   string
	Grants Grants
}

type Grants struct {
	Query []string
}

type Networks struct {
	IPs         []string
	Hosts       []string
	HostRegexps []string
}

type DbRowPolicy struct {
	Database       string
	TblRowPolicies []TblRowPolicy
}

type TblRowPolicy struct {
	Table  string
	Filter string // Empty means 0
}

// https://clickhouse.tech/docs/en/operations/settings/settings-profiles/
type Profile struct {
	Name string
	// https://clickhouse.tech/docs/en/operations/settings/permissions-for-queries/
	ReadOnly   int
	AllowDDL   int
	MaxThreads int
	// https://clickhouse.tech/docs/en/operations/settings/query-complexity/
	MaxMemoryUsage              int64
	MaxMemoryUsageForAllQueries int64
	MaxExecutionTime            int64
	Expert                      map[string]string
}

// https://clickhouse.tech/docs/en/operations/quotas/
type Quota struct {
	Name      string
	Intervals []Interval
}

type Interval struct {
	Duration      int64
	Queries       int64
	QuerySelects  int64
	QueryInserts  int64
	Errors        int64
	ResultRows    int64
	ReadRows      int64
	ExecutionTime int64
}

func (config *CKManClickHouseConfig) Normalize() {
	if config.Port == 0 {
		config.Port = ClickHouseDefaultPort
	}
	if config.HttpPort == 0 {
		config.HttpPort = ClickHouseDefaultHttpPort
	}
	if config.ZkPort == 0 {
		config.ZkPort = ClickHouseDefaultZkPort
	}
	if config.SshPort == 0 {
		config.SshPort = SshDefaultPort
	}
	if config.PromHost == "" {
		config.PromHost = PromHostDefault
	}
	if config.PromPort == 0 {
		config.PromPort = PromPortDefault
	}

	if config.PromMetricPort.ClickHouse == 0 {
		config.PromMetricPort.ClickHouse = ClickHouseMericPort
	}
	if config.PromMetricPort.ZooKeeper == 0 {
		config.PromMetricPort.ZooKeeper = ZookeeperMetricPort
	}
	if config.PromMetricPort.NodeExport == 0 {
		config.PromMetricPort.NodeExport = NodeExporterPort
	}

	if config.PkgType == "" {
		config.PkgType = PkgTypeDefault
	}

	if config.Keeper == "" {
		config.Keeper = Zookeeper
	}

	if !strings.HasSuffix(config.PkgType, "tgz") {
		config.Cwd = ""
	}

	if config.Cwd == "" {
		config.NeedSudo = true
	}

	if config.Protocol != clickhouse.HTTP.String() {
		config.Protocol = clickhouse.Native.String()
	}
}

func (config *CKManClickHouseConfig) Watch(host string) {
	if !strings.Contains(config.PkgType, "tgz") {
		return
	}
	for i, shard := range config.Shards {
		for j, replica := range shard.Replicas {
			if host == ALL_NODES_DEFAULT || host == replica.Ip {
				config.Shards[i].Replicas[j].Watch = true
			}
		}
	}
}

func (config *CKManClickHouseConfig) UnWatch(host string) {
	if !strings.Contains(config.PkgType, "tgz") {
		return
	}
	for i, shard := range config.Shards {
		for j, replica := range shard.Replicas {
			if host == ALL_NODES_DEFAULT || host == replica.Ip {
				config.Shards[i].Replicas[j].Watch = false
			}
		}
	}
}

func (config *CKManClickHouseConfig) Pack() {
	config.Password = strings.Repeat("*", len(config.Password))
	if config.SshPassword != "" {
		config.SshPassword = strings.Repeat("*", len(config.SshPassword))
	}
	if len(config.UsersConf.Users) > 0 {
		for idx, user := range config.UsersConf.Users {
			config.UsersConf.Users[idx].Password = strings.Repeat("*", len(user.Password))
		}
	}
}

func (config *CKManClickHouseConfig) UnPack(conf CKManClickHouseConfig) {
	reg := regexp.MustCompile(`^\*{1,}$`)
	if reg.MatchString(config.Password) {
		config.Password = conf.Password
	}
	if reg.MatchString(config.SshPassword) {
		config.SshPassword = conf.SshPassword
	}
	for idx, user := range config.UsersConf.Users {
		if reg.MatchString(user.Password) {
			for _, u := range conf.UsersConf.Users {
				if u.Name == user.Name {
					config.UsersConf.Users[idx].Password = u.Password
				}
			}
		}
	}
}

type ConnetOption struct {
	Protocol           clickhouse.Protocol
	Secure             bool
	InsecureSkipVerify bool
	Port               int
	User               string
	Password           string
}

func (config *CKManClickHouseConfig) GetConnOption() ConnetOption {
	var opt ConnetOption
	if config.Protocol == clickhouse.HTTP.String() {
		opt.Protocol = clickhouse.HTTP
		opt.Port = config.HttpPort
	} else {
		opt.Protocol = clickhouse.Native
		opt.Port = config.Port
	}
	opt.Secure = config.Secure
	opt.InsecureSkipVerify = true // hard code
	opt.User = config.User
	opt.Password = config.Password
	return opt
}

func (config *CKManClickHouseConfig) KeeperWithStanalone() bool {
	if config.Keeper == ClickhouseKeeper {
		return config.KeeperConf != nil && config.KeeperConf.Runtime == KeeperRuntimeStandalone
	}
	return false
}
