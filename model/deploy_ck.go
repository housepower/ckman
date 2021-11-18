package model

const (
	CkClusterImport       string = "import"
	CkClusterDeploy       string = "deploy"
	CkClientPackagePrefix string = "clickhouse-client"
	CkClientPackageSuffix string = "2.noarch.rpm"
	CkCommonPackagePrefix string = "clickhouse-common-static"
	CkCommonPackageSuffix string = "2.x86_64.rpm"
	CkServerPackagePrefix string = "clickhouse-server"
	CkServerPackageSuffix string = "2.noarch.rpm"

	ClickHouseDefaultDB          string = "default"
	ClickHouseDefaultUser        string = "default"
	ClickHouseDefaultPort        int    = 9000
	ClickHouseDefaultHttpPort    int    = 8123
	ClickHouseDefaultZkPort      int    = 2181
	ZkStatusDefaultPort          int    = 8080
	SshDefaultPort               int    = 22
	PromHostDefault              string = "127.0.0.1"
	PromPortDefault              int    = 9090
	ClickHouseUserProfileDefault string = "default"
	ClickHouseUserQuotaDefault   string = "default"
	ClickHouseUserNetIpDefault   string = "::/0"

	SshPasswordSave      int = 0
	SshPasswordNotSave   int = 1
	SshPasswordUsePubkey int = 2

	MaxTimeOut int = 3600
)

type CkDeployExt struct {
	UpgradePolicy  string
	Ipv6Enable     bool
	Restart        bool
}

type CkShard struct {
	Replicas []CkReplica `json:"replicas"`
}

type CkReplica struct {
	Ip       string `json:"ip" example:"192.168.101.105"`
	HostName string `json:"hostname" swaggerignore:"true"`
}

type CkImportConfig struct {
	Hosts        []string `json:"hosts" example:"192.168.101.105,192.168.101.107"`
	Port         int      `json:"port" example:"9000"`
	HttpPort     int      `json:"httpPort" example:"8123"`
	User         string   `json:"user" example:"ck"`
	Password     string   `json:"password" example:"123456"`
	Cluster      string   `json:"cluster" example:"test"`
	LogicCluster string   `json:"logic_cluster" example:"logic_test"`
	ZkNodes      []string `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort       int      `json:"zkPort" example:"2181"`
	ZkStatusPort int      `json:"zkStatusPort" example:"8080"`
	PromHost     string   `json:"prom_host" example:"127.0.01"`
	PromPort     int      `json:"prom_port" example:"9090"`
}

type CKManClickHouseConfig struct {
	Version          string    `json:"version" example:"21.9.1.7647"`
	Cluster          string    `json:"cluster" example:"test"`
	LogicCluster     *string   `json:"logic_cluster" yaml:"logic_cluster" example:"logic_test"`
	Port             int       `json:"port" example:"9000"`
	IsReplica        bool      `json:"isReplica" example:"true"`
	Hosts            []string  `json:"hosts" example:"192.168.0.1,192.168.0.2,192.168.0.3,192.168.0.4"`
	Shards           []CkShard `json:"shards" swaggerignore:"true"`
	ZkNodes          []string  `json:"zkNodes" example:"192.168.0.1,192.168.0.2,192.168.0.3"`
	ZkPort           int       `json:"zkPort" example:"2181"`
	ZkStatusPort     int       `json:"zkStatusPort" example:"8080"`
	PromHost         string    `json:"promHost" example:"127.0.0.1"`
	PromPort         int       `json:"promPort" example:"9090"`
	User             string    `json:"user" example:"ck"`
	Password         string    `json:"password" example:"123456"`
	Path             string    `json:"path" example:"/var/lib/"`
	SshUser          string    `json:"sshUser" example:"root"`
	AuthenticateType int       `json:"authenticateType" example:"0"`
	SshPassword      string    `json:"sshPassword" example:"123456"`
	SshPort          int       `json:"sshPort" example:"22"`
	Storage          *Storage
	MergeTreeConf    *MergeTreeConf
	UsersConf        UsersConf `swaggerignore:"true"`

	// don't need to regist to schema
	Mode     string            `json:"mode" swaggerignore:"true"`
	HttpPort int               `json:"httpPort" swaggerignore:"true"`
	ZooPath  map[string]string `json:"zooPath" swaggerignore:"true"`
}

type MergeTreeConf struct {
	Expert map[string]string
}

// Refers to https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-multiple-volumes
type Storage struct {
	Disks    []Disk
	Policies []Policy
}

type Disk struct {
	Name      string     `example:"hdfs1"`
	Type      string     `example:"hdfs"`
	DiskLocal *DiskLocal `swaggerignore:"true"`
	DiskHdfs  *DiskHdfs
	DiskS3    *DiskS3 `swaggerignore:"true"`
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
}

type User struct {
	Name         string
	Password     string
	Profile      string        // shall be in Profiles
	Quota        string        // shall be in Quotas
	Networks     Networks      // List of networks from which the user can connect to the ClickHouse server.
	DbRowPolices []DbRowPolicy // For the given database.table, only rows pass the filter are granted. For other database. tables, all rows are granted.
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
	ReadOnly   *int
	AllowDDL   *int
	MaxThreads *int
	// https://clickhouse.tech/docs/en/operations/settings/query-complexity/
	MaxMemoryUsage              *int64
	MaxMemoryUsageForAllQueries *int64
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
	if config.ZkStatusPort == 0 {
		config.ZkStatusPort = ZkStatusDefaultPort
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
}
