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
	ClickHouseRetainUser  string = "default"

	ClickHouseDefaultDB       string = "default"
	ClickHouseDefaultUser     string = "clickhouse"
	ClickHouseDefaultPassword string = "Ck123456!"
	ClickHouseDefaultPort     int    = 9000
	ClickHouseDefaultHttpPort int    = 8123
	ClickHouseDefaultZkPort   int    = 2181
	ZkStatusDefaultPort       int    = 8080
	SshDefaultPort            int    = 22

	SshPasswordSave      int = 0
	SshPasswordNotSave   int = 1
	SshPasswordUsePubkey int = 2
)

type CkDeployConfig struct {
	Path           string
	User           string
	Password       string
	ZkNodes        []string
	ZkPort         int
	ZkStatusPort   int
	ClusterName    string
	Shards         []CkShard
	PackageVersion string
	CkTcpPort      int
	CkHttpPort     int
	IsReplica      bool
	LogicCluster   *string
	Storage        *Storage
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
	ZkNodes      []string `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort       int      `json:"zkPort" example:"2181"`
	ZkStatusPort int      `json:"zkStatusPort" example:"8080"`
}

type CKManClickHouseConfig struct {
	Version         string  `json:"version"`
	Cluster         string  `json:"cluster"`
	LogicCluster    *string `json:"logic_cluster"`
	Port            int     `json:"port"`
	IsReplica        bool      `json:"isReplica"`
	Hosts            []string  `json:"hosts"`
	Shards           []CkShard `json:"shards"`
	ZkNodes          []string  `json:"zkNodes"`
	ZkPort           int       `json:"zkPort"`
	ZkStatusPort     int       `json:"zkStatusPort"`
	User             string    `json:"user"`
	Password         string    `json:"password"`
	Path             string    `json:"path"`
	SshUser          string    `json:"sshUser"`
	AuthenticateType int       `json:"sshPasswdFlag"`
	SshPassword      string    `json:"sshPassword"`
	SshPort          int       `json:"sshPort"`
	Storage          *Storage
	UsersConf        UsersConf

	// don't need to regist to schema
	Mode     string            `json:"mode"`
	HttpPort int               `json:"httpPort"`
	ZooPath  map[string]string `json:"zooPath"`
}

// Refers to https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-multiple-volumes
type Storage struct {
	Disks    []Disk
	Policies []Policy
}

type Disk struct {
	Name      string
	Type      string
	DiskLocal *DiskLocal
	DiskHdfs  *DiskHdfs
	DiskS3    *DiskS3
}

type DiskLocal struct {
	Path               string
	KeepFreeSpaceBytes *int64
}

type DiskHdfs struct {
	Endpoint string
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
	Name       string
	Volumns    []Volumn
	MoveFactor *float32
}

type Volumn struct {
	Name string
	// Every disk shall be in storage.Disks
	Disks                []string
	MaxDataPartSizeBytes *int64
	PreferNotToMerge     *string
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
	IPs         *[]string
	Hosts       *[]string
	HostRegexps *[]string
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

func (config *CkDeployConfig) Normalize() {
	if config.CkTcpPort == 0 {
		config.CkTcpPort = ClickHouseDefaultPort
	}
	if config.CkHttpPort == 0 {
		config.CkHttpPort = ClickHouseDefaultHttpPort
	}
	if config.ZkPort == 0 {
		config.ZkPort = ClickHouseDefaultZkPort
	}
	if config.ZkStatusPort == 0 {
		config.ZkStatusPort = ZkStatusDefaultPort
	}
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
}
