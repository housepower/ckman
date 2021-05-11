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
)

type DeployCkReq struct {
	Hosts      []string       `json:"hosts" example:"192.168.101.105"`
	User       string         `json:"user" example:"root"`
	Password   string         `json:"password" example:"123456"`
	Port       int            `json:"sshPort" example:"22"`
	ClickHouse CkDeployConfig `json:"clickhouse"`
}

type CkDeployConfig struct {
	Path           string    `json:"path" example:"/data01/"`
	User           string    `json:"user" example:"ck"`
	Password       string    `json:"password" example:"123456"`
	ZkNodes        []string  `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort         int       `json:"zkPort" example:"2181"`
	ZkStatusPort   int       `json:"zkStatusPort" example:"8080"`
	ClusterName    string    `json:"clusterName" example:"test"`
	Shards         []CkShard `json:"shards"`
	PackageVersion string    `json:"packageVersion" example:"20.8.5.45"`
	CkTcpPort      int       `json:"ckTcpPort" example:"9000"`
	CkHttpPort     int       `json:"ckHttpPort" example:"8123"`
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
	Mode         string            `json:"mode"`
	Hosts        []string          `json:"hosts"`
	Names        []string          `json:"names"`
	Port         int               `json:"port"`
	HttpPort     int               `json:"httpPort"`
	User         string            `json:"user"`
	Password     string            `json:"password"`
	Cluster      string            `json:"cluster"`
	ZkNodes      []string          `json:"zkNodes"`
	ZkPort       int               `json:"zkPort"`
	ZkStatusPort int               `json:"zkStatusPort"`
	IsReplica    bool              `json:"isReplica"`
	Version      string            `json:"version"`
	SshUser      string            `json:"sshUser"`
	SshPassword  string            `json:"sshPassword"`
	SshPort      int               `json:"sshPort"`
	Shards       []CkShard         `json:"shards"`
	Path         string            `json:"path"`
	ZooPath      map[string]string `json:"zooPath"`
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
