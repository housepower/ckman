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
)

type DeployCkReq struct {
	Hosts      []string       `json:"hosts" example:"192.168.101.105"`
	User       string         `json:"user" example:"root"`
	Password   string         `json:"password" example:"123456"`
	ClickHouse CkDeployConfig `json:"clickhouse"`
}

type CkDeployConfig struct {
	Path           string    `json:"path" example:"/data01/"`
	User           string    `json:"user" example:"ck"`
	Password       string    `json:"password" example:"123456"`
	ZkNodes        []string  `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort         int       `json:"zkPort" example:"2181"`
	ClusterName    string    `json:"clusterName" example:"test"`
	Shards         []CkShard `json:"shards"`
	PackageVersion string    `json:"packageVersion" example:"20.8.5.45"`
	CkTcpPort      int       `json:"ckTcpPort" example:"9000"`
}

type CkShard struct {
	Replicas []CkReplica `json:"replicas"`
}

type CkReplica struct {
	Ip       string `json:"ip" example:"192.168.101.105"`
	HostName string `json:"hostname" swaggerignore:"true"`
}

type CkImportConfig struct {
	Hosts    []string `json:"hosts" example:"192.168.101.105,192.168.101.107"`
	Port     int      `json:"port" example:"9000"`
	User     string   `json:"user" example:"ck"`
	Password string   `json:"password" example:"123456"`
	Cluster  string   `json:"cluster" example:"test"`
	ZkNodes  []string `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort   int      `json:"zkPort" example:"2181"`
}

type CKManClickHouseConfig struct {
	Mode        string    `json:"mode"`
	Hosts       []string  `json:"hosts"`
	Names       []string  `json:"names"`
	Port        int       `json:"port"`
	User        string    `json:"user"`
	Password    string    `json:"password"`
	DB          string    `json:"database"`
	Cluster     string    `json:"cluster"`
	ZkNodes     []string  `json:"zkNodes"`
	ZkPort      int       `json:"zkPort"`
	IsReplica   bool      `json:"isReplica"`
	Version     string    `json:"version"`
	SshUser     string    `json:"sshUser"`
	SshPassword string    `json:"sshPassword"`
	Shards      []CkShard `json:"shards"`
	Path        string    `json:"path"`
}
