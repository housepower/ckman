package model

const (
	CkClientPackagePrefix string = "clickhouse-client"
	CkClientPackageSuffix string = "2.noarch.rpm"
	CkCommonPackagePrefix string = "clickhouse-common-static"
	CkCommonPackageSuffix string = "2.x86_64.rpm"
	CkServerPackagePrefix string = "clickhouse-server"
	CkServerPackageSuffix string = "2.noarch.rpm"
)

type DeployCkReq struct {
	Hosts      []string
	User       string
	Password   string
	Directory  string
	ClickHouse CkDeployConfig
}

type CkDeployConfig struct {
	Path           string
	User           string
	Password       string
	ZkServers      []ZkNode
	ClusterName    string
	IsReplica      bool
	PackageVersion string
	CkTcpPort      int
}

type ZkNode struct {
	Host string
	Port int
}
