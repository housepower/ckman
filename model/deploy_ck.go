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
	Hosts      []string       `json:"hosts" example:"192.168.101.105,192.168.101.107"`
	User       string         `json:"user" example:"root"`
	Password   string         `json:"password" example:"123456"`
	ClickHouse CkDeployConfig `json:"clickhouse"`
}

type CkDeployConfig struct {
	Path           string   `json:"path" example:"/data01/"`
	User           string   `json:"user" example:"ck"`
	Password       string   `json:"password" example:"123456"`
	ZkNodes        []string `json:"zkNodes" example:"192.168.101.102,192.168.101.105,192.168.101.107"`
	ZkPort         int      `json:"zkPort" example:"2181"`
	ClusterName    string   `json:"clusterName" example:"test"`
	IsReplica      bool     `json:"isReplica" example:"false"`
	PackageVersion string   `json:"packageVersion" example:"20.8.5.45"`
	CkTcpPort      int      `json:"ckTcpPort" example:"9000"`
}
