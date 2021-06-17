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
	IsReplica      bool      `json:"isReplica"`
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
	Storage      Storage           `json:"storage"`
}

// Refers to https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#table_engine-mergetree-multiple-volumes
type Disk struct {
	Name string
	Type string
	//Required parameters if Type="local"
	Path string
	//Optinial parameters if Type="local"
	KeepFreeSpaceBytes int64 `json:"keep_free_space_bytes"`
	//Required parameters if Type="s3" or "hdfs"
	Endpoint string
	//Required parameters if Type="s3"
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"scret_access_key"`
	//Optinial parameters if Type="s3"
	Region                                string
	UseEnvironmentCredentials             bool `json:"use_environment_credentials"`
	UseInsecureImdsRequest                bool `json:"use_insecure_imds_request"`
	Proxy                                 []string
	ConnectTimeoutMs                      int    `json:"connect_timeout_ms"`
	RequestTimeoutMs                      int    `json:"request_timeout_ms"`
	RetryAttempts                         int    `json:"retry_attempts"`
	SingleReadRetries                     int    `json:"single_read_retries"`
	MinBytesForSeek                       int    `json:"min_bytes_for_seek"`
	MetadataPath                          string `json:"metadata_path"`
	CacheEnabled                          bool   `json:"cache_enabled"`
	CachePath                             string `json:"cache_path"`
	SkipAccessCheck                       bool   `json:"skip_access_check"`
	ServerSideEncryptionCustomerKeyBase64 string `json:"server_side_encryption_customer_key_base64"`
}

type Volumn struct {
	Name string
	// Every disk shall be in storage.Disks
	Disks                []string
	MaxDataPartSizeBytes int64  `json:"max_data_part_size_bytes"`
	PreferNotToMerge     string `json:"prefer_not_to_merge"`
}

type Policy struct {
	Name       string
	Volumns    []Volumn
	MoveFactor float32 `json:"move_factor"`
}

type Storage struct {
	Disks    map[string]Disk
	Policies map[string]Policy
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
