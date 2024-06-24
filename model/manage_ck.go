package model

const (
	CkStatusGreen  = "green"
	CkStatusYellow = "yellow"
	CkStatusRed    = "red"
)

type CkClusterInfoRsp struct {
	PkgType      string          `json:"pkgType"`
	Status       string          `json:"status"`
	Version      string          `json:"version"`
	Nodes        []CkClusterNode `json:"nodes"`
	Mode         string          `json:"mode"`
	NeedPassword bool            `json:"needPassword"`
	HttpPort     int             `json:"httpPort"`
}

type CkClusterNode struct {
	Ip            string `json:"ip"`
	HostName      string `json:"hostname"`
	Status        string `json:"status"`
	ShardNumber   int    `json:"shardNumber"`
	ReplicaNumber int    `json:"replicaNumber"`
	Disk          string `json:"disk"`
	Uptime        string `json:"uptime"`
}

type AddNodeReq struct {
	Ips   []string `json:"ips" example:"192.168.0.1,192.168.0.2"`
	Shard int      `json:"shard" example:"3"`
}

type GetLogReq struct {
	LogType string `json:"logType" example:"normal"`
	Lines   uint16 `json:"lines" example:"1000"`
	Tail    bool   `json:"tail" example:"true"`
}

type PingClusterReq struct {
	User     string `json:"user" example:"ck"`
	Password string `json:"password" example:"123456"`
}

type PurgerTableReq struct {
	Database string   `json:"database" example:"default"`
	Tables   []string `json:"tables" example:"t1,t2,t3"`
	Begin    string   `json:"begin" example:"2021-01-01"`
	End      string   `json:"end" example:"2021-04-01"`
}

const (
	ArchiveFmtCSV     = "CSV"
	ArchiveFmtORC     = "ORC"
	ArchiveFmtParquet = "Parquet"

	ArchiveTargetHDFS  = "hdfs"
	ArchiveTargetLocal = "local"
	ArchiveTargetS3    = "s3"
)

type ArchiveHdfs struct {
	Addr string `json:"addr" example:"localhost:8020"`
	User string `json:"user" example:"hdfs"`
	Dir  string `json:"dir" example:"/data01"`
}

type ArchiveLocal struct {
	Path string `json:"Path" example:"/data01/clickhouse/backup/"`
}

type ArchiveS3 struct {
	Endpoint        string `json:"Endpoint" example:"http://192.168.110.8:49000"`
	AccessKeyID     string `json:"AccessKeyID" example:"KZOqVTra982w51MK"`
	SecretAccessKey string `json:"SecretAccessKey" example:"7Zsdaywu7i5C2AyvLkbupSyVlIzP8qJ0"`
	Region          string `json:"Region" example:"zh-west-1"`
	Bucket          string `json:"Bucket" example:"ckman.backup"`
	Compression     string `json:"Compression" example:"gzip"` // none, gzip/gz, brotli/br, xz/LZMA, zstd/zst
}

type ArchiveTableReq struct {
	Database    string       `json:"database" example:"default"`
	Tables      []string     `json:"tables" example:"t1,t2,t3"`
	Begin       string       `json:"begin" example:"2021-01-01"` // include
	End         string       `json:"end" example:"2021-04-01"`   // exclude
	MaxFileSize int          `json:"maxfilesize" example:"10000000000"`
	Format      string       `json:"format" `               // pq, csv, orc
	Target      string       `json:"target" example:"hdfs"` // hdfs, local, remote
	HDFS        ArchiveHdfs  `json:"hdfs"`
	Local       ArchiveLocal `json:"local"`
	S3          ArchiveS3    `json:"s3"`
}

type RebalanceShardingkey struct {
	Database      string   `json:"database" example:"default"`
	Table         string   `json:"table" example:"t123"`
	ShardingKey   string   `json:"shardingKey" example:"_timestamp"`
	AllowLossRate float64  `json:"allowLossRate" example:"0.1"`
	SaveTemps     bool     `json:"saveTemps" example:"true"`
	ShardingType  TypeInfo `json:"-"`
	DistTable     string   `json:"-"`
}

type RebalanceTableReq struct {
	Keys           []RebalanceShardingkey `json:"keys"`
	ExceptMaxShard bool                   `json:"except_max_shard"` // remove the max shard's data to other shards
}

type TypeInfo struct {
	Type     int
	Nullable bool
	Array    bool
}
