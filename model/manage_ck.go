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
	Cpu           string `json:"cpu"`
	Memory        string `json:"memory"`
}

type AddNodeReq struct {
	Ips              []string `json:"ips" example:"192.168.0.1,192.168.0.2"`
	Shard            int      `json:"shard" example:"3"`
	SourceSchemaHost string   `json:"sourceSchemaHost" example:"192.168.0.10"`
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

const (
	RebalancePolicyPartition   = "partition"
	RebalancePolicyShardingKey = "shardingkey"
)

type RebalanceTables struct {
	Database      string   `json:"database" example:"default"`
	Table         string   `json:"table" example:"t123"`
	Policy        string   `json:"policy" example:"patition, shardingkey"`
	ShardingKey   string   `json:"shardingKey" example:"_timestamp"`
	AllowLossRate float64  `json:"allowLossRate" example:"0.1"`
	SaveTemps     bool     `json:"saveTemps" example:"true"`
	ShardingType  TypeInfo `json:"-"`
	DistTable     string   `json:"-"`
}

// TableRebalanceInfo is the per-table view returned by the rebalance_info
// endpoint. Replaces the older flat RebalanceInfo, which forced the frontend
// to re-group rows client-side and gave no per-table summary fields. Shards
// is the per-shard breakdown; the top-level fields are table-wide summaries
// (engine, imbalance, warnings) so the UI can render at-a-glance status.
//
// Warnings carry ZH+EN text so the UI doesn't have to localize string-keyed
// messages — matches the existing pattern used by Task.Step / NodeStatus.
type TableRebalanceInfo struct {
	Database       string                 `json:"database" example:"default"`
	Table          string                 `json:"table" example:"t123"`
	DistTable      string                 `json:"dist_table,omitempty" example:"t123_all"`
	Engine         string                 `json:"engine" example:"ReplicatedMergeTree"`
	ImbalanceRatio float64                `json:"imbalance_ratio" example:"0.23"`
	Warnings       []Internationalization `json:"warnings,omitempty"`
	Shards         []ShardRebalanceInfo   `json:"shards"`
}

// ShardRebalanceInfo carries one shard's data footprint for a single table.
// Bytes is uncompressed (matches the original "size" semantic); CompressedBytes
// is the on-disk footprint, useful for capacity / move-cost estimation.
type ShardRebalanceInfo struct {
	Host            string `json:"host" example:"192.168.0.1"`
	ShardNum        int    `json:"shard_num" example:"1"`
	Rows            uint64 `json:"rows" example:"10000000000"`
	Bytes           uint64 `json:"bytes" example:"10737418240"`
	CompressedBytes uint64 `json:"compressed_bytes" example:"2147483648"`
	PartitionCount  uint64 `json:"partition_count" example:"365"`
}

type RebalanceTableReq struct {
	RTables        []RebalanceTables `json:"tables"`
	ExceptMaxShard bool              `json:"except_max_shard"` // remove the max shard's data to other shards
}

// RebalancePlan is the preview returned by /rebalance_plan: what the
// rebalance WOULD do given the cluster's current state. Computed off the same
// code path as a real Run (paddingKeys + Strategy.Plan), but stops before any
// data is actually moved. UI uses this for a "confirm-before-execute" step.
type RebalancePlan struct {
	// ExceptShardDrain is populated when except_max_shard is true: the last
	// shard's data will be drained into the host with most free space before
	// the per-table strategy runs on the remaining shards.
	ExceptShardDrain *ExceptShardDrain `json:"except_shard_drain,omitempty"`
	Tables           []TablePlan       `json:"tables"`
}

type ExceptShardDrain struct {
	SrcHost string `json:"src_host"`
	DstHost string `json:"dst_host"`
}

// TablePlan describes what one table's rebalance will do. For the partition
// strategy this is a concrete list of partition moves; for the shardingkey
// strategy it's a summary of the reshuffle's scale (the actual row movement
// is computed at run time by the cluster() insert).
type TablePlan struct {
	Database  string                 `json:"database"`
	Table     string                 `json:"table"`
	Strategy  string                 `json:"strategy"` // "partition" | "shardingkey"
	Engine    string                 `json:"engine"`
	Moves     []PartitionMove        `json:"moves,omitempty"`
	Reshuffle *ReshuffleSummary      `json:"reshuffle,omitempty"`
	Warnings  []Internationalization `json:"warnings,omitempty"`
}

// PartitionMove is one (src → dst) hop for one partition under the partition
// strategy. Bytes is compressed (the actual transfer size for the FETCH /
// rsync paths).
type PartitionMove struct {
	Partition string `json:"partition"`
	SrcHost   string `json:"src_host"`
	DstHost   string `json:"dst_host"`
	Bytes     uint64 `json:"bytes"`
}

// ReshuffleSummary describes the shardingkey strategy's scale: how much data
// will be moved through the tmp table and on what key. The UI uses this to
// estimate execution time / disk footprint requirements.
type ReshuffleSummary struct {
	TotalRows       uint64 `json:"total_rows"`
	TotalBytes      uint64 `json:"total_bytes"`      // uncompressed
	CompressedBytes uint64 `json:"compressed_bytes"` // estimated tmp disk usage
	Shards          int    `json:"shards"`
	ShardingKey     string `json:"sharding_key"`
}

type TypeInfo struct {
	Type     int
	Nullable bool
	Array    bool
}
