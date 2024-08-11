package model

import (
	"time"
)

const (
	TTLActionDelete     string = "DELETE"
	TTLActionToDisk     string = "toDisk"
	TTLActionToVolume   string = "toVolume"
	TTLActionRecompress string = "Recompress"

	TTLTypeModify string = "MODIFY"
	TTLTypeRemove string = "REMOVE"
)

// https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes
type Index struct {
	Name        string
	Field       string
	Type        string //minmax, set, bloom_filter, ngrambf_v1, tokenbf_v1
	Granularity int
}

type CreateCkTableReq struct {
	Name          string            `json:"name" example:"test_table"`
	DistName      string            `json:"dist_table"`
	DB            string            `json:"database" example:"default"`
	Fields        []CkTableNameType `json:"fields"`
	Order         []string          `json:"order" example:"_timestamp"`
	Partition     CkTablePartition  `json:"partition"`
	Distinct      bool              `json:"distinct" example:"true"`
	TTL           []CkTableTTL      `json:"ttl"`
	Indexes       []Index           `json:"indexes"`
	StoragePolicy string            `json:"storage_policy" example:"external"`
	DryRun        bool              `json:"dryrun" example:"false"`
	Projections   []Projection      `json:"projections"`
	ForceCreate   bool              `json:"force_create"`
}

// https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#mergetree-table-ttl
type CkTableTTL struct {
	TimeCloumn string `json:"time_column" example:"_timestamp"`
	Interval   int    `json:"interval" example:"3"`
	Unit       string `json:"unit" example:"MONTH"`
	Action     string `json:"action" example:"toVolume"`
	Target     string `json:"target" example:"main"`
}

type DistLogicTableReq struct {
	Database   string `json:"database" example:"default"`
	LocalTable string `json:"table_name" example:"test_table"`
	DistTable  string `json:"dist_name" example:"dist_table"`
}

type CkTableNameType struct {
	Name    string   `json:"name" example:"_timestamp"`
	Type    string   `json:"type" example:"DateTime"`
	Options []string `json:"options"` //example:["DEFAULT now()", "CODEC(NONE)"]
}

type CkTablePartition struct {
	// 0 split partition by day
	// 1 split partition by week
	// 2 split partition by month
	Policy int    `json:"policy" example:"0"`
	Name   string `json:"name" example:"_timestamp"`
}

const (
	CkTablePartitionPolicyDay int = iota
	CkTablePartitionPolicyWeek
	CkTablePartitionPolicyMonth
	ClickHouseDefaultEngine          string = "MergeTree"
	ClickHouseDefaultReplicaEngine   string = "ReplicatedMergeTree"
	ClickHouseReplacingEngine        string = "ReplacingMergeTree"
	ClickHouseReplicaReplacingEngine string = "ReplicatedReplacingMergeTree"

	PolicyFull    string = "Full"
	PolicyRolling string = "Rolling"
)

type CreateCkTableParams struct {
	Name          string
	DistName      string
	Cluster       string
	Engine        string
	Fields        []CkTableNameType
	Order         []string
	Partition     CkTablePartition
	DB            string
	TTLExpr       string
	IndexExpr     string
	StoragePolicy string
	Projections   []Projection
}

type DeleteCkTableParams struct {
	Name     string
	DistName string
	Cluster  string
	DB       string
}

type DistLogicTblParams struct {
	Database     string
	TableName    string
	DistName     string
	ClusterName  string
	LogicCluster string
}

type CkTableNameTypeAfter struct {
	Name    string   `json:"name" example:"age"`
	Type    string   `json:"type" example:"Int32"`
	Options []string `json:"options" example:"DEFAULT now(),CODEC(NONE),COMMENT,年龄"`
	After   string   `json:"after" example:"_timestamp"`
}

type CkTableRename struct {
	From string `json:"from" example:"col_old"`
	To   string `json:"to" example:"col_new"`
}

const (
	ProjectionAdd   = 1
	ProjectionClear = 2
	ProjectionDrop  = 3
)

type Projection struct {
	Action int
	Name   string
	Sql    string
}

type AlterCkTableReq struct {
	Name        string                 `json:"name" example:"test_table"`
	DistName    string                 `json:"dist_name"`
	DB          string                 `json:"database" example:"default"`
	Add         []CkTableNameTypeAfter `json:"add"`
	Modify      []CkTableNameType      `json:"modify"`
	Drop        []string               `json:"drop" example:"age"`
	Rename      []CkTableRename        `json:"rename"`
	Projections []Projection           `json:"projections"`
	AddIndex    []Index                `json:"add_indexes"`
	DropIndex   []Index                `json:"drop_indexes"`
}

type AlterCkTableParams struct {
	Name        string
	DistName    string
	Cluster     string
	DB          string
	Add         []CkTableNameTypeAfter
	Drop        []string
	Modify      []CkTableNameType
	Rename      []CkTableRename
	Projections []Projection
	AddIndex    []Index
	DropIndex   []Index
}

type AlterTblTTL struct {
	Database  string `json:"database" example:"default"`
	TableName string `json:"tableName" example:"t1"`
	DistName  string `json:"distName" example:"distt1"`
}

type AlterTblsTTLReq struct {
	Tables  []AlterTblTTL `json:"tables"`
	TTLType string        `json:"ttl_type" example:"MODIFY"`
	TTL     []CkTableTTL  `json:"ttl"`
	TTLExpr string        `json:"-"`
}

type DescCkTableParams struct {
	Name string
	DB   string
}

type CkColumnAttribute struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultType       string `json:"defaultType"`
	DefaultExpression string `json:"defaultExpression"`
	Comment           string `json:"comment"`
	CodecExpression   string `json:"codecExpression"`
	TTLExpression     string `json:"ttlExpression"`
}

const (
	DML_UPDATE string = "UPDATE"
	DML_DELETE string = "DELETE"
)

type DMLOnLogicReq struct {
	Database     string
	Table        string
	Manipulation string
	KV           map[string]string
	Cond         string
}

type CkTableMetrics struct {
	Columns      uint64 `json:"columns"`
	Rows         uint64 `json:"rows"`
	Partitions   uint64 `json:"partitions"`
	Parts        uint64 `json:"parts"`
	Compressed   uint64 `json:"compressed"`
	UnCompressed uint64 `json:"uncompressed"`
	RWStatus     bool   `json:"readwrite_status"`
	// CompletedQueries uint64      `json:"completedQueries"`
	// FailedQueries    uint64      `json:"failedQueries"`
	// QueryCost        CkTableCost `json:"queryCost"`
}

type CKTableMerges struct {
	Table           string    `json:"table"`
	Host            string    `json:"host"`
	Elapsed         float64   `json:"elapsed"`
	MergeStart      time.Time `json:"merge_start"`
	Progress        float64   `json:"progress"`
	NumParts        uint64    `json:"num_parts"`
	Rows            uint64    `json:"rows"`
	Compressed      uint64    `json:"compressed"`
	Uncomressed     uint64    `json:"uncompressed"`
	ResultPartName  string    `json:"result_part_name"`
	SourcePartNames string    `json:"source_part_names"`
	MemUsage        uint64    `json:"memory_usage"`
	Algorithm       string    `json:"merge_algorithm"`
}

type PartitionInfo struct {
	Database     string    `json:"database"`
	Table        string    `json:"table"`
	Parts        uint64    `json:"parts"`
	Rows         uint64    `json:"rows"`
	Compressed   uint64    `json:"compressed"`
	UnCompressed uint64    `json:"uncompressed"`
	MinTime      time.Time `json:"min_time"`
	MaxTime      time.Time `json:"max_time"`
	DiskName     string    `json:"disk_name"`
}

type CkTableCost struct {
	Middle       float64 `json:"middle"`
	SecondaryMax float64 `json:"secondaryMax"`
	Max          float64 `json:"max"`
}

type CkUpgradeCkReq struct {
	PackageVersion  string `json:"packageVersion" example:"22.3.6.5"`
	Policy          string `json:"policy" example:"Rolling"`
	SkipSameVersion bool   `json:"skip" example:"true"`
}

type ShowSchemaRsp struct {
	CreateTableQuery string `json:"create_table_query"`
}

type GetConfigRsp struct {
	Mode   string `json:"mode"`
	Config string `json:"config"`
}

type QueryHistory struct {
	Cluster    string
	CreateTime time.Time
	QuerySql   string
	CheckSum   string
}

type OrderbyReq struct {
	Database    string
	Table       string
	DistName    string `json:"dist_name"`
	Orderby     []string
	Partitionby CkTablePartition
}

const (
	OperateCreate int = 1
	OperateUpdate int = 2
	OperateDelete int = 3
)

type MaterializedViewReq struct {
	Name      string `json:"name"`
	Engine    string `json:"engine"`
	Populate  bool   `json:"populate"`
	Database  string `json:"database"`
	Order     []string
	Partition CkTablePartition
	Statement string `json:"statement"`
	Dryrun    bool   `json:"dryrun"`
	Operate   int    `json:"operate"`
}

const (
	GroupUniqArrayPrefix = "gua_"
)

type GroupUniqArrayField struct {
	Name         string //字段名
	MaxSize      int    //聚合条数
	DefaultValue string //默认值
}

type GroupUniqArrayReq struct {
	TimeField string                //时间字段
	Fields    []GroupUniqArrayField //聚合字段
	Database  string                //数据库
	Table     string                //原始表名
	Populate  bool                  //是否要同步存量数据
}
