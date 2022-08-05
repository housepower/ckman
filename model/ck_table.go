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

type CreateCkTableReq struct {
	Name          string            `json:"name" example:"test_table"`
	DB            string            `json:"database" example:"default"`
	Fields        []CkTableNameType `json:"fields"`
	Order         []string          `json:"order" example:"_timestamp"`
	Partition     CkTablePartition  `json:"partition"`
	Distinct      bool              `json:"distinct" example:"true"`
	TTL           []CkTableTTL      `json:"ttl"`
	StoragePolicy string            `json:"storage_policy" example:"external"`
	DryRun        bool              `json:"dryrun" example:"false"`
}

//https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#mergetree-table-ttl
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

	UpgradePolicyFull    string = "Full"
	UpgradePolicyRolling string = "Rolling"
)

type CreateCkTableParams struct {
	Name          string
	Cluster       string
	Engine        string
	Fields        []CkTableNameType
	Order         []string
	Partition     CkTablePartition
	DB            string
	TTLExpr       string
	StoragePolicy string
}

type DeleteCkTableParams struct {
	Name    string
	Cluster string
	DB      string
}

type DistLogicTblParams struct {
	Database     string
	TableName    string
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
type AlterCkTableReq struct {
	Name   string                 `json:"name" example:"test_table"`
	DB     string                 `json:"database" example:"default"`
	Add    []CkTableNameTypeAfter `json:"add"`
	Modify []CkTableNameType      `json:"modify"`
	Drop   []string               `json:"drop" example:"age"`
	Rename []CkTableRename        `json:"rename"`
}

type AlterCkTableParams struct {
	Name    string
	Cluster string
	DB      string
	Add     []CkTableNameTypeAfter
	Drop    []string
	Modify  []CkTableNameType
	Rename  []CkTableRename
}

type AlterTblsTTLReq struct {
	Tables []struct {
		Database  string `json:"database" example:"default"`
		TableName string `json:"tableName" example:"t1"`
	} `json:"tables"`
	TTLType string       `json:"ttl_type" example:"MODIFY"`
	TTL     []CkTableTTL `json:"ttl"`
	TTLExpr string       `json:"-"`
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

type CkTableMetrics struct {
	Columns          uint64      `json:"columns"`
	Rows             uint64      `json:"rows"`
	Partitions       uint64      `json:"partitions"`
	Parts            uint64      `json:"parts"`
	Compressed       uint64      `json:"compressed"`
	UnCompressed     uint64      `json:"uncompressed"`
	RWStatus         bool        `json:"readwrite_status"`
	CompletedQueries uint64      `json:"completedQueries"`
	FailedQueries    uint64      `json:"failedQueries"`
	QueryCost        CkTableCost `json:"queryCost"`
}

type PartitionInfo struct {
	Database     string    `json:"database"`
	Table        string    `json:"table"`
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
