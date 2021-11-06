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
	StoragePolicy string            `json:"storage_policy"`
}

//https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/mergetree/#mergetree-table-ttl
type CkTableTTL struct {
	TimeCloumn string `json:"time_column"`
	Interval   int    `json:"interval"`
	Unit       string `json:"unit"`
	Action     string `json:"action"`
	Target     string `json:"target"`
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
	Options []string `json:"options"` //example:["DEFAULT now()", "CODEC(NONE)"]
	After   string   `json:"after" example:"_timestamp"`
}

type AlterCkTableReq struct {
	Name    string                 `json:"name" example:"test_table"`
	DB      string                 `json:"database" example:"default"`
	Add     []CkTableNameTypeAfter `json:"add"`
	Modify  []CkTableNameType      `json:"modify"`
	Drop    []string               `json:"drop" example:"age"`
	TTLType string                 `json:"ttl_type"`
	TTL     []CkTableTTL           `json:"ttl"`
}

type AlterCkTableParams struct {
	Name    string
	Cluster string
	DB      string
	Add     []CkTableNameTypeAfter
	Drop    []string
	Modify  []CkTableNameType
	TTLType string
	TTLExpr string
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

type CkTableCost struct {
	Middle       float64 `json:"middle"`
	SecondaryMax float64 `json:"secondaryMax"`
	Max          float64 `json:"max"`
}

type CkUpgradeCkReq struct {
	PackageVersion  string `json:"packageVersion" example:"20.9.3.45"`
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
