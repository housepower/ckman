package model

type CreateCkTableReq struct {
	Name      string            `json:"name" example:"test_table"`
	DB        string            `json:"database" example:"default"`
	Fields    []CkTableNameType `json:"fields"`
	Order     []string          `json:"order" example:"_timestamp"`
	Partition CkTablePartition  `json:"partition"`
}

type CkTableNameType struct {
	Name string `json:"name" example:"_timestamp"`
	Type string `json:"type" example:"DateTime"`
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
	ClickHouseDefaultEngine        string = "ReplacingMergeTree"
	ClickHouseReplicaDefaultEngine string = "ReplicatedReplacingMergeTree"
)

type CreateCkTableParams struct {
	Name      string
	Cluster   string
	Engine    string
	Fields    []CkTableNameType
	Order     []string
	Partition CkTablePartition
	DB        string
}

type DeleteCkTableParams struct {
	Name    string
	Cluster string
	DB      string
}

type CkTableNameTypeAfter struct {
	Name  string `json:"name" example:"age"`
	Type  string `json:"type" example:"Int32"`
	After string `json:"after" example:"_timestamp"`
}

type AlterCkTableReq struct {
	Name   string `json:"name" example:"test_table"`
	DB     string `json:"database" example:"default"`
	Add    []CkTableNameTypeAfter
	Modify []CkTableNameType `json:"modify"`
	Drop   []string          `json:"drop" example:"age"`
}

type AlterCkTableParams struct {
	Name    string
	Cluster string
	DB      string
	Add     []CkTableNameTypeAfter
	Drop    []string
	Modify  []CkTableNameType
}

type DescCkTableParams struct {
	Name string
	DB   string
}

type CkTableAttribute struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	DefaultType       string `json:"defaultType"`
	DefaultExpression string `json:"defaultExpression"`
	Comment           string `json:"comment"`
	CodecExpression   string `json:"codecExpression"`
	TTLExpression     string `json:"ttlExpression"`
}
