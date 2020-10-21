package model

type CreateCkTableReq struct {
	Name      string
	Fields    []CkTableNameType
	Order     []string
	Partition CkTablePartition
}

type CkTableNameType struct {
	Name string
	Type string
}

type CkTablePartition struct {
	Policy int
	Name   string
}

const (
	CkTablePartitionPolicyDay int = iota
	CkTablePartitionPolicyWeek
	CkTablePartitionPolicyMonth
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
	Name  string
	Type  string
	After string
}

type AlterCkTableReq struct {
	Name   string
	Add    []CkTableNameTypeAfter
	Drop   []string
	Modify []CkTableNameType
}

type AlterCkTableParams struct {
	Name    string
	Cluster string
	DB      string
	Add     []CkTableNameTypeAfter
	Drop    []string
	Modify  []CkTableNameType
}
