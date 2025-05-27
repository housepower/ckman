package model

type MetricQueryReq struct {
	Metric string
	Time   int64
}

type MetricQueryRangeReq struct {
	Title  string
	Metric string
	Start  int64
	End    int64
	Step   int64
}

type MetricRsp struct {
	Metric Metric          `json:"metric"`
	Values [][]interface{} `json:"values"`
}

type Metric struct {
	Name     string `json:"__name__"`
	Instance string `json:"instance"`
	Job      string `json:"job"`
}

var MetricMap = map[string]M{
	"Queries/second":            {"metric_log", "avg(ProfileEvent_Query)", 1},
	"Selected Bytes/second":     {"metric_log", "avg(ProfileEvent_SelectedBytes)", 1048576},
	"Selected Rows/second":      {"metric_log", "avg(ProfileEvent_SelectedRows)", 1},
	"Inserted/second":           {"metric_log", "avg(ProfileEvent_InsertQuery)", 1},
	"Inserted Rows/second":      {"metric_log", "avg(ProfileEvent_InsertedRows)", 1},
	"Inserted Bytes/second":     {"metric_log", "avg(ProfileEvent_InsertedBytes)", 1048576},
	"Merged":                    {"metric_log", "avg(ProfileEvent_Merge)", 1},
	"Merged Rows/second":        {"metric_log", "avg(ProfileEvent_MergedRows)", 1},
	"Merged Time":               {"metric_log", "avg(ProfileEvent_MergesTimeMilliseconds)", 1000},
	"Replicated Part Fetches":   {"metric_log", "avg(ProfileEvent_ReplicatedPartFetches)", 1},
	"Replicated Part Merges":    {"metric_log", "avg(ProfileEvent_ReplicatedPartMerges)", 1},
	"Replicated Part Mutations": {"metric_log", "avg(ProfileEvent_ReplicatedPartMutations)", 1},
	"Total MergeTree Parts":     {"asynchronous_metric_log", "TotalPartsOfMergeTreeTables", 1},
	"Max Parts For Partition":   {"asynchronous_metric_log", "MaxPartCountForPartition", 1},
	"CPU Usage (cores)":         {"metric_log", "avg(ProfileEvent_OSCPUVirtualTimeMicroseconds)", 1000000},
	"IO Wait":                   {"metric_log", "avg(ProfileEvent_OSIOWaitMicroseconds)", 1000000},
	"Memory (tracked)":          {"metric_log", "avg(CurrentMetric_MemoryTracking)", 1048576},
	"Network Recive":            {"metric_log", "avg(ProfileEvent_NetworkReceiveBytes)", 1048576},
	"Network Send":              {"metric_log", "avg(ProfileEvent_NetworkSendBytes)", 1048576},
	"Read From Disk":            {"metric_log", "avg(ProfileEvent_OSReadBytes)", 1048576},
	"File Open":                 {"metric_log", "avg(ProfileEvent_FileOpen)", 1},
	"DiskUsed(default)":         {"asynchronous_metric_log", "DiskUsed_default", 1073741824},
	"Read From Filesystem":      {"metric_log", "avg(ProfileEvent_OSReadChars)", 1048576},
	"Load Average (15 minutes)": {"asynchronous_metric_log", "LoadAverage15", 1},
	"Zookeeper Create":          {"metric_log", "avg(ProfileEvent_ZooKeeperCreate)", 1},
	"Zookeeper Remove":          {"metric_log", "avg(ProfileEvent_ZooKeeperRemove)", 1},
	"Zookeeper Wait":            {"metric_log", "avg(ProfileEvent_ZooKeeperWaitMicroseconds)", 1000000},
	"Zookeeper Session":         {"metric_log", "avg(CurrentMetric_ZooKeeperSession)", 1},
	"Zookeeper Bytes Received":  {"metric_log", "avg(ProfileEvent_ZooKeeperBytesReceived)", 1048576},
	"Zookeeper Bytes Sent":      {"metric_log", "avg(ProfileEvent_ZooKeeperBytesSent)", 1048576},
}

type M struct {
	Table string
	Field string
	Unit  int
}
