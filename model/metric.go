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
	Metric Metric
	Value  [][]interface{}
}

type Metric struct {
	Name     string `json:"__name__"`
	Instance string
	Job      string
}

var MetricMap = map[string]M{
	"CPU Usage (cores)": {"system.metric_log", "avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000"},
}

type M struct {
	Table string
	Field string
}
