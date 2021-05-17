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
