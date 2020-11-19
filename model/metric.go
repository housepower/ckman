package model

type MetricQueryReq struct {
	Metric string
	Time   int64
}

type MetricQueryRangeReq struct {
	Metric string
	Start  int64
	End    int64
	Step   int64
}
