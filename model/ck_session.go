package model

type CkSessionInfo struct {
	StartTime     int64  `json:"startTime"`
	QueryDuration uint64 `json:"queryDuration"`
	Query         string `json:"query"`
	User          string `json:"user"`
	QueryId       string `json:"queryId"`
	Address       string `json:"address"`
	Threads       int    `json:"threads"`
	Host          string `json:"host"` //sql running in which node
}

type SessionCond struct {
	StartTime int64
	EndTime   int64
	Limit     int
}

type SessionList []*CkSessionInfo

func (l SessionList) Len() int {
	return len(l)
}

func (l SessionList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l SessionList) Less(i, j int) bool {
	return l[i].QueryDuration > l[j].QueryDuration
}
