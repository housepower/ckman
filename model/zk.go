package model

type ZkStatusRsp struct {
	Host                string  `json:"host"`
	Version             string  `json:"version"`
	ServerState         string  `json:"server_state"`
	PeerState           string  `json:"peer_state"`
	AvgLatency          float64 `json:"avg_latency"`
	ApproximateDataSize float64 `json:"approximate_data_size"`
	ZnodeCount          float64 `json:"znode_count"`
	OutstandingRequests float64 `json:"outstanding_requests"`
	WatchCount          float64 `json:"watch_count"`
}

type ZkReplicatedTableStatusRsp struct {
	Header [][]string                `json:"header"`
	Tables []ZkReplicatedTableStatus `json:"tables"`
}

type ZkReplicatedTableStatus struct {
	Name   string     `json:"name"`
	Values [][]string `json:"values"`
}
