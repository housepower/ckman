package model

type ZkStatusRsp struct {
	Host                string  `json:"host"`
	Version             string  `json:"version"`
	ServerState         string  `json:"server_state"`
	PeerState           string  `json:"peer_state,omitempty"`
	AvgLatency          float64 `json:"avg_latency"`
	ApproximateDataSize float64 `json:"approximate_data_size"`
	ZnodeCount          float64 `json:"znode_count"`
	OutstandingRequests float64 `json:"outstanding_requests"`
	WatchCount          float64 `json:"watch_count"`
}

type ReplicatedTableStatusRsp struct {
	Header [][]string              `json:"header"`
	Tables []ReplicatedTableStatus `json:"tables"`
}

type ReplicatedTableStatus struct {
	Name   string     `json:"name"`
	Values [][]uint32 `json:"values"`
}

type ReplicatedQueueRsp struct {
	NodeName       string `json:"node_name"`
	Type           string `json:"type"`
	CreateTime     string `json:"create_time"`
	NumTries       uint32 `json:"num_tries"`
	PostponeReason string `json:"postpone_reason"`
	LastException  string `json:"last_exception"`
}
