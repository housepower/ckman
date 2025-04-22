package model

import "time"

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

type ReplicatedTableRsp struct {
	Table         string  `json:"table"`
	Node          string  `json:"node"`
	ShardReplica  string  `json:"shard_replica"`
	QueueSize     uint32  `json:"queue_size"`
	Inserts       uint32  `json:"inserts"`
	Merges        uint32  `json:"merges"`
	LogPointer    uint64  `json:"log_pointer"`
	Progress      float64 `json:"progress"`
	LastException string  `json:"last_exception"`
}

type ReplicatedQueueRsp struct {
	NodeName       string    `json:"node_name"`
	Type           string    `json:"type"`
	CreateTime     time.Time `json:"create_time"`
	NumTries       uint32    `json:"num_tries"`
	PostponeReason string    `json:"postpone_reason"`
	LastException  string    `json:"last_exception"`
}
