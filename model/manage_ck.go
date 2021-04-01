package model

const (
	CkStatusGreen  = "green"
	CkStatusYellow = "yellow"
	CkStatusRed    = "red"
)

type CkClusterInfoRsp struct {
	Status  string          `json:"status"`
	Version string          `json:"version"`
	Nodes   []CkClusterNode `json:"nodes"`
}

type CkClusterNode struct {
	Ip            string `json:"ip"`
	HostName      string `json:"hostname"`
	Status        string `json:"status"`
	ShardNumber   int    `json:"shardNumber"`
	ReplicaNumber int    `json:"replicaNumber"`
}

type AddNodeReq struct {
	Ip    string `json:"ip" example:"192.168.101.108"`
	Shard int    `json:"shard" example:"3"`
}

type PingClusterReq struct {
	Database string `json:"database" example:"default"`
	User     string `json:"user" example:"ck"`
	Password string `json:"password" example:"123456"`
}

type PingClusterRsp struct {
	Message  string   `json:"message"`
	FailList []string `json:"failList"`
}
