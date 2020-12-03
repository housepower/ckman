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
	Ip       string `json:"ip"`
	HostName string `json:"hostname"`
	Status   string `json:"status"`
}

type AddNodeReq struct {
	Ip    string `json:"ip" example:"192.168.101.108"`
	Shard int    `json:"shard" example:"3"`
}
