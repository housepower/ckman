package model

type UpdateConfigReq struct {
	Peers         []string `json:"peers" example:"192.168.21.74"`
	Prometheus    []string `json:"prometheus" example:"192.168.101.105:19090"`
	AlertManagers []string `json:"alertManagers" example:"192.168.101.105:19093"`
}
