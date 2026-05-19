package model

type NodeOverrideReq struct {
	Xml string `json:"xml"`
}

type NodeOverrideRsp struct {
	IP  string `json:"ip"`
	Xml string `json:"xml"`
}

type NodeOverrideApplyRsp struct {
	Reloaded bool   `json:"reloaded"`
	Warning  string `json:"warning,omitempty"`
}
