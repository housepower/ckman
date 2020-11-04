package model

type LoginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginRsp struct {
	Username string `json:"username"`
	Token    string `json:"token"`
}
