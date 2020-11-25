package model

type LoginReq struct {
	Username string `json:"username" example:"ckman"`
	Password string `json:"password" example:"63cb91a2ceb9d4f7c8b1ba5e50046f52"`
}

type LoginRsp struct {
	Username string `json:"username" example:"ckman"`
	Token    string `json:"token" example:"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"`
}
