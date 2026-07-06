package model

const (
	RbacAccessReadOnly  = "readonly"
	RbacAccessReadWrite = "readwrite"
)

// RbacUserCreateReq 创建用户请求
type RbacUserCreateReq struct {
	Name     string   `json:"name" example:"alice"`
	Password string   `json:"password" example:"123456"`
	Access   string   `json:"access" example:"readonly"` // readonly | readwrite
	Profile  string   `json:"profile" example:"readonly_p"`
	Networks []string `json:"networks"`
}

// RbacUserAlterReq 修改用户请求,字段均可选,仅修改传入项
type RbacUserAlterReq struct {
	Password string `json:"password"`
	Access   string `json:"access"`
	Profile  string `json:"profile"`
}

// RbacUserInfo 用户信息(List/Get 返回)
type RbacUserInfo struct {
	Name     string   `json:"name"`
	Access   string   `json:"access"`
	Profile  string   `json:"profile"`
	Networks []string `json:"networks"`
}

// RbacProfileConstraint profile 约束
type RbacProfileConstraint struct {
	Readonly bool        `json:"readonly"`
	Min      interface{} `json:"min"`
	Max      interface{} `json:"max"`
}

// RbacProfileCreateReq 创建/修改 profile 请求
type RbacProfileCreateReq struct {
	Name        string                           `json:"name" example:"readonly_p"`
	Settings    map[string]interface{}           `json:"settings"`
	Constraints map[string]RbacProfileConstraint `json:"constraints"`
}

// RbacProfileInfo profile 信息(List/Get 返回)
type RbacProfileInfo struct {
	Name     string                 `json:"name"`
	Settings map[string]interface{} `json:"settings"`
}
