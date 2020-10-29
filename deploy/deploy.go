package deploy

// 部署接口，每个组件都需要实现该接口
type Deploy interface {
	Init(base *DeployBase, conf interface{}) error
	Prepare() error
	Install() error
	Config() error
	Start() error
	Check() error
}
