package deploy

type Deploy interface {
	Init(base *DeployBase, conf interface{}) error
	Prepare() error
	Install() error
	Config() error
	Start() error
	Check() error
}
