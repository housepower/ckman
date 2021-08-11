package deploy

type Deploy interface {
	Init() error
	Prepare() error
	Install() error
	Config() error
	Start() error
	Check() error
}
