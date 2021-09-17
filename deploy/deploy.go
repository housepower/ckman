package deploy

type Deploy interface {
	Init() error
	Prepare() error
	Install() error
	Config() error
	Start() error
	Stop() error
	Restart() error
	Check(timeout int) error
}
