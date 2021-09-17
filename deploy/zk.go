package deploy

type ZKDeployFacotry struct{}

type ZKDeploy struct {
	DeployBase
}

func (d *ZKDeploy) Init() error {
	return nil
}

func (d *ZKDeploy) Prepare() error {
	return nil
}

func (d *ZKDeploy) Install() error {
	return nil
}

func (d *ZKDeploy) Config() error {
	return nil
}

func (d *ZKDeploy) Start() error {
	return nil
}

func (d *ZKDeploy) Stop() error {
	return nil
}

func (d *ZKDeploy) Restart() error {
	return nil
}

func (d *ZKDeploy) Check(timeout int) error {
	return nil
}
