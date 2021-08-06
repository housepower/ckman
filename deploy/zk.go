package deploy

type ZKDeployFacotry struct{}

func (ZKDeployFacotry) Create() Deploy {
	return &ZKDeploy{}
}

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

func (d *ZKDeploy) Check() error {
	return nil
}
