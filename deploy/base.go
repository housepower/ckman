package deploy

import "github.com/housepower/ckman/common"

type DeployBase struct {
	Packages []string
	pool     *common.WorkerPool	/* pool must not be exposed, beacuse will cause gob deepcopy error */
}

func (d *DeployBase)CreatePool(){
	d.pool = common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault)
}
