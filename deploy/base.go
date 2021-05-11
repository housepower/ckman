package deploy

import "github.com/housepower/ckman/common"

type DeployBase struct {
	Hosts     []string
	Packages  []string
	User      string
	Password  string
	Port      int
	Directory string
	Pool      *common.WorkerPool
}
