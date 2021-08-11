package deploy

import "github.com/housepower/ckman/common"

type DeployBase struct {
	Hosts            []string
	Packages         []string
	User             string
	Password         string
	AuthenticateType int
	Port             int
	Pool             *common.WorkerPool
}
