package deploy

import (
	"testing"

	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
)

func TestDeploy(t *testing.T) {
	log.InitLoggerConsole()
	d := NewCkDeploy(model.CKManClickHouseConfig{})
	d.Init()
	d.Prepare()
	d.Install()
	d.Config()
	d.Start()
	d.Check(10)
	d.Upgrade()
	d.Restart()
	d.Stop()
	d.Uninstall()
	StartCkCluster(&model.CKManClickHouseConfig{})
	StopCkCluster(&model.CKManClickHouseConfig{})
}
