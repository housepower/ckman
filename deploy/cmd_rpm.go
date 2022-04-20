package deploy

import (
	"fmt"
	"github.com/housepower/ckman/common"
	"path"
	"strings"
)

type RpmFacotry struct{}

const (
	rpmPrefix string = "DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps"
)

func (RpmFacotry) Create() CmdAdpt {
	return &RpmPkg{}
}

type RpmPkg struct {}

func (p *RpmPkg)StartCmd(svr string) string{
	return "systemctl start " + svr
}
func (p *RpmPkg)StopCmd(svr string) string{
	return "systemctl stop " + svr
}

func (p *RpmPkg)RestartCmd(svr string) string {
	return "systemctl restart " + svr
}

func (p *RpmPkg)InstallCmd(pkgs []string) string{
	var cmd string
	for _, pkg := range pkgs {
		cmd += fmt.Sprintf("%s -ivh %s;", rpmPrefix, path.Join(common.TmpWorkDirectory, pkg))
	}
	return strings.TrimSuffix(cmd, ";")
}

func (p *RpmPkg) UpgradeCmd(pkgs []string) string {
	var cmd string
	for _, pkg := range pkgs {
		cmd += fmt.Sprintf("%s -Uvh %s;", rpmPrefix, path.Join(common.TmpWorkDirectory, pkg))
	}
	return strings.TrimSuffix(cmd, ";")
}

func (p *RpmPkg) Uninstall(pkgs []string) string {
	return "rpm -e " + strings.Join(pkgs, " ")
}

