package deploy

import (
	"fmt"
	"path"
	"strings"

	"github.com/housepower/ckman/common"
)

type RpmFacotry struct{}

const (
	rpmPrefix string = "DEBIAN_FRONTEND=noninteractive rpm --force --nosignature --nodeps"
)

func (RpmFacotry) Create() CmdAdpt {
	return &RpmPkg{}
}

type RpmPkg struct{}

func (p *RpmPkg) StartCmd(svr, cwd string) string {
	return "systemctl start " + svr
}
func (p *RpmPkg) StopCmd(svr, cwd string) string {
	return "systemctl stop " + svr
}

func (p *RpmPkg) RestartCmd(svr, cwd string) string {
	return "systemctl restart " + svr
}

func (p *RpmPkg) InstallCmd(svr string, pkgs Packages) string {
	var cmd string
	if svr == CkSvrName {
		for _, pkg := range pkgs.PkgLists {
			cmd += fmt.Sprintf("%s -ivh %s;", rpmPrefix, path.Join(common.TmpWorkDirectory, pkg))
		}
	} else if svr == KeeperSvrName {
		cmd += fmt.Sprintf("%s -ivh %s;", rpmPrefix, path.Join(common.TmpWorkDirectory, pkgs.Keeper))
	}
	return strings.TrimSuffix(cmd, ";")
}

func (p *RpmPkg) UpgradeCmd(svr string, pkgs Packages) string {
	var cmd string
	if svr == CkSvrName {
		for _, pkg := range pkgs.PkgLists {
			cmd += fmt.Sprintf("%s -Uvh %s;", rpmPrefix, path.Join(common.TmpWorkDirectory, pkg))
		}
	} else if svr == KeeperSvrName {
		cmd += fmt.Sprintf("%s -Uvh %s;", rpmPrefix, path.Join(common.TmpWorkDirectory, pkgs.Keeper))
	}
	return strings.TrimSuffix(cmd, ";")
}

func (p *RpmPkg) Uninstall(svr string, pkgs Packages, version string) string {
	if svr == KeeperSvrName {
		return fmt.Sprintf("rpm -e $(rpm -qa |grep clickhouse-keeper |grep %s)", version)
	} else {
		return fmt.Sprintf("rpm -e $(rpm -qa |grep clickhouse |grep %s)", version)
	}
}
