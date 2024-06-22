package deploy

import (
	"path"
	"strings"

	"github.com/housepower/ckman/common"
)

type DebFacotry struct{}

func (DebFacotry) Create() CmdAdpt {
	return &DebPkg{}
}

type DebPkg struct{}

func (p *DebPkg) StartCmd(svr, cwd string) string {
	return "service " + svr + " start"
}
func (p *DebPkg) StopCmd(svr, cwd string) string {
	return "service " + svr + " stop"
}

func (p *DebPkg) RestartCmd(svr, cwd string) string {
	return "service " + svr + " restart"
}

func (p *DebPkg) InstallCmd(svr string, pkgs Packages) string {
	if svr == CkSvrName {
		for idx, pkg := range pkgs.PkgLists {
			pkgs.PkgLists[idx] = path.Join(common.TmpWorkDirectory, pkg)
		}
		return "DEBIAN_FRONTEND=noninteractive dpkg -i " + strings.Join(pkgs.PkgLists, " ")
	} else {
		return "dpkg -i " + pkgs.Keeper
	}
}

func (p *DebPkg) UpgradeCmd(svr string, pkgs Packages) string {
	return p.InstallCmd(svr, pkgs)
}

func (p *DebPkg) Uninstall(svr string, pkgs Packages, version string) string {
	if svr == CkSvrName {
		return "dpkg -P clickhouse-client clickhouse-common-static clickhouse-server"
	} else {
		return "dpkg -P clickhouse-keeper"
	}
}
