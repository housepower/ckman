package deploy

import (
	"github.com/housepower/ckman/common"
	"path"
	"strings"
)

type DebFacotry struct{}

func (DebFacotry) Create() CmdAdpt {
	return &DebPkg{}
}

type DebPkg struct {}

func (p *DebPkg)StartCmd(svr, cwd string) string{
	return "service " + svr + " start"
}
func (p *DebPkg)StopCmd(svr, cwd string) string{
	return "service " + svr + " stop"
}

func (p *DebPkg)RestartCmd(svr, cwd string) string {
	return "service " + svr + " restart"
}

func (p *DebPkg)InstallCmd(pkgs Packages) string{
	for idx, pkg := range pkgs.PkgLists {
		pkgs.PkgLists[idx] = path.Join(common.TmpWorkDirectory, pkg)
	}
	return "DEBIAN_FRONTEND=noninteractive dpkg -i " + strings.Join(pkgs.PkgLists, " ")
}

func (p *DebPkg) UpgradeCmd(pkgs Packages) string {
	for idx, pkg := range pkgs.PkgLists {
		pkgs.PkgLists[idx] = path.Join(common.TmpWorkDirectory, pkg)
	}
	return "DEBIAN_FRONTEND=noninteractive dpkg -i " + strings.Join(pkgs.PkgLists, " ")
}

func (p *DebPkg) Uninstall(pkgs Packages) string {
	return "dpkg -P clickhouse-client clickhouse-common-static  clickhouse-server"
}

