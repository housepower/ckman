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

func (p *DebPkg)StartCmd(svr string) string{
	return "service " + svr + " start"
}
func (p *DebPkg)StopCmd(svr string) string{
	return "service " + svr + " stop"
}

func (p *DebPkg)RestartCmd(svr string) string {
	return "service " + svr + " restart"
}

func (p *DebPkg)InstallCmd(pkgs []string) string{
	for idx, pkg := range pkgs {
		pkgs[idx] = path.Join(common.TmpWorkDirectory, pkg)
	}
	return "DEBIAN_FRONTEND=noninteractive dpkg -i " + strings.Join(pkgs, " ")
}

func (p *DebPkg) UpgradeCmd(pkgs []string) string {
	for idx, pkg := range pkgs {
		pkgs[idx] = path.Join(common.TmpWorkDirectory, pkg)
	}
	return "DEBIAN_FRONTEND=noninteractive dpkg -i " + strings.Join(pkgs, " ")
}

func (p *DebPkg) Uninstall(pkgs []string) string {
	return "dpkg -P clickhouse-client clickhouse-common-static  clickhouse-server"
}

