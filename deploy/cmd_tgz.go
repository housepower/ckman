package deploy

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/common"
)

type TgzFacotry struct{}

func (TgzFacotry) Create() CmdAdpt {
	return &TgzPkg{}
}

type TgzPkg struct{}

func (p *TgzPkg) StartCmd(svr, cwd string) string {
	return fmt.Sprintf("%s/bin/%s --config-file=%s/etc/%s/config.xml --pid-file=%s/run/%s.pid --daemon", cwd, svr, cwd, svr, cwd, svr)
}
func (p *TgzPkg) StopCmd(svr, cwd string) string {
	return fmt.Sprintf("ps -ef |grep %s/bin/%s |grep -v grep |awk '{print $2}' |xargs kill", cwd, svr)
}

func (p *TgzPkg) RestartCmd(svr, cwd string) string {
	return p.StopCmd(svr, cwd) + ";" + p.StartCmd(svr, cwd)
}

func (p *TgzPkg) InstallCmd(svr string, pkgs Packages) string {
	content := fmt.Sprintf("mkdir -p %s/bin %s/etc/clickhouse-server/config.d %s/etc/clickhouse-server/users.d %s/log/clickhouse-server %s/run %s/data/clickhouse;", pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
	if svr == CkSvrName {
		for _, pkg := range pkgs.PkgLists {
			lastIndex := strings.LastIndex(pkg, "-")
			extractDir := pkg[:lastIndex]
			content += fmt.Sprintf("tar -xvf /tmp/%s -C /tmp;", pkg)
			content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %s/bin;", extractDir, pkgs.Cwd)
			if !strings.Contains(extractDir, common.PkgModuleCommon) {
				content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-* %s/etc/;", extractDir, pkgs.Cwd)
			}
		}
	} else if svr == KeeperSvrName {
		pkg := pkgs.Keeper
		lastIndex := strings.LastIndex(pkg, "-")
		extractDir := pkg[:lastIndex]
		content += fmt.Sprintf("tar -xvf /tmp/%s -C /tmp;", pkg)
		content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %s/bin;", extractDir, pkgs.Cwd)
	}
	return strings.TrimSuffix(content, ";")
}

func (p *TgzPkg) UpgradeCmd(svr string, pkgs Packages) string {
	return p.InstallCmd(svr, pkgs)
}

func (p *TgzPkg) Uninstall(svr string, pkgs Packages, version string) string {
	return fmt.Sprintf("rm -rf %s/*", pkgs.Cwd)
}
