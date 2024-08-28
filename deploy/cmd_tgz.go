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
	if svr == KeeperSvrName {
		return fmt.Sprintf("%sbin/%s --config-file=%setc/%s/keeper_config.xml --pid-file=%srun/%s.pid --daemon", cwd, svr, cwd, svr, cwd, svr)
	} else {
		return fmt.Sprintf("%sbin/%s --config-file=%setc/%s/config.xml --pid-file=%srun/%s.pid --daemon", cwd, svr, cwd, svr, cwd, svr)
	}
}
func (p *TgzPkg) StopCmd(svr, cwd string) string {
	return fmt.Sprintf("ps -ef |grep %sbin/%s |grep -v grep |awk '{print $2}' |xargs kill", cwd, svr)
}

func (p *TgzPkg) RestartCmd(svr, cwd string) string {
	return p.StopCmd(svr, cwd) + "; sleep 5;" + p.StartCmd(svr, cwd)
}

func (p *TgzPkg) InstallCmd(svr string, pkgs Packages) string {
	content := ""
	if svr == CkSvrName {
		content = fmt.Sprintf("mkdir -p %sbin %setc/clickhouse-server/config.d %setc/clickhouse-server/users.d %slog/clickhouse-server %srun;",
			pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
		for _, pkg := range pkgs.PkgLists {
			lastIndex := strings.LastIndex(pkg, "-")
			extractDir := pkg[:lastIndex]
			content += fmt.Sprintf("tar -xf /tmp/%s -C /tmp;", pkg)
			content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %sbin;", extractDir, pkgs.Cwd)
			if strings.Contains(extractDir, common.PkgModuleClient) {
				content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-client %setc/;", extractDir, pkgs.Cwd)
			} else if strings.Contains(extractDir, common.PkgModuleServer) {
				content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-server %setc/;", extractDir, pkgs.Cwd)
			}
		}
	} else if svr == KeeperSvrName {
		content = fmt.Sprintf("mkdir -p %sbin %s/etc/clickhouse-keeper %slog/clickhouse-keeper %srun;",
			pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
		pkg := pkgs.Keeper
		lastIndex := strings.LastIndex(pkg, "-")
		extractDir := pkg[:lastIndex]
		content += fmt.Sprintf("tar -xf /tmp/%s -C /tmp;", pkg)
		content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %sbin;", extractDir, pkgs.Cwd)
		content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-keeper/* %setc/clickhouse-keeper/;", extractDir, pkgs.Cwd)
	}
	return strings.TrimSuffix(content, ";")
}

func (p *TgzPkg) UpgradeCmd(svr string, pkgs Packages) string {
	return p.InstallCmd(svr, pkgs)
}

func (p *TgzPkg) Uninstall(svr string, pkgs Packages, version string) string {
	return fmt.Sprintf("rm -rf %s*", pkgs.Cwd)
}
