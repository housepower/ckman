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

func (p *TgzPkg) InstallCmd(pkgs Packages) string {
	content := fmt.Sprintf("mkdir -p %s/bin %s/etc/clickhouse-server/config.d %s/etc/clickhouse-server/users.d %s/log/clickhouse-server %s/run %s/data/clickhouse;", pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
	for _, pkg := range pkgs.PkgLists {
		lastIndex := strings.LastIndex(pkg, "-")
		extractDir := pkg[:lastIndex]
		content += fmt.Sprintf("tar -xvf /tmp/%s -C /tmp;", pkg)
		content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %s/bin;", extractDir, pkgs.Cwd)
		if !strings.Contains(extractDir, common.PkgModuleCommon) {
			content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-* %s/etc/;", extractDir, pkgs.Cwd)
		}
	}
	//content += fmt.Sprintf(`echo "PATH=$PATH:%s/bin" > %s/.profile;`, pkgs.Cwd, pkgs.Cwd)
	//content += fmt.Sprintf(`echo source %s/.profile >> ${HOME}/.bash_profile;`, pkgs.Cwd)
	//content += fmt.Sprintf("source ${HOME}/.bash_profile;")
	//content += "useradd clickhouse;"
	//content += "groupadd clickhouse;"
	//content += fmt.Sprintf("chown -R clickhouse:clickhouse %s", pkgs.Cwd)
	return strings.TrimSuffix(content, ";")
}

func (p *TgzPkg) UpgradeCmd(pkgs Packages) string {
	return p.InstallCmd(pkgs)
}

func (p *TgzPkg) Uninstall(pkgs Packages) string {
	return fmt.Sprintf("rm -rf %s/*", pkgs.Cwd)
}
