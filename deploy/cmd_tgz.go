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

func (p *TgzPkg) StartCmd(svr, cwd, dataDir string) string {
	configName := "config.xml"
	if svr == KeeperSvrName {
		configName = "keeper_config.xml"
	}
	pidFile := fmt.Sprintf("%s/run/%s.pid", cwd, svr)
	statusFile := fmt.Sprintf("%s/status", dataDir)
	bin := fmt.Sprintf("%s/bin/%s", cwd, svr)
	configFile := fmt.Sprintf("%s/etc/%s/%s", cwd, svr, configName)
	// If the previous pid is still alive, abort to avoid double instance;
	// otherwise wipe stale pid/status (which can keep ClickHouse from starting)
	// and launch.
	livenessCheck := fmt.Sprintf(
		`if [ -f %s ]; then P=$(cat %s 2>/dev/null); if [ -n "$P" ] && kill -0 "$P" 2>/dev/null; then echo "%s already running (pid $P)" >&2; exit 1; fi; fi`,
		pidFile, pidFile, svr)
	cleanup := fmt.Sprintf("rm -f %s %s", pidFile, statusFile)
	start := fmt.Sprintf("%s --config-file=%s --pid-file=%s --daemon", bin, configFile, pidFile)
	return fmt.Sprintf("%s; %s; %s", livenessCheck, cleanup, start)
}

func (p *TgzPkg) StopCmd(svr, cwd, dataDir string) string {
	pidFile := fmt.Sprintf("%s/run/%s.pid", cwd, svr)
	statusFile := fmt.Sprintf("%s/status", dataDir)
	bin := fmt.Sprintf("%s/bin/%s", cwd, svr)
	kill := fmt.Sprintf("ps -ef |grep %s |grep -v grep |awk '{print $2}' |xargs kill", bin)
	// Wait up to 30s for the process to actually exit before removing files,
	// so subsequent starts see a clean slate.
	wait := fmt.Sprintf(`for i in $(seq 1 30); do ps -ef |grep %s |grep -v grep >/dev/null || break; sleep 1; done`, bin)
	cleanup := fmt.Sprintf("rm -f %s %s", pidFile, statusFile)
	return fmt.Sprintf("%s; %s; %s", kill, wait, cleanup)
}

func (p *TgzPkg) RestartCmd(svr, cwd, dataDir string) string {
	return p.StopCmd(svr, cwd, dataDir) + "; " + p.StartCmd(svr, cwd, dataDir)
}

func (p *TgzPkg) InstallCmd(svr string, pkgs Packages) string {
	content := ""
	if svr == CkSvrName {
		content = fmt.Sprintf("mkdir -p %s/bin %s/etc/clickhouse-server/config.d %s/etc/clickhouse-server/users.d %s/log/clickhouse-server %s/run;",
			pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
		for _, pkg := range pkgs.PkgLists {
			lastIndex := strings.LastIndex(pkg, "-")
			extractDir := pkg[:lastIndex]
			content += fmt.Sprintf("tar -xf /tmp/%s -C /tmp;", pkg)
			content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %s/bin;", extractDir, pkgs.Cwd)
			if strings.Contains(extractDir, common.PkgModuleClient) {
				content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-client %s/etc/;", extractDir, pkgs.Cwd)
			} else if strings.Contains(extractDir, common.PkgModuleServer) {
				content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-server %s/etc/;", extractDir, pkgs.Cwd)
			}
		}
	} else if svr == KeeperSvrName {
		content = fmt.Sprintf("mkdir -p %s/bin %s/etc/clickhouse-keeper %s/log/clickhouse-keeper %s/run;",
			pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
		pkg := pkgs.Keeper
		lastIndex := strings.LastIndex(pkg, "-")
		extractDir := pkg[:lastIndex]
		content += fmt.Sprintf("tar -xf /tmp/%s -C /tmp;", pkg)
		content += fmt.Sprintf("cp -rf /tmp/%s/usr/bin/* %s/bin;", extractDir, pkgs.Cwd)
		content += fmt.Sprintf("cp -rf /tmp/%s/etc/clickhouse-keeper/* %s/etc/clickhouse-keeper/;", extractDir, pkgs.Cwd)
	}
	return strings.TrimSuffix(content, ";")
}

func (p *TgzPkg) UpgradeCmd(svr string, pkgs Packages) string {
	return p.InstallCmd(svr, pkgs)
}

func (p *TgzPkg) Uninstall(svr string, pkgs Packages, version string) string {
	return fmt.Sprintf("rm -rf %s/bin %s/etc %s/log %s/run", pkgs.Cwd, pkgs.Cwd, pkgs.Cwd, pkgs.Cwd)
}
