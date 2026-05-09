package deploy

import (
	"strings"

	"github.com/housepower/ckman/common"
)

type CmdAdpt interface {
	// dataDir is the on-disk data directory used by the service (e.g.
	// <Path>/clickhouse for clickhouse-server, <KeeperConf.Path>/clickhouse-keeper
	// for clickhouse-keeper). Used by tgz mode to clean stale pid/status files;
	// ignored by systemd-managed packages (rpm/deb).
	StartCmd(svr, cwd, dataDir string) string
	StopCmd(svr, cwd, dataDir string) string
	RestartCmd(svr, cwd, dataDir string) string
	InstallCmd(svr string, pkgs Packages) string
	UpgradeCmd(svr string, pkgs Packages) string
	Uninstall(svr string, pkgs Packages, version string) string
}

type CmdFactory interface {
	Create() CmdAdpt
}

func GetSuitableCmdAdpt(pkgType string) CmdAdpt {
	pkgType = common.GetStringwithDefault(pkgType, "x86_64.rpm")
	suffix := strings.Split(pkgType, ".")[1]

	switch suffix {
	case common.PkgSuffixTgz:
		return TgzFacotry{}.Create()
	case common.PkgSuffixDeb:
		return DebFacotry{}.Create()
	case common.PkgSuffixRpm:
		return RpmFacotry{}.Create()
	default:
		return RpmFacotry{}.Create()
	}

}
