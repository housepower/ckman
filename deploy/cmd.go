package deploy

import (
	"strings"

	"github.com/housepower/ckman/common"
)

type CmdAdpt interface {
	StartCmd(svr, cwd string) string
	StopCmd(svr, cwd string) string
	RestartCmd(svr, cwd string) string
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
