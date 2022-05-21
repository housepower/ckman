package deploy

import (
	"github.com/housepower/ckman/common"
	"strings"
)

type CmdAdpt interface {
	StartCmd(svr, cwd string) string
	StopCmd(svr, cwd string) string
	RestartCmd(svr, cwd string) string
	InstallCmd(pkgs Packages) string
	UpgradeCmd(pkgs Packages) string
	Uninstall(pkgs Packages) string
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
