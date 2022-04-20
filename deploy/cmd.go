package deploy

import (
	"github.com/housepower/ckman/common"
	"strings"
)

type CmdAdpt interface {
	StartCmd(svr string) string
	StopCmd(svr string) string
	RestartCmd(svr string) string
	InstallCmd(pkgs []string) string
	UpgradeCmd(pkgs []string) string
	Uninstall(pkgs []string) string
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
