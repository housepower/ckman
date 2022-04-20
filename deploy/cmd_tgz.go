package deploy

type TgzFacotry struct{}

func (TgzFacotry) Create() CmdAdpt {
	return &TgzPkg{}
}

type TgzPkg struct {}

func (p *TgzPkg)StartCmd(svr string) string{
	return ""
}
func (p *TgzPkg)StopCmd(svr string) string{
	return ""
}

func (p *TgzPkg)RestartCmd(svr string) string {
	return ""
}

func (p *TgzPkg)InstallCmd(pkgs []string) string{
	return ""
}

func (p *TgzPkg) UpgradeCmd(pkgs []string) string {
	return ""
}

func (p *TgzPkg) Uninstall(pkgs []string) string {
	return ""
}

