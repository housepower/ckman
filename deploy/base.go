package deploy

type Packages struct {
	PkgLists []string
	Cwd      string
	Keeper   string
}

type DeployBase struct {
	Packages Packages
}
