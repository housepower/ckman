package deploy

type Packages struct {
	PkgLists []string
	Cwd      string
}

type DeployBase struct {
	Packages Packages
}
